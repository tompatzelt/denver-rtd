/**
 * Denver RTD transit API — Cloudflare Worker.
 *
 * Serves two endpoints to the React frontend:
 *   GET /api/stops/nearby?lat=&lon=&radius_km=
 *   GET /api/arrivals/:stopId
 *
 * Static GTFS data (stops/trips/routes) lives in a Workers KV namespace —
 * see scripts/prepare-static-data.mjs for how to populate it. RTD's live
 * GTFS-realtime feed (protobuf) is fetched and decoded on each request,
 * cached at the edge for CACHE_TTL_SECONDS via the Cache API.
 */

import gtfsRealtimeBindings from "gtfs-realtime-bindings";
const { transit_realtime } = gtfsRealtimeBindings;

const TRIP_UPDATES_URL = "https://www.rtd-denver.com/files/gtfs-rt/TripUpdate.pb";
const CACHE_TTL_SECONDS = 15;

// Warm-isolate in-memory cache: survives across requests as long as this
// Worker instance stays alive, so we don't re-read KV on every hit.
let stopsCache = null;
let tripsCache = null;
let routesCache = null;
let stopRoutesCache = null;
let routeStopsCache = null;

async function loadStops(env) {
  if (stopsCache) return stopsCache;
  const raw = await env.GTFS_STATIC.get("stops.json");
  if (!raw) throw new Error("stops.json missing from KV — run the upload step in the README");
  stopsCache = JSON.parse(raw);
  return stopsCache;
}

async function loadTrips(env) {
  if (tripsCache) return tripsCache;
  const raw = await env.GTFS_STATIC.get("trips.json");
  if (!raw) throw new Error("trips.json missing from KV");
  tripsCache = JSON.parse(raw);
  return tripsCache;
}

async function loadRoutes(env) {
  if (routesCache) return routesCache;
  const raw = await env.GTFS_STATIC.get("routes.json");
  if (!raw) throw new Error("routes.json missing from KV");
  routesCache = JSON.parse(raw);
  return routesCache;
}

async function loadStopRoutes(env) {
  if (stopRoutesCache) return stopRoutesCache;
  const raw = await env.GTFS_STATIC.get("stop_routes.json");
  if (!raw) throw new Error("stop_routes.json missing from KV");
  stopRoutesCache = JSON.parse(raw);
  return stopRoutesCache;
}

async function loadRouteStops(env) {
  if (routeStopsCache) return routeStopsCache;
  const raw = await env.GTFS_STATIC.get("route_stops.json");
  if (!raw) throw new Error("route_stops.json missing from KV");
  routeStopsCache = JSON.parse(raw);
  return routeStopsCache;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const r = 6371;
  const p1 = (lat1 * Math.PI) / 180;
  const p2 = (lat2 * Math.PI) / 180;
  const dphi = ((lat2 - lat1) * Math.PI) / 180;
  const dlambda = ((lon2 - lon1) * Math.PI) / 180;
  const a = Math.sin(dphi / 2) ** 2 + Math.cos(p1) * Math.cos(p2) * Math.sin(dlambda / 2) ** 2;
  return 2 * r * Math.asin(Math.sqrt(a));
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*", // tighten to your real domain before shipping
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}

async function handleNearby(url, env) {
  const lat = parseFloat(url.searchParams.get("lat"));
  const lon = parseFloat(url.searchParams.get("lon"));
  if (Number.isNaN(lat) || Number.isNaN(lon)) {
    return json({ error: "lat and lon query params are required" }, 400);
  }
  const radiusKm = parseFloat(url.searchParams.get("radius_km") || "0.8");
  const limit = parseInt(url.searchParams.get("limit") || "20", 10);

  const [stops, stopRoutes] = await Promise.all([loadStops(env), loadStopRoutes(env)]);

  const candidates = [];
  for (const s of stops) {
    const d = haversineKm(lat, lon, s.lat, s.lon);
    if (d <= radiusKm) {
      candidates.push({
        ...s,
        distance_m: Math.round(d * 1000),
        routes: stopRoutes[s.stop_id] || [],
      });
    }
  }
  candidates.sort((a, b) => a.distance_m - b.distance_m);

  // Nearest-of-each-mode is computed against the full candidate list, before
  // dedup, so it always reflects the truly closest stop.
  const nearestBusStop = candidates.find((s) => s.routes.some((r) => r.mode === "bus")) || null;
  const nearestRailStop = candidates.find((s) => s.routes.some((r) => r.mode === "rail")) || null;

  // Collapse stops that only offer route+direction combos already covered
  // by a closer stop — e.g. redundant stop poles a block apart serving the
  // same line toward the same place. A stop offering the *opposite*
  // direction of the same route (or a route not seen yet) still counts as
  // new information and stays in the list.
  const seenCombos = new Set();
  const deduped = [];
  for (const stop of candidates) {
    const newRoutes = stop.routes.filter((r) => !seenCombos.has(`${r.short_name}|${r.headsign}`));
    if (stop.routes.length > 0 && newRoutes.length === 0) continue; // fully redundant
    for (const r of stop.routes) seenCombos.add(`${r.short_name}|${r.headsign}`);
    deduped.push(stop);
    if (deduped.length >= limit) break;
  }

  return json({
    stops: deduped,
    nearest_bus: nearestBusStop
      ? { stop_id: nearestBusStop.stop_id, stop_name: nearestBusStop.stop_name, distance_m: nearestBusStop.distance_m }
      : null,
    nearest_rail: nearestRailStop
      ? { stop_id: nearestRailStop.stop_id, stop_name: nearestRailStop.stop_name, distance_m: nearestRailStop.distance_m }
      : null,
  });
}

async function fetchTripUpdatesFeed() {
  const cache = caches.default;
  const cacheKey = new Request(TRIP_UPDATES_URL);
  let buf;

  const cached = await cache.match(cacheKey);
  if (cached) {
    buf = await cached.arrayBuffer();
  } else {
    const resp = await fetch(TRIP_UPDATES_URL);
    if (!resp.ok) throw new Error(`RTD feed responded ${resp.status}`);
    buf = await resp.arrayBuffer();
    const toCache = new Response(buf.slice(0), {
      headers: { "Cache-Control": `max-age=${CACHE_TTL_SECONDS}` },
    });
    // Don't block the response on the cache write.
    await cache.put(cacheKey, toCache);
  }

  return transit_realtime.FeedMessage.decode(new Uint8Array(buf));
}

async function handleRoutesList(env) {
  const routes = await loadRoutes(env);
  const list = Object.entries(routes).map(([route_id, r]) => ({ route_id, ...r }));
  list.sort((a, b) => {
    if (a.mode !== b.mode) return a.mode === "rail" ? -1 : 1;
    return a.short_name.localeCompare(b.short_name, undefined, { numeric: true });
  });
  return json(list);
}

async function handleRouteDirections(routeId, env) {
  const routeStops = await loadRouteStops(env);
  const routes = await loadRoutes(env);
  if (!routes[routeId]) return json({ error: "unknown route_id" }, 404);

  const directions = Object.keys(routeStops)
    .filter((k) => k.startsWith(`${routeId}|`))
    .map((k) => {
      const directionId = k.slice(routeId.length + 1);
      return {
        direction_id: directionId,
        headsign: routeStops[k].headsign,
        stop_count: routeStops[k].stops.length,
      };
    });

  return json({ route_id: routeId, route: routes[routeId], directions });
}

async function handleRouteTimetable(routeId, directionId, env) {
  const [routeStops, trips, routes] = await Promise.all([loadRouteStops(env), loadTrips(env), loadRoutes(env)]);
  if (!routes[routeId]) return json({ error: "unknown route_id" }, 404);

  const key = `${routeId}|${directionId}`;
  const entry = routeStops[key];
  if (!entry) return json({ error: "unknown route_id/direction_id combination" }, 404);

  let feed;
  try {
    feed = await fetchTripUpdatesFeed();
  } catch (err) {
    // Static stop list still works even if the live feed is temporarily down.
    return json({
      route_id: routeId,
      direction_id: directionId,
      headsign: entry.headsign,
      stops: entry.stops.map((s) => ({ ...s, eta_minutes: null, arrival_epoch: null })),
      live_error: err.message,
    });
  }

  const now = Date.now() / 1000;
  const earliestByStop = {};

  for (const entity of feed.entity) {
    if (!entity.tripUpdate) continue;
    const tu = entity.tripUpdate;
    const tripId = tu.trip?.tripId;
    const tripMeta = trips[tripId];
    if (!tripMeta) continue;

    const tuRouteId = tu.trip?.routeId || tripMeta.route_id;
    if (tuRouteId !== routeId) continue;
    if (String(tripMeta.direction_id) !== String(directionId)) continue;

    for (const stu of tu.stopTimeUpdate || []) {
      const event = stu.arrival || stu.departure;
      if (!event || event.time == null) continue;
      const epoch = Number(event.time);
      if (epoch - now < -60) continue;
      if (earliestByStop[stu.stopId] == null || epoch < earliestByStop[stu.stopId]) {
        earliestByStop[stu.stopId] = epoch;
      }
    }
  }

  const stopsWithEta = entry.stops.map((s) => {
    const epoch = earliestByStop[s.stop_id];
    if (epoch == null) return { ...s, eta_minutes: null, arrival_epoch: null };
    return { ...s, eta_minutes: Math.max(0, Math.round((epoch - now) / 60)), arrival_epoch: epoch };
  });

  return json({ route_id: routeId, direction_id: directionId, headsign: entry.headsign, stops: stopsWithEta });
}

async function handleArrivals(stopId, env) {
  const stops = await loadStops(env);
  const stop = stops.find((s) => s.stop_id === stopId);
  if (!stop) return json({ error: "unknown stop_id" }, 404);

  const [trips, routes, feed] = await Promise.all([
    loadTrips(env),
    loadRoutes(env),
    fetchTripUpdatesFeed(),
  ]);

  const now = Date.now() / 1000;
  const upcoming = [];

  for (const entity of feed.entity) {
    if (!entity.tripUpdate) continue;
    const tu = entity.tripUpdate;
    const tripId = tu.trip?.tripId;
    const tripMeta = trips[tripId] || {};
    const routeId = tu.trip?.routeId || tripMeta.route_id || "";
    const routeMeta = routes[routeId] || {};

    for (const stu of tu.stopTimeUpdate || []) {
      if (stu.stopId !== stopId) continue;
      const event = stu.arrival || stu.departure;
      if (!event || event.time == null) continue;

      const arrivalEpoch = Number(event.time);
      const etaSeconds = arrivalEpoch - now;
      if (etaSeconds < -60) continue; // already left

      upcoming.push({
        trip_id: tripId,
        route_id: routeId,
        route_short_name: routeMeta.short_name || routeId || "?",
        route_color: routeMeta.color || "444444",
        headsign: tripMeta.trip_headsign || "",
        arrival_epoch: arrivalEpoch,
        eta_minutes: Math.max(0, Math.round(etaSeconds / 60)),
      });
    }
  }

  upcoming.sort((a, b) => a.arrival_epoch - b.arrival_epoch);
  return json({ stop, arrivals: upcoming.slice(0, 15) });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    try {
      if (url.pathname === "/api/health") {
        const stops = await loadStops(env).catch(() => []);
        const stopRoutes = await loadStopRoutes(env).catch(() => ({}));
        return json({ status: "ok", stops_loaded: stops.length, stops_with_routes: Object.keys(stopRoutes).length });
      }

      if (url.pathname === "/api/stops/nearby") {
        return await handleNearby(url, env);
      }

      if (url.pathname === "/api/routes") {
        return await handleRoutesList(env);
      }

      const directionsMatch = url.pathname.match(/^\/api\/routes\/([^/]+)\/directions$/);
      if (directionsMatch) {
        return await handleRouteDirections(decodeURIComponent(directionsMatch[1]), env);
      }

      const timetableMatch = url.pathname.match(/^\/api\/routes\/([^/]+)\/timetable$/);
      if (timetableMatch) {
        const directionId = url.searchParams.get("direction_id");
        if (directionId == null) return json({ error: "direction_id query param is required" }, 400);
        return await handleRouteTimetable(decodeURIComponent(timetableMatch[1]), directionId, env);
      }

      const arrivalsMatch = url.pathname.match(/^\/api\/arrivals\/(.+)$/);
      if (arrivalsMatch) {
        return await handleArrivals(decodeURIComponent(arrivalsMatch[1]), env);
      }

      return json({ error: "not found" }, 404);
    } catch (err) {
      return json({ error: err.message || "internal error" }, 500);
    }
  },
};
