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

  const results = [];
  for (const s of stops) {
    const d = haversineKm(lat, lon, s.lat, s.lon);
    if (d <= radiusKm) {
      results.push({
        ...s,
        distance_m: Math.round(d * 1000),
        routes: stopRoutes[s.stop_id] || [],
      });
    }
  }
  results.sort((a, b) => a.distance_m - b.distance_m);
  const nearest = results.slice(0, limit);

  const nearestBusStop = results.find((s) => s.routes.some((r) => r.mode === "bus")) || null;
  const nearestRailStop = results.find((s) => s.routes.some((r) => r.mode === "rail")) || null;

  return json({
    stops: nearest,
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
