import "dotenv/config";
import express from "express";
import cors from "cors";
import neo4j from "neo4j-driver";
import {
  runSearchQuery,
  messageToQuerySpec,
  mergeQuerySpec
} from "./searchQuery.js";
import { isTraceEnabled, traceStep } from "./trace.js";

/** @typedef {import("../../shared/contracts.d.ts").ApiErrorResponse} ApiErrorResponse */
/** @typedef {import("../../shared/contracts.d.ts").GameSummary} GameSummary */
/** @typedef {import("../../shared/contracts.d.ts").GraphPayload} GraphPayload */
/** @typedef {import("../../shared/contracts.d.ts").RecommendCriteria} RecommendCriteria */
/** @typedef {import("../../shared/contracts.d.ts").RecommendRequestBody} RecommendRequestBody */
/** @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec */
/** @typedef {import("../../shared/contracts.d.ts").SearchRequestBody} SearchRequestBody */
/** @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId */

const PORT = Number(process.env.PORT || 4000);
const NEO4J_URI = process.env.NEO4J_URI || "bolt://localhost:7687";
const NEO4J_AUTH_MODE = process.env.NEO4J_AUTH_MODE || "basic";
const NEO4J_USER = process.env.NEO4J_USER || "neo4j";
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD || "password";
const NEO4J_DATABASE = process.env.NEO4J_DATABASE || "neo4j";
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "http://localhost:5173";
const USE_SEARCH_DEFAULT = (process.env.USE_SEARCH_DEFAULT || "true") !== "false";

/** Maximum raw similarity from getNeighbors (rating×2 + players×1.5 + time×1 + year×0.6 + category + mechanic). */
const NEIGHBOR_SIMILARITY_RAW_MAX =
  2.0 + 1.5 + 1.0 + 0.6 + 1.0 + 1.0;

/**
 * Map raw weighted sum to [0, 1] so UI can show 0–100% match.
 * @param {number | null | undefined} raw
 */
function normalizeNeighborSimilarity(raw) {
  const r = toNumber(raw, 0);
  if (r <= 0) return 0;
  const n = r / NEIGHBOR_SIMILARITY_RAW_MAX;
  return Math.min(1, Math.max(0, n));
}

const app = express();
app.use(cors({ origin: FRONTEND_ORIGIN }));
app.use(express.json());

const authToken = NEO4J_AUTH_MODE === "none" ? neo4j.auth.none() : neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD);

const driver = neo4j.driver(NEO4J_URI, authToken);

function toNumber(value, fallback = null) {
  if (value == null) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toStringOrNull(value) {
  if (value == null) return null;
  return String(value);
}

/** @returns {GameSummary} */
function nodeToGame(node) {
  const properties = node.properties;
  return {
    id: node.elementId,
    bggId: toStringOrNull(properties.bgg_id),
    name: toStringOrNull(properties.name) || "Unknown Game",
    yearPublished: toNumber(properties.yearpublished ?? properties.year_published),
    minPlayers: toNumber(properties.minplayers ?? properties.min_players),
    maxPlayers: toNumber(properties.maxplayers ?? properties.max_players),
    playTime: toNumber(
      properties.playingtime ?? properties.maxplaytime ?? properties.playing_time
    ),
    rating: toNumber(
      properties.geek_rating != null
        ? properties.geek_rating
        : properties.bayesavg ?? properties.average,
      0
    ),
    usersRated: toNumber(properties.usersrated, 0),
    complexity: toNumber(properties.complexity)
  };
}

/**
 * @param {string} cypher
 * @param {object} [params]
 * @param {string} [label] - used in trace logs
 */
async function runQuery(cypher, params = {}, label = "cypher") {
  const quiet = label === "skip";
  if (!quiet) {
    traceStep("neo4j", `run start: ${label}`);
  }
  const t0 = Date.now();
  const session = driver.session({
    defaultAccessMode: neo4j.session.READ,
    database: NEO4J_DATABASE
  });
  try {
    const result = await session.run(cypher, params);
    if (!quiet) {
      traceStep("neo4j", `run done: ${label}`, { ms: Date.now() - t0, recordCount: result.records.length });
    }
    return result;
  } catch (err) {
    if (!quiet) {
      traceStep("neo4j", `run error: ${label}`, { ms: Date.now() - t0, message: err?.message });
    }
    throw err;
  } finally {
    await session.close();
  }
}

/**
 * @param {RecommendRequestBody | SearchRequestBody} body
 * @param {import("../../shared/contracts.d.ts").RecommendRequestBody["filters"]} [f]
 */
function bodyToQuerySpec(body = {}, f = body.filters) {
  traceStep("recommend", "1. bodyToQuerySpec: parse message heuristics + filters");
  const fl = f || {};
  const fromMsg = messageToQuerySpec(String(body.message || ""));
  const merged = mergeQuerySpec(
    fromMsg,
    {
      keyword: fl.keyword != null && fl.keyword !== "" ? String(fl.keyword) : undefined,
      players: toNumber(fl.players) ?? null,
      maxTime: toNumber(fl.maxTime) ?? null,
      maxPrice: toNumber(fl.maxPrice) ?? null,
      minRating: toNumber(fl.minRating) ?? null,
      sort: fl.sort,
      limit: 40
    },
    fl.preset ?? null
  );
  traceStep("recommend", "2. merged QuerySpec for /recommend", {
    messageSnippet: (body.message && String(body.message).slice(0, 60)) || null,
    ...merged
  });
  return merged;
}

/**
 * @param {SearchRequestBody} body
 */
function parseSearchRequest(body = {}) {
  traceStep("search-api", "1. parseSearchRequest: incoming", {
    hasMessage: Boolean(body.message),
    hasQuery: Boolean(body.query),
    hasLegacyFilters: Boolean(body.filters),
    mergeMessage: body.mergeMessage !== false,
    includeGraph: Boolean(body.includeGraph)
  });
  const mergeMessage = body.mergeMessage !== false;
  const fromMsg = mergeMessage ? messageToQuerySpec(String(body.message || "")) : {};
  const ex = body.query && typeof body.query === "object" ? body.query : {};
  const legacyF = body.filters && typeof body.filters === "object" ? body.filters : {};
  const merged = mergeQuerySpec(
    fromMsg,
    { ...ex, ...legacyF },
    /** @type {import("../../shared/contracts.d.ts").QueryPresetId | null} */ (
      ex.preset ?? legacyF.preset
    ) ?? null
  );
  if (body.limit != null) merged.limit = body.limit;
  traceStep("search-api", "2. parseSearchRequest: merged spec", { ...merged, includeGraph: Boolean(body.includeGraph) });
  return { query: merged, includeGraph: Boolean(body.includeGraph) };
}

async function pickCenter(criteria) {
  traceStep("pickCenter", "1. pickCenter: top-1 by rating", { ...criteria });
  const result = await runQuery(
    `
    MATCH (g:Game)
    WHERE ($keyword IS NULL OR toLower(coalesce(g.name, "")) CONTAINS toLower($keyword))
      AND ($players IS NULL OR toInteger(coalesce(g.min_players, g.minplayers, 0)) <= $players)
      AND ($maxTime IS NULL OR toInteger(coalesce(g.playingtime, g.maxplaytime, g.playing_time, 9999)) <= $maxTime)
    WITH g,
         coalesce(toFloat(g.geek_rating), toFloat(g.bayesavg), toFloat(g.average), 0.0) AS rating,
         coalesce(toInteger(g.usersrated), 0) AS users
    RETURN g
    ORDER BY rating DESC, users DESC
    LIMIT 1
    `,
    {
      keyword: criteria.keyword || null,
      players: criteria.players ? neo4j.int(criteria.players) : null,
      maxTime: criteria.maxTime ? neo4j.int(criteria.maxTime) : null
    },
    "pickCenter"
  );

  if (!result.records.length) {
    traceStep("pickCenter", "2. pickCenter: no row");
    return null;
  }
  const n = result.records[0].get("g");
  traceStep("pickCenter", "2. pickCenter: selected", { name: n.properties?.name, bgg_id: n.properties?.bgg_id });
  return n;
}

async function getNeighbors(centerElementId, limit = 30) {
  traceStep("graph", "1. getNeighbors: start", { centerId: centerElementId, limit });
  const result = await runQuery(
    `
    MATCH (target:Game)
    WHERE elementId(target) = $centerId
    MATCH (other:Game)
    WHERE other <> target
    WITH target, other,
         coalesce(toFloat(target.geek_rating), toFloat(target.bayesavg), toFloat(target.average), 0.0) AS targetRating,
         coalesce(toFloat(other.geek_rating), toFloat(other.bayesavg), toFloat(other.average), 0.0) AS otherRating,
         coalesce(toInteger(target.min_players), toInteger(target.minplayers), 1) AS targetMinPlayers,
         coalesce(toInteger(target.max_players), toInteger(target.maxplayers), 8) AS targetMaxPlayers,
         coalesce(toInteger(other.min_players), toInteger(other.minplayers), 1) AS otherMinPlayers,
         coalesce(toInteger(other.max_players), toInteger(other.maxplayers), 8) AS otherMaxPlayers,
         coalesce(toFloat(target.playingtime), toFloat(target.maxplaytime), toFloat(target.play_time), 60.0) AS targetPlayTime,
         coalesce(toFloat(other.playingtime), toFloat(other.maxplaytime), toFloat(other.play_time), 60.0) AS otherPlayTime,
         coalesce(toInteger(target.yearpublished), toInteger(target.year_published), 2000) AS targetYear,
         coalesce(toInteger(other.yearpublished), toInteger(other.year_published), 2000) AS otherYear,
         coalesce(toInteger(other.usersrated), 0) AS users
    , toFloat(size([a IN coalesce(target.categories, []) WHERE
        ANY(b IN coalesce(other.categories, []) WHERE toString(a) = toString(b)) | a
      ])) AS catOverlapN
    , toFloat(size([a IN coalesce(target.mechanisms, []) WHERE
        ANY(b IN coalesce(other.mechanisms, []) WHERE toString(a) = toString(b)) | a
      ])) AS mechOverlapN
    WITH other, users, targetRating, otherRating, targetMinPlayers, targetMaxPlayers, otherMinPlayers, otherMaxPlayers,
         targetPlayTime, otherPlayTime, targetYear, otherYear, catOverlapN, mechOverlapN
    WITH other, users,
         (1.0 / (1.0 + abs(targetRating - otherRating))) AS ratingScore,
         CASE
           WHEN targetMinPlayers <= otherMaxPlayers AND otherMinPlayers <= targetMaxPlayers THEN 1.0
           ELSE 0.0
         END AS playersOverlapScore,
         (1.0 / (1.0 + (abs(targetPlayTime - otherPlayTime) / 30.0))) AS timeScore,
         (1.0 / (1.0 + (abs(targetYear - otherYear) / 5.0))) AS yearScore,
         CASE
           WHEN catOverlapN > 0.0 THEN 1.0
           ELSE 0.0
         END AS categoryScore,
         CASE
           WHEN mechOverlapN > 0.0 THEN 1.0
           ELSE 0.0
         END AS mechanicScore
    WITH other, users,
         (ratingScore * 2.0)
         + (playersOverlapScore * 1.5)
         + timeScore
         + (yearScore * 0.6)
         + categoryScore
         + mechanicScore
         AS similarity
    RETURN other, similarity, users
    ORDER BY similarity DESC, users DESC
    LIMIT $limit
    `,
    {
      centerId: centerElementId,
      limit: neo4j.int(limit)
    },
    "getNeighbors"
  );

  traceStep("graph", "2. getNeighbors: rows", { neighborCount: result.records.length });
  return result.records.map((record) => ({
    node: record.get("other"),
    similarity: normalizeNeighborSimilarity(record.get("similarity"))
  }));
}

/** @returns {Promise<GraphPayload>} */
async function graphFromCenter(centerNode) {
  const center = nodeToGame(centerNode);
  traceStep("graph", "0. graphFromCenter", { center: center.name, id: center.id, bggId: center.bggId });
  const neighborRows = await getNeighbors(center.id, 30);
  const neighbors = neighborRows.map((row) => ({
    ...nodeToGame(row.node),
    similarity: row.similarity
  }));

  const payload = {
    centerId: center.id,
    nodes: [
      { ...center, kind: "center" },
      ...neighbors.map((neighbor) => ({ ...neighbor, kind: "neighbor" }))
    ],
    edges: neighbors.map((neighbor) => ({
      id: `${center.id}->${neighbor.id}`,
      source: center.id,
      target: neighbor.id,
      weight: neighbor.similarity
    }))
  };
  traceStep("graph", "3. graphFromCenter: done", { nodeCount: payload.nodes.length, edgeCount: payload.edges.length });
  return payload;
}

/** @param {RecommendRequestBody} body */
function parseCriteria(body = {}) {
  const message = String(body.message || "").trim();
  const filters = body.filters || {};
  const criteria = {
    keyword: String(filters.keyword || "").trim(),
    players: toNumber(filters.players),
    maxTime: toNumber(filters.maxTime)
  };
  if (!criteria.keyword && message && message.length <= 80) {
    criteria.keyword = message;
  }
  if (criteria.players == null && message) {
    const playersMatch = message.match(/(\d+)\s*player/i);
    if (playersMatch) criteria.players = Number(playersMatch[1]);
  }
  if (criteria.maxTime == null && message) {
    const timeMatch = message.match(/(\d+)\s*(min|minute)/i);
    if (timeMatch) criteria.maxTime = Number(timeMatch[1]);
  }
  return criteria;
}

app.get("/api/health", async (_req, res) => {
  try {
    const result = await runQuery("RETURN 1 AS ok", {}, "skip");
    res.json({ ok: result.records[0].get("ok") === neo4j.int(1) });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/graph/default", async (_req, res) => {
  traceStep("route", "GET /api/graph/default: start");
  try {
    const center = await pickCenter({ keyword: null, players: null, maxTime: null });
    if (!center) {
      traceStep("route", "GET /api/graph/default: 404 no games");
      return res.status(404).json({ error: "No games found in Neo4j." });
    }
    const graph = await graphFromCenter(center);
    traceStep("route", "GET /api/graph/default: 200", { source: "default" });
    return res.json({ source: "default", graph });
  } catch (error) {
    traceStep("route", "GET /api/graph/default: error", { message: error?.message });
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/graph/node/:id", async (req, res) => {
  try {
    traceStep("route", "GET /api/graph/node/:id", { id: req.params.id });
    const result = await runQuery(`MATCH (g:Game) WHERE elementId(g) = $id RETURN g LIMIT 1`, {
      id: req.params.id
    }, "graphNodeById");
    if (!result.records.length) {
      return res.status(404).json({ error: "Node not found." });
    }
    return res.json({ source: "clicked", graph: await graphFromCenter(result.records[0].get("g")) });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post("/api/search", async (req, res) => {
  traceStep("route", "POST /api/search: start");
  try {
    const { query, includeGraph } = parseSearchRequest(req.body || {});
    const { hits, spec } = await runSearchQuery(driver, NEO4J_DATABASE, runQuery, query, neo4j);
    const payload = { query: spec, hits };
    if (includeGraph && hits.length) {
      const firstId = hits[0].game.id;
      traceStep("route", "POST /api/search: building graph for top hit", { firstId, name: hits[0].game.name });
      const gr = await runQuery(
        `MATCH (g:Game) WHERE elementId(g) = $id RETURN g LIMIT 1`,
        { id: firstId },
        "loadGameForGraph"
      );
      if (gr.records.length) {
        payload.graph = await graphFromCenter(gr.records[0].get("g"));
      }
    }
    traceStep("route", "POST /api/search: 200", { hitCount: hits.length, includeGraph: Boolean(payload.graph) });
    return res.json(payload);
  } catch (error) {
    traceStep("route", "POST /api/search: error", { message: error?.message });
    return res.status(500).json({ error: error.message });
  }
});

app.post("/api/recommend", async (req, res) => {
  traceStep("route", "POST /api/recommend: start", { useSearchDefault: USE_SEARCH_DEFAULT });
  try {
    if (USE_SEARCH_DEFAULT) {
      const q = bodyToQuerySpec(req.body || {}, (req.body || {}).filters);
      traceStep("route", "POST /api/recommend: search path", { useSearchDefault: true });
      const { hits, spec: querySpec } = await runSearchQuery(driver, NEO4J_DATABASE, runQuery, q, neo4j);
      if (hits.length) {
        const firstId = hits[0].game.id;
        const gr = await runQuery(
          `MATCH (g:Game) WHERE elementId(g) = $id RETURN g LIMIT 1`,
          { id: firstId },
          "loadGameForGraph"
        );
        if (gr.records.length) {
          traceStep("route", "POST /api/recommend: 200 from search", {
            topGame: hits[0].game.name,
            hitCount: hits.length
          });
          return res.json({
            source: "search",
            fromSearch: true,
            query: querySpec,
            searchMeta: { query: querySpec, topHit: hits[0] },
            criteria: {
              keyword: querySpec.keyword || "",
              players: querySpec.players ?? null,
              maxTime: querySpec.maxTime ?? null,
              maxPrice: querySpec.maxPrice ?? null,
              minRating: querySpec.minRating ?? null,
              preset: querySpec.preset ?? null,
              sort: querySpec.sort
            },
            graph: await graphFromCenter(gr.records[0].get("g"))
          });
        }
        traceStep("route", "POST /api/recommend: search had hits but load node failed, fallback");
      } else {
        traceStep("route", "POST /api/recommend: search returned 0 hits, fallback to pickCenter", {});
      }
    } else {
      traceStep("route", "POST /api/recommend: USE_SEARCH_DEFAULT off, use pickCenter only", {});
    }
    const criteria = parseCriteria(req.body || {});
    const center = await pickCenter(criteria);
    if (!center) {
      traceStep("route", "POST /api/recommend: 404", { criteria });
      return res.status(404).json({
        error: "No game matched these criteria.",
        criteria
      });
    }
    traceStep("route", "POST /api/recommend: 200 from pickCenter", {});
    return res.json({ source: "criteria", criteria, graph: await graphFromCenter(center) });
  } catch (error) {
    traceStep("route", "POST /api/recommend: error", { message: error?.message });
    return res.status(500).json({ error: error.message });
  }
});

process.on("SIGINT", async () => {
  await driver.close();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await driver.close();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`Backend listening on http://localhost:${PORT}`);
  if (isTraceEnabled()) {
    console.log(
      "[kg-trace] Step logging is ON. Set TRACE_STEPS=false in app/backend/.env to disable (health checks use quiet queries)."
    );
  }
});