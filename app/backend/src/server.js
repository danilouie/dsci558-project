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
import {
  buildQuerySpecFromNaturalLanguage,
  parseNaturalLanguageMessage,
  USE_OLLAMA_NL
} from "./nlQuery.js";
import { gameNodeToSummary } from "./gameMapper.js";

/** @typedef {import("../../shared/contracts.d.ts").ApiErrorResponse} ApiErrorResponse */
/** @typedef {import("../../shared/contracts.d.ts").GameSummary} GameSummary */
/** @typedef {import("../../shared/contracts.d.ts").GraphPayload} GraphPayload */
/** @typedef {import("../../shared/contracts.d.ts").RecommendCriteria} RecommendCriteria */
/** @typedef {import("../../shared/contracts.d.ts").RecommendRequestBody} RecommendRequestBody */
/** @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec */
/** @typedef {import("../../shared/contracts.d.ts").SearchRequestBody} SearchRequestBody */
/** @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId */
/** @typedef {import("../../shared/contracts.d.ts").SearchHit} SearchHit */

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

/** @param {unknown} v */
function recordFloatOrNull(v) {
  if (v == null) return null;
  if (typeof v === "object" && "toNumber" in /** @type {object} */ (v)) {
    try {
      const n = /** @type {{ toNumber: () => number }} */ (v).toNumber();
      return Number.isFinite(n) ? n : null;
    } catch {
      return null;
    }
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {import("neo4j-driver").Node} node
 * @param {{ estimatedPrice?: number | null }} [opts]
 * @returns {GameSummary}
 */
function nodeToGame(node, opts) {
  return gameNodeToSummary(node, { estimatedPrice: opts?.estimatedPrice });
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
async function bodyToQuerySpecAsync(body = {}, f = body.filters) {
  traceStep("recommend", "1. bodyToQuerySpec: NL + filters");
  const fl = f || {};
  const { spec: merged, nlParse } = await buildQuerySpecFromNaturalLanguage({
    message: String(body.message || ""),
    filters: fl,
    limit: 40,
    runQuery
  });
  traceStep("recommend", "2. merged QuerySpec for /recommend", {
    messageSnippet: (body.message && String(body.message).slice(0, 60)) || null,
    useOllama: USE_OLLAMA_NL,
    ...merged
  });
  return { spec: merged, nlParse };
}

/**
 * @param {SearchRequestBody} body
 */
async function parseSearchRequest(body = {}) {
  traceStep("search-api", "1. parseSearchRequest: incoming", {
    hasMessage: Boolean(body.message),
    hasQuery: Boolean(body.query),
    hasLegacyFilters: Boolean(body.filters),
    mergeMessage: body.mergeMessage !== false,
    includeGraph: Boolean(body.includeGraph)
  });
  const mergeMessage = body.mergeMessage !== false;
  let fromMsg = {};
  /** @type {import("../../shared/contracts.d.ts").NlParseMeta | null} */
  let nlParse = null;
  if (mergeMessage && String(body.message || "").trim()) {
    if (USE_OLLAMA_NL) {
      const r = await parseNaturalLanguageMessage({
        message: String(body.message || ""),
        runQuery
      });
      fromMsg = r.partial;
      nlParse = r.nlParse;
    } else {
      fromMsg = messageToQuerySpec(String(body.message || ""));
    }
  }
  const ex = body.query && typeof body.query === "object" ? body.query : {};
  const legacyF = body.filters && typeof body.filters === "object" ? body.filters : {};
  const hasMsg = mergeMessage && String(body.message || "").trim();
  const structured = { ...ex, ...legacyF };
  const merged = mergeQuerySpec(
    hasMsg ? {} : structured,
    fromMsg,
    hasMsg
      ? undefined
      : /** @type {import("../../shared/contracts.d.ts").QueryPresetId | null} */ (ex.preset ?? legacyF.preset) ?? null
  );
  if (body.limit != null) merged.limit = body.limit;
  traceStep("search-api", "2. parseSearchRequest: merged spec", {
    ...merged,
    includeGraph: Boolean(body.includeGraph)
  });
  return { query: merged, includeGraph: Boolean(body.includeGraph), nlParse };
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
    WITH other, users, similarity
    OPTIONAL MATCH (other)-[:HAS_PRICE_POINT]->(p:PricePoint)
    WITH other, users, similarity, p
    ORDER BY p.date DESC
    WITH other, users, similarity, collect(p) AS pcol
    WITH other, users, similarity,
         CASE
           WHEN size(pcol) = 0 OR pcol[0] IS NULL
           THEN null
           ELSE toFloat(pcol[0].mean_price)
         END AS estPrice
    RETURN other, users, similarity, estPrice
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
    similarity: normalizeNeighborSimilarity(record.get("similarity")),
    estPrice: record.get("estPrice")
  }));
}

/**
 * Latest mean PricePoint mean_price for a :Game, by element id.
 * @param {string} elementId
 */
async function fetchEstPriceByElementId(elementId) {
  const result = await runQuery(
    `
    MATCH (g:Game) WHERE elementId(g) = $eid
    CALL {
      WITH g
      OPTIONAL MATCH (g)-[:HAS_PRICE_POINT]->(p:PricePoint)
      WITH p
      ORDER BY p.date DESC
      LIMIT 1
      RETURN p AS latestP
    }
    RETURN CASE
      WHEN latestP IS NULL THEN null
      ELSE toFloat(latestP.mean_price) END AS est
    LIMIT 1
    `,
    { eid: elementId },
    "estPriceByElement"
  );
  if (!result.records.length) return null;
  return recordFloatOrNull(result.records[0].get("est"));
}

/**
 * Orbit = top K games from the same `runSearchQuery` list (all match the query; order = search sort).
 * Does not use getNeighbors.
 * @param {SearchHit[]} hits
 * @returns {import("../../shared/contracts.d.ts").GraphPayload}
 */
function graphFromSearchHits(hits) {
  const capNeighbors = 30;
  if (!hits.length) {
    return { centerId: "", nodes: [], edges: [], neighborMode: "search_hits" };
  }
  const first = hits[0].game;
  const centerId = first.id;
  const nTotal = Math.min(hits.length, 1 + capNeighbors);
  /** @type {import("../../shared/contracts.d.ts").GraphNode} */
  const centerNode = {
    ...first,
    kind: "center",
    queryResultRank: 1
  };
  /** @type {import("../../shared/contracts.d.ts").GraphNode[]} */
  const nodes = [centerNode];
  /** @type {import("../../shared/contracts.d.ts").GraphEdge[]} */
  const edges = [];
  for (let i = 1; i < nTotal; i += 1) {
    const g = hits[i].game;
    const rank1 = i + 1;
    const k = i - 1;
    const sim = 1.0 - k * 0.0001;
    nodes.push({
      ...g,
      kind: "neighbor",
      queryResultRank: rank1,
      similarity: sim
    });
    edges.push({
      id: `${centerId}->${g.id}`,
      source: centerId,
      target: g.id,
      weight: sim
    });
  }
  return {
    centerId,
    neighborMode: "search_hits",
    nodes,
    edges
  };
}

/** @returns {Promise<GraphPayload>} */
async function graphFromCenter(centerNode) {
  const centerEst = await fetchEstPriceByElementId(centerNode.elementId);
  const center = nodeToGame(centerNode, { estimatedPrice: centerEst });
  traceStep("graph", "0. graphFromCenter", { center: center.name, id: center.id, bggId: center.bggId });
  const neighborRows = await getNeighbors(center.id, 30);
  const neighbors = neighborRows.map((row) => {
    const n = recordFloatOrNull(row.estPrice);
    return {
      ...nodeToGame(row.node, { estimatedPrice: n }),
      similarity: row.similarity
    };
  });

  const payload = {
    centerId: center.id,
    neighborMode: "similarity",
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
    // Do not use whole NL as name CONTAINS; phrases like "Games like Brass" never match a node.
    const similarityChat =
      /\b(?:board\s*?)?games?\s+(?:like|similar to)\b/i.test(message) ||
      (/^(?:find|show|recommend|give|search|looking for|i\s+want|get)\b/i.test(message) && /\b(like|similar to)\b/i.test(message));
    if (!similarityChat) {
      criteria.keyword = message;
    }
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

/** Center graph on game by BoardGameGeek id (prompt anchor / similarity reference). */
app.get("/api/graph/bgg/:bggId", async (req, res) => {
  try {
    const raw = String(req.params.bggId ?? "").trim();
    if (!raw) {
      return res.status(400).json({ error: "Missing bgg_id." });
    }
    traceStep("route", "GET /api/graph/bgg/:bggId", { bggId: raw });
    const result = await runQuery(
      `MATCH (g:Game) WHERE toString(g.bgg_id) = $bgg RETURN g LIMIT 1`,
      { bgg: raw },
      "graphByBggId"
    );
    if (!result.records.length) {
      return res.status(404).json({ error: "No game with this bgg_id." });
    }
    return res.json({ source: "bgg", bggId: raw, graph: await graphFromCenter(result.records[0].get("g")) });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post("/api/search", async (req, res) => {
  traceStep("route", "POST /api/search: start");
  try {
    const { query, includeGraph, nlParse } = await parseSearchRequest(req.body || {});
    const { hits, spec } = await runSearchQuery(driver, NEO4J_DATABASE, runQuery, query, neo4j);
    const payload = { query: spec, hits, nlParse: nlParse ?? null };
    if (includeGraph && hits.length) {
      traceStep("route", "POST /api/search: building graph from search hits", {
        firstId: hits[0].game.id,
        name: hits[0].game.name,
        hitCount: hits.length
      });
      payload.graph = graphFromSearchHits(hits);
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
  /** @type {import("../../shared/contracts.d.ts").NlParseMeta | null | undefined} */
  let nlParseRecommend = null;
  try {
    if (USE_SEARCH_DEFAULT) {
      const parsed = await bodyToQuerySpecAsync(req.body || {}, (req.body || {}).filters);
      nlParseRecommend = parsed.nlParse ?? null;
      const q = parsed.spec;
      traceStep("route", "POST /api/recommend: search path", { useSearchDefault: true });
      const { hits, spec: querySpec } = await runSearchQuery(driver, NEO4J_DATABASE, runQuery, q, neo4j);
      if (hits.length) {
        traceStep("route", "POST /api/recommend: 200 from search", {
          topGame: hits[0].game.name,
          hitCount: hits.length
        });
        return res.json({
          source: "search",
          fromSearch: true,
          query: querySpec,
          searchMeta: { query: querySpec, topHit: hits[0] },
          nlParse: nlParseRecommend ?? null,
          criteria: {
            keyword: querySpec.keyword || "",
            players: querySpec.players ?? null,
            maxTime: querySpec.maxTime ?? null,
            maxPrice: querySpec.maxPrice ?? null,
            minRating: querySpec.minRating ?? null,
            preset: querySpec.preset ?? null,
            sort: querySpec.sort
          },
          graph: graphFromSearchHits(hits)
        });
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