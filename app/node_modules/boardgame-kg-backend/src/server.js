import "dotenv/config";
import express from "express";
import cors from "cors";
import neo4j from "neo4j-driver";

/** @typedef {import("../../shared/contracts").ApiErrorResponse} ApiErrorResponse */
/** @typedef {import("../../shared/contracts").GameSummary} GameSummary */
/** @typedef {import("../../shared/contracts").GraphPayload} GraphPayload */
/** @typedef {import("../../shared/contracts").RecommendCriteria} RecommendCriteria */
/** @typedef {import("../../shared/contracts").RecommendRequestBody} RecommendRequestBody */

const PORT = Number(process.env.PORT || 4000);
const NEO4J_URI = process.env.NEO4J_URI || "bolt://localhost:7687";
const NEO4J_AUTH_MODE = process.env.NEO4J_AUTH_MODE || "basic";
const NEO4J_USER = process.env.NEO4J_USER || "neo4j";
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD || "password";
const NEO4J_DATABASE = process.env.NEO4J_DATABASE || "neo4j";
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "http://localhost:5173";

const app = express();
app.use(cors({ origin: FRONTEND_ORIGIN }));
app.use(express.json());

const authToken = NEO4J_AUTH_MODE === "none"
  ? neo4j.auth.none()
  : neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD);

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
    playTime: toNumber(properties.playingtime ?? properties.maxplaytime ?? properties.playing_time),
    rating: toNumber(properties.bayesavg ?? properties.average, 0),
    usersRated: toNumber(properties.usersrated, 0)
  };
}

async function runQuery(cypher, params = {}) {
  const session = driver.session({
    defaultAccessMode: neo4j.session.READ,
    database: NEO4J_DATABASE
  });
  try {
    return await session.run(cypher, params);
  } finally {
    await session.close();
  }
}

async function pickCenter(criteria) {
  const result = await runQuery(
    `
    MATCH (g:Game)
    WHERE ($keyword IS NULL OR toLower(coalesce(g.name, "")) CONTAINS toLower($keyword))
      AND ($players IS NULL OR toInteger(coalesce(g.minplayers, g.min_players, 0)) <= $players)
      AND ($maxTime IS NULL OR toInteger(coalesce(g.playingtime, g.maxplaytime, g.playing_time, 9999)) <= $maxTime)
    WITH g,
         coalesce(toFloat(g.bayesavg), toFloat(g.average), 0.0) AS rating,
         coalesce(toInteger(g.usersrated), 0) AS users
    RETURN g
    ORDER BY rating DESC, users DESC
    LIMIT 1
    `,
    {
      keyword: criteria.keyword || null,
      players: criteria.players ? neo4j.int(criteria.players) : null,
      maxTime: criteria.maxTime ? neo4j.int(criteria.maxTime) : null
    }
  );

  if (!result.records.length) return null;
  return result.records[0].get("g");
}

async function getNeighbors(centerElementId, limit = 30) {
  const result = await runQuery(
    `
    MATCH (target:Game)
    WHERE elementId(target) = $centerId
    MATCH (other:Game)
    WHERE other <> target
    WITH target, other,
         coalesce(toFloat(target.bayesavg), toFloat(target.average), 0.0) AS targetRating,
         coalesce(toFloat(other.bayesavg), toFloat(other.average), 0.0) AS otherRating,
         coalesce(toInteger(target.minplayers), toInteger(target.min_players), 1) AS targetMinPlayers,
         coalesce(toInteger(target.maxplayers), toInteger(target.max_players), 8) AS targetMaxPlayers,
         coalesce(toInteger(other.minplayers), toInteger(other.min_players), 1) AS otherMinPlayers,
         coalesce(toInteger(other.maxplayers), toInteger(other.max_players), 8) AS otherMaxPlayers,
         coalesce(toFloat(target.playingtime), toFloat(target.maxplaytime), toFloat(target.playing_time), 60.0) AS targetPlayTime,
         coalesce(toFloat(other.playingtime), toFloat(other.maxplaytime), toFloat(other.playing_time), 60.0) AS otherPlayTime,
         coalesce(toInteger(target.yearpublished), toInteger(target.year_published), 2000) AS targetYear,
         coalesce(toInteger(other.yearpublished), toInteger(other.year_published), 2000) AS otherYear,
         coalesce(toInteger(other.usersrated), 0) AS users,
         toLower(coalesce(target.category, "")) AS targetCategory,
         toLower(coalesce(other.category, "")) AS otherCategory,
         toLower(coalesce(target.mechanic, "")) AS targetMechanic,
         toLower(coalesce(other.mechanic, "")) AS otherMechanic
    WITH other, users,
         (1.0 / (1.0 + abs(targetRating - otherRating))) AS ratingScore,
         CASE
           WHEN targetMinPlayers <= otherMaxPlayers AND otherMinPlayers <= targetMaxPlayers THEN 1.0
           ELSE 0.0
         END AS playersOverlapScore,
         (1.0 / (1.0 + (abs(targetPlayTime - otherPlayTime) / 30.0))) AS timeScore,
         (1.0 / (1.0 + (abs(targetYear - otherYear) / 5.0))) AS yearScore,
         CASE
           WHEN targetCategory <> "" AND otherCategory <> "" AND (targetCategory CONTAINS otherCategory OR otherCategory CONTAINS targetCategory) THEN 1.0
           ELSE 0.0
         END AS categoryScore,
         CASE
           WHEN targetMechanic <> "" AND otherMechanic <> "" AND (targetMechanic CONTAINS otherMechanic OR otherMechanic CONTAINS targetMechanic) THEN 1.0
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
    }
  );

  return result.records.map((record) => ({
    node: record.get("other"),
    similarity: toNumber(record.get("similarity"), 0)
  }));
}

/** @returns {Promise<GraphPayload>} */
async function graphFromCenter(centerNode) {
  const center = nodeToGame(centerNode);
  const neighborRows = await getNeighbors(center.id, 30);
  const neighbors = neighborRows.map((row) => ({
    ...nodeToGame(row.node),
    similarity: row.similarity
  }));

  return {
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
    const result = await runQuery("RETURN 1 AS ok");
    res.json({ ok: result.records[0].get("ok") === neo4j.int(1) });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/graph/default", async (_req, res) => {
  try {
    const center = await pickCenter({ keyword: null, players: null, maxTime: null });
    if (!center) {
      return res.status(404).json({ error: "No games found in Neo4j." });
    }

    return res.json({ source: "default", graph: await graphFromCenter(center) });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/graph/node/:id", async (req, res) => {
  try {
    const result = await runQuery(
      `MATCH (g:Game) WHERE elementId(g) = $id RETURN g LIMIT 1`,
      { id: req.params.id }
    );

    if (!result.records.length) {
      return res.status(404).json({ error: "Node not found." });
    }

    return res.json({ source: "clicked", graph: await graphFromCenter(result.records[0].get("g")) });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post("/api/recommend", async (req, res) => {
  try {
    const criteria = parseCriteria(req.body || {});
    const center = await pickCenter(criteria);

    if (!center) {
      return res.status(404).json({
        error: "No game matched these criteria.",
        criteria
      });
    }

    return res.json({ source: "criteria", criteria, graph: await graphFromCenter(center) });
  } catch (error) {
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
});