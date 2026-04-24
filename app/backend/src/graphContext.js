/**
 * Load :PricePoint, :Review (BGQ), :BggReview nodes for the center :Game and merge into graph payload.
 * @typedef {import("../../shared/contracts.d.ts").GraphPayload} GraphPayload
 * @typedef {import("../../shared/contracts.d.ts").GraphNode} GraphNode
 * @typedef {import("../../shared/contracts.d.ts").GraphEdge} GraphEdge
 * @typedef {import("../../shared/contracts.d.ts").PricePointContext} PricePointContext
 * @typedef {import("../../shared/contracts.d.ts").BgqReviewContext} BgqReviewContext
 * @typedef {import("../../shared/contracts.d.ts").BggReviewContext} BggReviewContext
 */

import { isTraceEnabled, traceStep } from "./trace.js";

/**
 * @param {unknown} d
 * @returns {string | null}
 */
function formatNeoDate(d) {
  if (d == null) return null;
  if (typeof d === "string") return d;
  if (typeof d === "object" && d !== null) {
    const o = d;
    if (typeof o.toString === "function" && o.year != null) {
      const y = o.year?.low ?? o.year;
      const m = o.month?.low ?? o.month;
      const day = o.day?.low ?? o.day;
      if (y && m && day) {
        return `${y}-${String(m).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
      }
    }
  }
  if (d instanceof Date) return d.toISOString().slice(0, 10);
  return String(d);
}

/**
 * @param {import("neo4j-driver").Node} n
 * @param {import("../../shared/contracts.d.ts").GraphEntityType} graphEntityType
 * @param {import("../../shared/contracts.d.ts").GraphContextPayload} context
 * @param {string} name
 * @returns {GraphNode}
 */
function contextNode(n, graphEntityType, context, name) {
  return {
    id: n.elementId,
    bggId: null,
    name,
    yearPublished: null,
    minPlayers: null,
    maxPlayers: null,
    playTime: null,
    rating: null,
    usersRated: null,
    kind: "context",
    graphEntityType,
    context
  };
}

/**
 * @param {import("neo4j-driver").Node} p
 * @param {import("../../shared/contracts.d.ts").PricePointContext} ctx
 */
function nameForPricePoint(p, ctx) {
  const mean = ctx.meanPrice;
  const d = ctx.date ? ctx.date.slice(0, 10) : "—";
  if (mean != null && Number.isFinite(mean)) {
    return `$${Number(mean).toFixed(2)} · ${d}`;
  }
  return `Price · ${d}`;
}

/**
 * @param {import("neo4j-driver").Node} p
 * @returns {import("../../shared/contracts.d.ts").PricePointContext}
 */
function mapPricePointContext(p) {
  const pr = p.properties;
  return {
    pricePointId: pr.price_point_id != null ? String(pr.price_point_id) : p.elementId,
    date: formatNeoDate(pr.date) ?? (pr.date != null ? String(pr.date) : null),
    minPrice: pr.min_price != null ? Number(pr.min_price) : pr.min != null ? Number(pr.min) : null,
    meanPrice: pr.mean_price != null ? Number(pr.mean_price) : pr.mean != null ? Number(pr.mean) : null,
    maxPrice: pr.max_price != null ? Number(pr.max_price) : pr.max != null ? Number(pr.max) : null,
    source: pr.source != null ? String(pr.source) : null
  };
}

/**
 * @param {import("neo4j-driver").Node} r
 * @returns {import("../../shared/contracts.d.ts").BgqReviewContext}
 */
function mapBgqContext(r) {
  const p = r.properties;
  return {
    reviewId: p.review_id != null ? String(p.review_id) : r.elementId,
    title: p.title != null ? String(p.title) : null,
    author: p.author != null ? String(p.author) : null,
    url: p.url != null ? String(p.url) : null,
    score: p.score != null ? Number(p.score) : null,
    category: p.category != null ? String(p.category) : null,
    publishedAt: p.published_at != null ? formatNeoDate(p.published_at) : p.published_date != null ? String(p.published_date) : null,
    body: p.body != null ? String(p.body) : null
  };
}

/**
 * @param {import("neo4j-driver").Node} br
 * @returns {import("../../shared/contracts.d.ts").BggReviewContext}
 */
function mapBggContext(br) {
  const p = br.properties;
  return {
    bggReviewId: p.bgg_review_id != null ? String(p.bgg_review_id) : br.elementId,
    username: p.username != null ? String(p.username) : null,
    rating: p.rating != null ? Number(p.rating) : null,
    commentText: p.comment_text != null ? String(p.comment_text) : null,
    sources: p.sources != null ? String(p.sources) : null,
    page: p.page != null ? (typeof p.page === "object" && p.page !== null && "toNumber" in p.page ? p.page.toNumber() : Number(p.page)) : null
  };
}

/**
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {string} gameElementId - center :Game elementId
 * @returns {Promise<{ contextNodes: GraphNode[]; contextEdges: GraphEdge[] }>}
 */
export async function fetchContextNodesForCenter(runQuery, gameElementId) {
  /** @type {GraphNode[]} */
  const contextNodes = [];
  /** @type {GraphEdge[]} */
  const contextEdges = [];
  if (!gameElementId) {
    return { contextNodes, contextEdges };
  }

  const t0 = Date.now();

  const rPrice = await runQuery(
    `
    MATCH (g:Game) WHERE elementId(g) = $eid
    MATCH (g)-[:HAS_PRICE_POINT]->(p:PricePoint)
    WITH p ORDER BY p.date DESC
    LIMIT 5
    RETURN p
    `,
    { eid: gameElementId },
    "graphContextPricePoints"
  );

  for (const rec of rPrice.records) {
    const p = rec.get("p");
    if (!p) continue;
    const ctx = mapPricePointContext(p);
    const gn = contextNode(p, "pricePoint", ctx, nameForPricePoint(p, ctx));
    contextNodes.push(/** @type {GraphNode} */ (gn));
  }

  const rBgq = await runQuery(
    `
    MATCH (g:Game) WHERE elementId(g) = $eid
    MATCH (g)-[:HAS_REVIEW]->(r:Review)
    WITH r ORDER BY coalesce(r.published_at, datetime("1970-01-01")) DESC
    LIMIT 1
    RETURN r
    `,
    { eid: gameElementId },
    "graphContextBgqReview"
  );
  for (const rec of rBgq.records) {
    const r = rec.get("r");
    if (!r) continue;
    const ctx = mapBgqContext(r);
    const title = ctx.title != null && ctx.title.length > 0 ? (ctx.title.length > 32 ? `BGQ: ${ctx.title.slice(0, 30)}…` : `BGQ: ${ctx.title}`) : "BGQ review";
    contextNodes.push(
      /** @type {GraphNode} */ (contextNode(r, "bgqReview", ctx, title))
    );
  }

  const rBgg = await runQuery(
    `
    MATCH (g:Game) WHERE elementId(g) = $eid
    MATCH (g)-[:HAS_BGG_REVIEW]->(br:BggReview)
    WITH br ORDER BY rand()
    LIMIT 5
    RETURN br
    `,
    { eid: gameElementId },
    "graphContextBggReviews"
  );
  for (const rec of rBgg.records) {
    const br = rec.get("br");
    if (!br) continue;
    const ctx = mapBggContext(br);
    const u = ctx.username || "user";
    const shortU = u.length > 12 ? `${u.slice(0, 10)}…` : u;
    contextNodes.push(
      /** @type {GraphNode} */ (contextNode(br, "bggReview", ctx, `@${shortU}`))
    );
  }

  for (const cn of contextNodes) {
    contextEdges.push({
      id: `${gameElementId}->${cn.id}`,
      source: gameElementId,
      target: cn.id,
      weight: 0.05
    });
  }

  if (isTraceEnabled()) {
    traceStep("graph", "contextNodes attached", {
      gameElementId,
      count: contextNodes.length,
      ms: Date.now() - t0
    });
  }

  return { contextNodes, contextEdges };
}

/**
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {GraphPayload} payload
 * @returns {Promise<GraphPayload>}
 */
export async function appendContextToGraphPayload(runQuery, payload) {
  if (!payload || !payload.centerId || !payload.nodes.length) return payload;
  const center = payload.nodes.find((n) => n.kind === "center");
  if (!center) return payload;

  const { contextNodes, contextEdges } = await fetchContextNodesForCenter(runQuery, center.id);
  if (contextNodes.length === 0) {
    return payload;
  }
  return {
    ...payload,
    nodes: [...payload.nodes, ...contextNodes],
    edges: [...payload.edges, ...contextEdges]
  };
}
