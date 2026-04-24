/**
 * Aggregate BGG user-review text for a :Game and summarize via Ollama.
 * @typedef {import("../../shared/contracts.d.ts").BggReviewSummaryResponse} BggReviewSummaryResponse
 */

import neo4j from "neo4j-driver";
import { ollamaChat } from "./ollamaNlp.js";
import { traceStep } from "./trace.js";

const MAX_REVIEWS = 100;
const MAX_USER_CONTENT_CHARS = 28000;

const BGG_SUMMARY_SYSTEM = `You are summarizing public BoardGameGeek user review comments for one board game.
Output 2–4 short paragraphs in plain text (no JSON, no bullet list unless the user content is very long).
Note recurring themes, praise, criticism, and common play experiences. Be faithful to the reviews; do not invent specific ratings, numbers, or usernames. If the excerpts are very short or empty, say so briefly.`;

/**
 * @param {import("neo4j-driver").Node} br
 * @returns {{ username: string | null; rating: number | null; commentText: string | null }}
 */
function reviewFromNode(br) {
  const p = br.properties;
  let rating = null;
  if (p.rating != null) {
    if (typeof p.rating === "object" && p.rating !== null && "toNumber" in p.rating) {
      rating = /** @type {{ toNumber: () => number }} */ (p.rating).toNumber();
    } else {
      const n = Number(p.rating);
      rating = Number.isFinite(n) ? n : null;
    }
  }
  return {
    username: p.username != null ? String(p.username) : null,
    rating,
    commentText: p.comment_text != null ? String(p.comment_text) : null
  };
}

/**
 * @param {string} gameName
 * @param {Array<{ username: string | null; rating: number | null; commentText: string | null }>} reviews
 * @returns {string}
 */
export function buildSummaryUserContent(gameName, reviews) {
  const lines = [];
  lines.push(`Game: ${gameName}`);
  lines.push(`Below are ${reviews.length} user review excerpt(s) from BoardGameGeek (order is stable, not by date).`);
  lines.push("");
  let body = lines.join("\n");
  for (let i = 0; i < reviews.length; i += 1) {
    const r = reviews[i];
    const u = r.username || "anonymous";
    const rat = r.rating != null && Number.isFinite(r.rating) ? `rating ${r.rating}/10` : "no rating";
    const excerpt = (r.commentText || "").trim() || "(no comment text)";
    const block = [`[${i + 1}] ${u} (${rat}):`, excerpt].join("\n");
    if (body.length + block.length + 2 > MAX_USER_CONTENT_CHARS) {
      body += "\n\n[Additional reviews omitted to fit model context; truncated after " + i + " review(s).]";
      break;
    }
    body += "\n\n" + block;
  }
  if (body.length > MAX_USER_CONTENT_CHARS) {
    return body.slice(0, MAX_USER_CONTENT_CHARS) + "\n\n[Truncated.]";
  }
  return body;
}

/**
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {string} gameElementId
 * @returns {Promise<{ gameName: string; reviews: ReturnType<typeof reviewFromNode>[] } | { error: "not_found" }>}
 */
export async function fetchBggReviewsForGame(runQuery, gameElementId) {
  if (!gameElementId || String(gameElementId).trim() === "") {
    return { error: "not_found" };
  }

  const eid = String(gameElementId).trim();
  const gameR = await runQuery(
    `MATCH (g:Game) WHERE elementId(g) = $eid RETURN g.name as gameName LIMIT 1`,
    { eid },
    "bggSummaryGameExists"
  );
  if (!gameR.records.length) {
    return { error: "not_found" };
  }
  const rawName = gameR.records[0].get("gameName");
  const gameName = rawName != null ? String(rawName) : "Unknown game";

  const cypher = `
    MATCH (g:Game) WHERE elementId(g) = $eid
    MATCH (g)-[:HAS_BGG_REVIEW]->(br:BggReview)
    WITH br
    ORDER BY toString(coalesce(br.bgg_review_id, '')), toLower(toString(coalesce(br.username, '')))
    RETURN br
    LIMIT $lim
  `;
  const revR = await runQuery(cypher, { eid, lim: neo4j.int(MAX_REVIEWS) }, "bggSummaryReviews");
  const reviews = revR.records
    .map((rec) => {
      const br = rec.get("br");
      return br ? reviewFromNode(/** @type {import("neo4j-driver").Node} */ (br)) : null;
    })
    .filter((x) => x != null);

  return { gameName, reviews: /** @type {ReturnType<typeof reviewFromNode>[]} */ (reviews) };
}

/**
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {string} gameElementId
 * @returns {Promise<BggReviewSummaryResponse | { error: "not_found" } | { error: "ollama"; message: string }>}
 */
export async function summarizeBggReviewsForGame(runQuery, gameElementId) {
  const data = await fetchBggReviewsForGame(runQuery, gameElementId);
  if ("error" in data && data.error === "not_found") {
    traceStep("bggSummary", "game not found", { gameElementId });
    return { error: "not_found" };
  }
  const { gameName, reviews } = data;
  if (reviews.length === 0) {
    traceStep("bggSummary", "zero reviews", { gameElementId, gameName });
    return { summary: "No BGG user reviews are linked to this game in the graph yet.", reviewCount: 0 };
  }

  const userContent = buildSummaryUserContent(gameName, reviews);
  traceStep("bggSummary", "ollama start", { gameElementId, reviewCount: reviews.length, promptChars: userContent.length });
  try {
    const text = await ollamaChat(
      [
        { role: "system", content: BGG_SUMMARY_SYSTEM },
        { role: "user", content: userContent }
      ],
      { temperature: 0.3, label: "bggReviewSummary" }
    );
    const summary = typeof text === "string" ? text.trim() : String(text);
    traceStep("bggSummary", "ollama done", { reviewCount: reviews.length, outChars: summary.length });
    return { summary, reviewCount: reviews.length };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    traceStep("bggSummary", "ollama error", { message: message.slice(0, 200) });
    return { error: "ollama", message };
  }
}
