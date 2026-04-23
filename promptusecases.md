# Example prompts for use cases

Copy-paste **chat / natural-language** examples aligned with [usecases.md](usecases.md). With `USE_OLLAMA_NL=true`, the backend runs **one** Llama call (`planBoardGameQuery` in [app/backend/src/ollamaNlp.js](app/backend/src/ollamaNlp.js)) and returns JSON `{ intent, similarity_target, filters, result_limit, explanation }`. That plan is merged into **`QuerySpec`** (plus optional FAISS for similarity). With Ollama off, behavior falls back to keyword + defaults in [app/backend/src/searchQuery.js](app/backend/src/searchQuery.js).

For routing details, presets, sort fields, and machine types, see [app/backend/NL_PROMPT_REFERENCE.md](app/backend/NL_PROMPT_REFERENCE.md) (note: that file may still mention older multi-step names in places—the **live** system prompt is `BOARD_GAME_PLANNER_SYSTEM` in `ollamaNlp.js`).

---

## UC-1: Find strong games under a fixed budget

**Intent:** well-rated games at or below a price cap.

| Example prompt |
|----------------|
| *"Well-rated board games under 30 dollars."* |
| *"Show me the best games I can get for under 40 dollars with at least 7.5 BGG rating."* |
| *"Affordable games under 25 dollars, high geek rating, sort by price low to high."* |
| *"What should I buy under 50 dollars that people actually like?"* |

**Also covered by UI preset *Best under $30* in spirit:**

| Example prompt |
|----------------|
| *"best games under 30 dollars"* |
| *"best under budget, cheap but good"* |

---

## UC-2: Maximize “value for money”

**Intent:** geek rating per dollar (latest mean price), not just cheap—distinct from **ridge predicted quality** (UC-3).

| Example prompt |
|----------------|
| *"I want the best value for my money, rating per dollar, descending."* |
| *"Value for price — games with the most bang for the buck under 45 dollars."* |
| *"Sort by rating divided by price, I want the highest value for money."* |
| *"Value for price games, max 35 dollars."* |
| *"Preset value for price, emphasize rating per dollar."* |

---

## UC-3: Overpriced vs undervalued (ridge **pred_avg_quality**)

**Intent:** “bad buy” vs “good buy” using the **ridge** field **`pred_avg_quality`** on `:Game` (see [SCHEMA.md](SCHEMA.md)), not legacy `value_score` / graph booleans (those are not on the export).

**How it behaves**

- Presets **`undervalued`** / **`overpriced`** set `undervaluedOnly` / `overpricedOnly` and sort by **`pred_avg_quality`** (desc / asc).
- Search narrows with **`minPredAvgQuality`** / **`maxPredAvgQuality`** when you pass them (API or merged NL spec).
- Optional server env (see `getThresholds()` in `searchQuery.js`): **`PRED_AVG_QUALITY_UNDERVALUED_MIN`**, **`PRED_AVG_QUALITY_OVERPRICED_MAX`** — tighten bounds when set.
- If a preset is on but there is **no** numeric min/max and **no** env threshold, results are restricted to games that **have** `pred_avg_quality` set.

| Example prompt |
|----------------|
| *"Undervalued games — strong predicted quality from the ridge model, under 60 dollars."* |
| *"Show me undervalued titles; I want high predicted average quality."* |
| *"Overpriced picks: low predicted quality vs what I’d expect, sort worst first."* |
| *"Ridge says these are poor value — overpriced board games."* |
| *"Games where pred_avg_quality should be high but I’m under a budget — undervalued hunt."* |

**Contrast with UC-2:** *"rating per dollar"* → heuristic **rating/price**; *"predicted quality / undervalued / ridge"* → **`pred_avg_quality`** path.

---

## UC-4: Use crowd demand and trade behavior

**Intent:** want vs own spread, or trade-heavy games.

| Example prompt |
|----------------|
| *"High want low own games — people want it but not many own it yet."* |
| *"What do people want but not own, sort by wants minus owns."* |
| *"Games with high BGG want count and more wants than owns."* |
| *"Frequently traded games, high want to trade, sort by WTT."* |
| *"Board games that get traded a lot on BGG."* |

*Avoid conflating this with “undervalued” (ridge)—phrase **high want / low own** explicitly for UC-4.*

---

## UC-5: Natural-language shortcut (no preset name required)

**Intent:** mix budget, players, time, category, style in one sentence.

| Example prompt |
|----------------|
| *"Strategy games for 3 players under 40 dollars and under 2 hours."* |
| *"Cooperative games for 4 people, max 90 minutes, fantasy theme."* |
| *"Economic worker placement, 2 to 4 players, under 50 dollars."* |
| *"Dungeon crawler, heavy games, 3 players, at least 8 BGG rating."* |
| *"Light party games for 5 or more, under 30 minutes."* |

**Similarity + filters (planner `intent` = hybrid when both apply):**

| Example prompt |
|----------------|
| *"Games like Terraforming Mars but under 50 dollars for 2 players."* |
| *"Similar to Gloomhaven in the strategy category, under 2 hours play time."* |

---

## UC-6: List-only vs list + graph (same prompts; API flag differs)

**Intent:** the **prompts** are identical to other UCs; list vs graph is a **request** choice (`includeGraph: false` vs `true` on `POST /api/search`), not different wording.

| Example (works for list or list+graph) |
|----------------------------------------|
| *"Highly rated engine builders under 40 dollars."* |

*Use the app’s search API: `includeGraph: false` for list-only, `includeGraph: true` to return `graph` centered on the first hit.*

---

## UC-7: “Smart” multi-constraint demo (composite)

**Intent:** one shot with price + demand + rating — matches preset **Smart demo** / `composite_demo`.

| Example prompt |
|----------------|
| *"Smart demo — cheap, highly rated, in demand, good for a course presentation."* |
| *"Composite demo preset: under 50 dollars, at least 7 rating, with want counts."* |
| *"I want the smart multi-filter: budget cap, min rating, and some crowd signal."* |

*Exact numeric defaults come from `PRESET_DEFAULTS.composite_demo` in [app/backend/src/searchQuery.js](app/backend/src/searchQuery.js).*

---

## UC-8: Explore similar games from a center (graph)

**Intent:** graph re-centering is **UI/API** (click a node, or `GET /api/graph/node/:id` / `GET /api/graph/bgg/:bggId`). You can still **arrive** at a center with NL first.

**Step 1 — NL search to get a first hit, then open graph in UI**

| Example prompt |
|----------------|
| *"Wingspan" —* then click the result or enable include-graph search so the first hit becomes the center. |
| *"Find me a highly rated 2 player game under 30 dollars"* — use top result as center. |

**Step 2 — Pure “similar to one game” (FAISS path when enabled)**

| Example prompt |
|----------------|
| *"Games like Brass Birmingham."* |
| *"I want something similar to Azul, same vibe."* |
| *"More games like Spirit Island."* |

*Planner JSON: `intent`: `"similarity"`, `similarity_target`: anchor title. Clicking neighbors in the app does not require new prompts.*

---

## UC-9: Sort by predicted quality (ridge)

**Intent:** order by **`pred_avg_quality`** explicitly (same ordering as preset **`undervalued`** / **`overpriced`** sorts, or UI sort **Predicted quality (ridge)**).

| Example prompt |
|----------------|
| *"Sort by predicted average quality, highest ridge score first."* |
| *"Rank these by pred_avg_quality descending."* |
| *"Show me the best predicted quality games under 40 dollars."* |

*`value_score` in `QuerySpec` is still accepted as an alias and sorts like `pred_avg_quality` in the backend.*

---

## Preset name → one-line example prompt (quick test matrix)

| Preset (UI / id) | Example natural-language prompt |
|------------------|---------------------------------|
| Best under $30 / `best_under_budget` | *"best rated games that are under 30 dollars"* |
| Value for price / `value_for_price` | *"value for price, rating per dollar"* |
| Highly rated + cheap / `highly_rated_cheap` | *"highly rated and cheap, under 30, good minimum rating"* |
| Undervalued / `undervalued` | *"undervalued games, high predicted quality (ridge)"* |
| Overpriced / `overpriced` | *"overpriced games, low predicted quality"* |
| High want, low own / `high_want_low_own` | *"high want low own games"* |
| Frequently traded / `frequently_traded` | *"frequently traded, high WTT"* |
| Rating / $ / `rating_per_dollar` | *"sort by rating per dollar descending"* |
| Smart demo / `composite_demo` | *"use the smart demo preset"* |

---

## Negative / edge-case examples (for regression testing)

| What you are testing | Example prompt |
|----------------------|----------------|
| Empty / useless | *""* or *"   "* — expect fallback or no meaningful filters. |
| Noisy LLM | *"just give me the JSON thanks"* — should not crash; server parses first JSON object or falls back. |
| Contrast: high want vs undervalued | *"high want low own"* vs *"undervalued under 40"* — should map to different `preset` / flags (UC-4 vs UC-3). |
| Similarity without anchor | *"something similar"* — target may be null; similarity pipeline should degrade gracefully. |

---

*Pairs with: [usecases.md](usecases.md) (behavior), [app/backend/NL_PROMPT_REFERENCE.md](app/backend/NL_PROMPT_REFERENCE.md) (types; cross-check `ollamaNlp.js` for the live planner schema).*
