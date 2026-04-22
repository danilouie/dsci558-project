# Example prompts for use cases

Copy-paste **chat / natural-language** examples aligned with [usecases.md](usecases.md). With `USE_OLLAMA_NL=true`, the backend maps these to `QuerySpec` + optional FAISS; with Ollama off, behavior falls back to keyword + defaults in [app/backend/src/searchQuery.js](app/backend/src/searchQuery.js).

For routing details, presets, and sort fields, see [app/backend/NL_PROMPT_REFERENCE.md](app/backend/NL_PROMPT_REFERENCE.md).

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

**Intent:** rating (or value) per dollar, not just cheap.

| Example prompt |
|----------------|
| *"I want the best value for my money, rating per dollar, descending."* |
| *"Value for price — games with the most bang for the buck under 45 dollars."* |
| *"Sort by rating divided by price, I want the highest value for money."* |
| *"Value for price games, max 35 dollars."* |

---

## UC-3: Spot overpriced or undervalued titles

**Intent:** model/flag or proxy for “poor value” vs “good buy”.

| Example prompt |
|----------------|
| *"Show me undervalued games, good value score, rating per dollar."* |
| *"What is undervalued right now under 60 dollars?"* |
| *"I want to see overpriced hot games, bad value for the price."* |
| *"Overpriced board games, sort by price descending."* |

*Note: Without `value_score` / flags on the graph, behavior uses env `RATING_PER_DOLLAR_*` and `VALUE_SCORE_*` per [SCHEMA.md](SCHEMA.md).*

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

*Avoid conflating this with “undervalued” — phrase **high want / low own** explicitly for UC-4.*

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

**Similarity + filters (Ollama classifies *both*):**

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

*Clicking neighbors in the app does not require new prompts; it uses element id / BGG id routes.*

---

## Preset name → one-line example prompt (quick test matrix)

| Preset (UI / id) | Example natural-language prompt |
|------------------|---------------------------------|
| Best under $30 / `best_under_budget` | *"best rated games that are under 30 dollars"* |
| Value for price / `value_for_price` | *"value for price, rating per dollar"* |
| Highly rated + cheap / `highly_rated_cheap` | *"highly rated and cheap, under 30, good minimum rating"* |
| Undervalued / `undervalued` | *"undervalued board games, good value"* |
| Overpriced / `overpriced` | *"overpriced games, poor value for money"* |
| High want, low own / `high_want_low_own` | *"high want low own games"* |
| Frequently traded / `frequently_traded` | *"frequently traded, high WTT"* |
| Rating / $ / `rating_per_dollar` | *"sort by rating per dollar descending"* |
| Smart demo / `composite_demo` | *"use the smart demo preset"* |

---

## Negative / edge-case examples (for regression testing)

| What you are testing | Example prompt |
|----------------------|----------------|
| Empty / useless | *""* or *"   "* — expect fallback or no meaningful filters. |
| Noisy LLM (step 3) | *"just give me the JSON thanks"* — should not crash; server strips or ignores junk if Ollama misbehaves. |
| Contrast: high want vs undervalued | *"high want low own"* vs *"undervalued under 40"* — should map to different `preset` / flags. |

---

*Pairs with: [usecases.md](usecases.md) (behavior), [app/backend/NL_PROMPT_REFERENCE.md](app/backend/NL_PROMPT_REFERENCE.md) (machine types and JSON).*
