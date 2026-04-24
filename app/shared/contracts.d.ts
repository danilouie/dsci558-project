export type NodeKind = "center" | "neighbor" | "context";

/** Subgraph nodes attached to the center game: price history, BGQ article, BGG comments. Omitted = game node. */
export type GraphEntityType = "game" | "pricePoint" | "bgqReview" | "bggReview";

export interface PricePointContext {
  pricePointId: string;
  /** ISO date string when available */
  date: string | null;
  minPrice: number | null;
  meanPrice: number | null;
  maxPrice: number | null;
  source: string | null;
}

export interface BgqReviewContext {
  reviewId: string;
  title: string | null;
  author: string | null;
  url: string | null;
  score: number | null;
  category: string | null;
  publishedAt: string | null;
  body: string | null;
}

export interface BggReviewContext {
  bggReviewId: string;
  username: string | null;
  rating: number | null;
  commentText: string | null;
  sources: string | null;
  page: number | null;
}

export type GraphContextPayload = PricePointContext | BgqReviewContext | BggReviewContext;

/** Response from POST /api/graph/summarize-bgg-reviews (success). */
export interface BggReviewSummaryResponse {
  summary: string;
  reviewCount: number;
}

export interface GameSummary {
  id: string;
  bggId?: string | null;
  name: string;
  yearPublished?: number | null;
  minPlayers?: number | null;
  maxPlayers?: number | null;
  playTime?: number | null;
  /** Primary display rating (typically Bayes / geek rating when present). */
  rating?: number | null;
  usersRated?: number | null;
  complexity?: number | null;
  similarity?: number;
  /** Present when item came from /api/search */
  searchExplain?: SearchExplain;

  /** From graph ETL (BGG-style fields on :Game) */
  geekRating?: number | null;
  averageRating?: number | null;
  numVoters?: number | null;
  description?: string | null;
  categories?: string[];
  mechanisms?: string[];
  minAge?: number | null;
  minPlaytime?: number | null;
  maxPlaytime?: number | null;
  bestMinPlayers?: number | null;
  bestMaxPlayers?: number | null;
  isExpansion?: boolean | null;
  /** Overall BGG rank when stored on the node */
  rank?: number | null;
  /** Latest :PricePoint mean_price (est. market price), when loaded from the graph */
  estimatedPrice?: number | null;
  /** Ridge pipeline predicted average quality (`pred_avg_quality` on :Game) */
  predAvgQuality?: number | null;
  /** Ridge summary stats on :Game when present */
  meanOfMean?: number | null;
  maxOfMax?: number | null;
  minOfMin?: number | null;
  /**
   * When the graph is built from search `hits` (not profile similarity), 1-based index in
   * the result list (center = 1). Omitted for browse / getNeighbors graphs.
   */
  queryResultRank?: number;
}

export interface GraphNode extends GameSummary {
  kind: NodeKind;
  /** When not `game`, this node is not a BGG game row — see `context`. */
  graphEntityType?: GraphEntityType;
  /** Type-specific fields for context nodes (price, BGQ, BGG). */
  context?: GraphContextPayload;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  weight: number;
}

export interface GraphPayload {
  centerId: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
  /**
   * `search_hits` = orbit = top K games from the same `runSearchQuery` list (query-matched, query-ranked).
   * `similarity` = legacy profile-similarity from getNeighbors (browse, click-by-id, default).
   * @default undefined (treat as `similarity` for backward compatibility)
   */
  neighborMode?: "search_hits" | "similarity";
}

export type QueryPresetId =
  | "best_under_budget"
  | "value_for_price"
  | "highly_rated_cheap"
  | "overpriced"
  | "undervalued"
  | "high_want_low_own"
  | "frequently_traded"
  | "rating_per_dollar"
  | "composite_demo";

/**
 * How to order results. `rank_value_asc` = best BGG rank (lower number first).
 * `value_score` is an alias for sorting by ridge `pred_avg_quality` on `GameSummary`.
 */
export type SearchSortField =
  | "rating"
  | "geek_rating"
  | "rank_value_asc"
  | "mean_price"
  | "rating_per_dollar"
  | "want_minus_own"
  | "wtt_to_wants"
  | "value_score"
  | "wants"
  | "wtt"
  | "price_drop"
  | "pred_avg_quality";

/** Chat planner intent (Llama JSON); aliases for legacy PromptKind below. */
export type PlannerIntent = "filter" | "similarity" | "hybrid";

/** @deprecated use PlannerIntent — kept for older responses */
export type PromptKind = PlannerIntent | "similar_to_game" | "filtering" | "both";

/** Optional debug metadata when the backend used Ollama / FAISS for the message. */
export interface NlParseMeta {
  promptKind?: PromptKind | null;
  /** Preferred: same as planner `intent` */
  plannerIntent?: PlannerIntent | null;
  /** Ollama | heuristic */
  source?: "ollama" | "heuristic" | "mixed";
  /** Anchor title resolved for similarity (if any) */
  similarToGame?: string | null;
  anchorBggId?: string | null;
  /** When description FAISS constrained the candidate set */
  faissSimilarity?: boolean;
  /** Which embedding index served similar-by-description */
  faissIndex?: "all_games" | "category" | null;
  /** Single planner turn — short model rationale (not used for querying) */
  plannerExplanation?: string | null;
}

/** Nested object returned by the board-game query planner (Ollama JSON). */
export interface PlannerFiltersPayload {
  min_players?: number | null;
  max_players?: number | null;
  min_playtime?: number | null;
  max_playtime?: number | null;
  max_complexity?: number | null;
  min_complexity?: number | null;
  categories?: string[] | null;
  mechanisms?: string[] | null;
  min_avg_rating?: number | null;
  max_price?: number | null;
  year_from?: number | null;
  year_to?: number | null;
  max_min_age?: number | null;
  is_expansion?: boolean | null;
}

/** Full JSON response from the single-shot planner (snake_case matches Ollama JSON). */
export interface PlannerResponse {
  intent: PlannerIntent;
  similarity_target: string | null;
  filters: PlannerFiltersPayload;
  result_limit: number;
  explanation: string;
  /** Optional UI preset id — same strings as `QueryPresetId`. */
  preset?: QueryPresetId | null;
  /**
   * Ridge “undervalued” — high `pred_avg_quality` on `:Game`, not BGG `min_avg_rating`.
   * Pair with `sort` / preset or defaults to `pred_avg_quality` desc in NL merge.
   */
  undervalued_only?: boolean;
  /** Ridge “overpriced” — low `pred_avg_quality`; defaults to sort asc when unset. */
  overpriced_only?: boolean;
  sort?: SearchSortField | null;
  sort_direction?: "asc" | "desc" | null;
}

export interface QuerySpec {
  keyword?: string | null;
  /** Legacy: games that support at least this many players at the table (min_players <= players). Prefer supportsPlayerCount. */
  players?: number | null;
  /** Games that accommodate exactly this player count (min <= N <= max on :Game). */
  supportsPlayerCount?: number | null;
  /** Overlap filter: game player range intersects [filterMinPlayers, filterMaxPlayers]. */
  filterMinPlayers?: number | null;
  filterMaxPlayers?: number | null;
  maxTime?: number | null;
  /** Minimum listed playtime (minutes), lower bound on sessions */
  minPlaytime?: number | null;
  maxPrice?: number | null;
  minPrice?: number | null;
  minRating?: number | null;
  /** Exact-normalized labels (lower case) matched with IN against category strings */
  categoryContains?: string[];
  mechanismContains?: string[];
  minComplexity?: number | null;
  maxComplexity?: number | null;
  minYear?: number | null;
  maxYear?: number | null;
  /** Games with box min age <= this value (family-friendly ceiling) */
  maxMinAge?: number | null;
  /** When true/false, restrict expansions; null = no filter */
  isExpansion?: boolean | null;
  /** Filter games with ridge `pred_avg_quality` in range (games without the property excluded when bound is set) */
  minPredAvgQuality?: number | null;
  maxPredAvgQuality?: number | null;
  minWants?: number | null;
  minOwns?: number | null;
  undervaluedOnly?: boolean;
  overpricedOnly?: boolean;
  /** @deprecated Not used by search Cypher; undervalued/overpriced use `pred_avg_quality` bounds + env thresholds. */
  proxyRatingPerDollarMin?: number | null;
  /** @deprecated Not used by search Cypher. */
  proxyRatingPerDollarMax?: number | null;
  /** BGG username for "games I do not own" style filters */
  usernameExcludesOwns?: string | null;
  /** Days for min recent price window vs latest (trend / "cheap vs recent") */
  priceWindowDays?: number | null;
  minPriceDrop?: number | null;
  sort?: SearchSortField;
  sortDirection?: "asc" | "desc";
  limit?: number;
  preset?: QueryPresetId | null;
  /**
   * When set (non-empty), restrict search to these BGG ids (semantic similarity pipeline).
   * Omit or empty = no restriction.
   */
  bggIdAllowList?: string[] | null;
}

export interface SearchExplain {
  meanPrice: number | null;
  /** Latest :PricePoint date (ISO) */
  priceDate: string | null;
  wants: number;
  wtb: number;
  wtt: number;
  owns: number;
  rankValue: number | null;
  /** Always null — graph has no `value_score`; use `game.predAvgQuality`. */
  valueScore: number | null;
  /** Always null — “value” flags are expressed via ridge `pred_avg_quality` filters. */
  overpriced: boolean | null;
  /** Always null — same as `overpriced`. */
  undervalued: boolean | null;
  ratingPerDollar: number | null;
  /** Always null — price-window drop was removed from search Cypher. */
  priceDropVsWindowMin: number | null;
  /** Always false — no `value_score` on `:Game`. */
  hasValueScoreProp: boolean;
  sort: SearchSortField;
  preset: QueryPresetId | null;
}

export interface SearchHit {
  game: GameSummary;
  explain: SearchExplain;
}

/** API filter bar + `/api/recommend` body — same optional keys as QuerySpec */
export type GameFiltersPayload = Partial<QuerySpec>;

export interface SearchRequestBody {
  message?: string;
  query?: QuerySpec;
  /**
   * Merge: explicit `query` wins; message heuristics fill gaps.
   * @default true
   */
  mergeMessage?: boolean;
  /** @default 40 */
  limit?: number;
  /**
   * When true, return top result as graph center and similar neighbors.
   * @default false
   */
  includeGraph?: boolean;
  /** @deprecated use query + /api/search */
  filters?: GameFiltersPayload;
}

export interface SearchApiResponse {
  query: QuerySpec;
  hits: SearchHit[];
  graph?: GraphPayload;
  nlParse?: NlParseMeta | null;
}

export interface RecommendCriteria {
  keyword: string;
  players: number | null;
  maxTime: number | null;
  maxPrice?: number | null;
  minRating?: number | null;
  preset?: QueryPresetId | null;
  sort?: SearchSortField;
}

export interface RecommendRequestBody {
  message?: string;
  filters?: GameFiltersPayload;
}

export interface SearchMeta {
  query: QuerySpec;
  topHit: SearchHit | null;
}

export interface GraphApiResponse {
  source: string;
  graph: GraphPayload;
  criteria?: RecommendCriteria;
  /** When recommend used search */
  fromSearch?: boolean;
  query?: QuerySpec;
  /** Explain / ranking context for the centered game */
  searchMeta?: SearchMeta | null;
  nlParse?: NlParseMeta | null;
}

export interface ApiErrorResponse {
  error?: string;
}
