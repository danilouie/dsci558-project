export type NodeKind = "center" | "neighbor";

export interface GameSummary {
  id: string;
  bggId?: string | null;
  name: string;
  yearPublished?: number | null;
  minPlayers?: number | null;
  maxPlayers?: number | null;
  playTime?: number | null;
  rating?: number | null;
  usersRated?: number | null;
  complexity?: number | null;
  similarity?: number;
  /** Present when item came from /api/search */
  searchExplain?: SearchExplain;
}

export interface GraphNode extends GameSummary {
  kind: NodeKind;
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
 * `value_score` uses a numeric property on :Game when present; otherwise ignored in ORDER BY.
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
  | "price_drop";

export interface QuerySpec {
  keyword?: string | null;
  players?: number | null;
  maxTime?: number | null;
  maxPrice?: number | null;
  minPrice?: number | null;
  minRating?: number | null;
  /** Substrings (case-insensitive) matched against :Game categories list */
  categoryContains?: string[];
  /** Substrings (case-insensitive) against mechanisms list */
  mechanismContains?: string[];
  minComplexity?: number | null;
  maxComplexity?: number | null;
  minWants?: number | null;
  minOwns?: number | null;
  undervaluedOnly?: boolean;
  overpricedOnly?: boolean;
  /**
   * When set with undervaluedOnly/overpricedOnly and no model flags, require
   * rating/price ratio at or above this value (undervalued) or at or below (overpriced).
   */
  proxyRatingPerDollarMin?: number | null;
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
  valueScore: number | null;
  overpriced: boolean | null;
  undervalued: boolean | null;
  ratingPerDollar: number | null;
  /** (min mean_price in window − latest) / null if unknown */
  priceDropVsWindowMin: number | null;
  /** Whether ORDER BY value_score was available */
  hasValueScoreProp: boolean;
  sort: SearchSortField;
  preset: QueryPresetId | null;
}

export interface SearchHit {
  game: GameSummary;
  explain: SearchExplain;
}

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
  /** @deprecated use query + /api/search; kept for /api/recommend */
  filters?: {
    keyword?: string;
    players?: number | null;
    maxTime?: number | null;
    maxPrice?: number | null;
    minRating?: number | null;
    preset?: QueryPresetId | null;
    sort?: SearchSortField;
  };
}

export interface SearchApiResponse {
  query: QuerySpec;
  hits: SearchHit[];
  graph?: GraphPayload;
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
  filters?: {
    keyword?: string;
    players?: number | null;
    maxTime?: number | null;
    maxPrice?: number | null;
    minRating?: number | null;
    preset?: QueryPresetId | null;
    sort?: SearchSortField;
  };
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
}

export interface ApiErrorResponse {
  error?: string;
}
