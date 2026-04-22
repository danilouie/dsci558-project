"""Per-game feature export: constants, column order, and metadata schema."""

from __future__ import annotations

FEATURE_VERSION_DEFAULT = "v2"
EPS_POS_NEG = 1e-6
EPS_SENT_STD = 1e-6
# When neg_fraction is ~0, raw pos/neg ratio explodes; cap for downstream stability.
POS_NEG_RATIO_CAP = 1_000_000.0

# Scalar columns in fixed order for flattening / run_meta.json (non-sentiment subset).
SCALAR_COLUMNS_EMBEDDING: tuple[str, ...] = (
    "n_reviews",
    "review_count_feature",
    "dispersion",
    "median_distance_to_mean",
    "max_distance_to_mean",
    "n_unique_reviewers",
    "reviews_per_user_mean",
    "reviews_per_user_std",
)

SCALAR_COLUMNS_SENTIMENT: tuple[str, ...] = (
    "sentiment_mean",
    "sentiment_std",
    "sentiment_min",
    "sentiment_max",
    "sentiment_median",
    "pos_fraction",
    "neg_fraction",
    "pos_neg_ratio",
    "sentiment_distribution_ratio",
)

# Full base row when sentiment features are enabled (embedding + sentiment).
SCALAR_COLUMNS_BASE: tuple[str, ...] = SCALAR_COLUMNS_EMBEDDING + SCALAR_COLUMNS_SENTIMENT

SCALAR_COLUMNS_CONCEPT: tuple[str, ...] = ("value_score_mean", "value_score_std")

SCALAR_COLUMNS_EXTENDED: tuple[str, ...] = (
    "sentiment_p25",
    "sentiment_p75",
    "neutral_fraction",
    "sentiment_iqr",
)

DOC_KINDS_REVIEW = frozenset({"bgq_review", "bgg_review"})

# BGG CSV-derived numeric columns (after merge_tabular_for_bgg_id); floats for scaling.
SCALAR_COLUMNS_BGG_TABULAR: tuple[str, ...] = (
    "year",
    "bgg_chart_rank",
    "rank_value",
    "geek_rating",
    "avg_rating",
    "bayes_average",
    "num_voters",
    "usersrated_ranks",
    "is_expansion",
    "min_players",
    "max_players",
    "best_min_players",
    "best_max_players",
    "min_playtime",
    "max_playtime",
    "min_age",
    "complexity",
    "abstracts_rank",
    "cgs_rank",
    "childrensgames_rank",
    "familygames_rank",
    "partygames_rank",
    "strategygames_rank",
    "thematic_rank",
    "wargames_rank",
    "has_games_csv",
    "has_ranks_csv",
)

STRING_COLUMNS_BGG_TABULAR: tuple[str, ...] = (
    "game_name",
    "categories",
    "mechanisms",
    "primary_category",
)

STRING_COLUMNS_SPLITS: tuple[str, ...] = ("stage_a_split", "stage_c_split")

# Neo4j User–Game collection edges: shares count/(o+w+wb+wt) per bgg_id (neo4j/SCHEMA.md).
SCALAR_COLUMNS_COLLECTION_SHARES: tuple[str, ...] = (
    "coll_share_owns",
    "coll_share_wants",
    "coll_share_wtb",
    "coll_share_wtt",
)

# Already normalized to a simplex; exclude from RobustScaler/StandardScaler in preprocess.
SUPERVISION_SCALAR_COLUMNS: tuple[str, ...] = SCALAR_COLUMNS_COLLECTION_SHARES

# Price blocks imported from price_features module at runtime for full lists;
# defaults for preprocess mean-division (level-like log/spread slopes).
DEFAULT_PRICE_COLUMNS_MEAN_DIVIDE: tuple[str, ...] = (
    "log1p_last_mean",
    "log1p_last_min",
    "log1p_last_max",
    "last_week_spread",
    "log1p_mean_hist",
    "log1p_median_weekly_mean",
    "price_slope_4w",
    "price_slope_12w",
    "price_slope_full",
    "price_vol",
    "price_iqr",
    "mean_intraweek_range_last",
    "mean_intraweek_range_avg",
)
