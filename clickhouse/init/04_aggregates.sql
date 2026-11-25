CREATE TABLE IF NOT EXISTS agg_daily_metrics (
    metric_date Date,
    metric_name LowCardinality(String),
    value Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, metric_name);

CREATE TABLE IF NOT EXISTS agg_top_movies (
    metric_date Date,
    rank UInt32,
    movie_id String,
    view_count UInt64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, rank);

CREATE TABLE IF NOT EXISTS agg_retention (
    metric_date Date,
    d1 Float64,
    d7 Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;
