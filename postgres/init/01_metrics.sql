CREATE TABLE IF NOT EXISTS metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE IF NOT EXISTS top_movies (
    metric_date DATE NOT NULL,
    rank INT NOT NULL,
    movie_id TEXT NOT NULL,
    view_count BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, rank)
);

CREATE TABLE IF NOT EXISTS retention (
    metric_date DATE PRIMARY KEY,
    d1 DOUBLE PRECISION,
    d7 DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name, metric_date);
