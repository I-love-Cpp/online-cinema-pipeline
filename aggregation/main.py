import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, timedelta

import asyncpg
import clickhouse_connect
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query

from metrics import QUERIES
from pg_writer import upsert_metric, upsert_top_movies, upsert_retention

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aggregation")

scheduler = AsyncIOScheduler()


async def wait_for_clickhouse(host: str, port: int, user: str, password: str):
    last = None
    for attempt in range(1, 61):
        try:
            client = clickhouse_connect.get_client(
                host=host, port=port, username=user, password=password,
            )
            client.query("SELECT 1")
            log.info("ClickHouse ready after %d attempt(s) as user=%s", attempt, user)
            return client
        except Exception as e:
            last = e
            log.warning("ClickHouse not ready (attempt %d): %s", attempt, e)
            await asyncio.sleep(2)
    raise RuntimeError(f"ClickHouse did not become ready: {last}")


async def wait_for_postgres(dsn: str):
    last = None
    for attempt in range(1, 61):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
            log.info("PostgreSQL ready after %d attempt(s)", attempt)
            return pool
        except Exception as e:
            last = e
            log.warning("PostgreSQL not ready (attempt %d): %s", attempt, e)
            await asyncio.sleep(2)
    raise RuntimeError(f"PostgreSQL did not become ready: {last}")


async def compute_for_date(target: date):
    log.info("Aggregation START date=%s", target)
    t0 = time.monotonic()
    processed = 0
    ch = app.state.ch
    pg = app.state.pg

    ch_rows = []
    for name in ("dau", "avg_watch_time", "conversion"):
        rows = ch.query(QUERIES[name], parameters={"date": target}).result_rows
        val = float(rows[0][0]) if rows and rows[0] and rows[0][0] is not None else 0.0
        await upsert_metric(pg, target, name, val)
        ch_rows.append([target, name, val])
        processed += 1

    top_rows = ch.query(QUERIES["top_movies"], parameters={"date": target}).result_rows
    await upsert_top_movies(pg, target, top_rows)
    processed += len(top_rows)

    ret = ch.query(QUERIES["retention"], parameters={"date": target}).result_rows
    if ret and ret[0]:
        d1 = float(ret[0][0]) if ret[0][0] is not None else 0.0
        d7 = float(ret[0][1]) if ret[0][1] is not None else 0.0
    else:
        d1, d7 = 0.0, 0.0
    await upsert_retention(pg, target, d1, d7)
    processed += 1

    if ch_rows:
        ch.insert("agg_daily_metrics", ch_rows,
                  column_names=["metric_date", "metric_name", "value"])
    if top_rows:
        ch.insert("agg_top_movies",
                  [[target, rank, movie_id, int(vc)]
                   for rank, (movie_id, vc) in enumerate(top_rows, start=1)],
                  column_names=["metric_date", "rank", "movie_id", "view_count"])
    ch.insert("agg_retention", [[target, d1, d7]],
              column_names=["metric_date", "d1", "d7"])

    elapsed = time.monotonic() - t0
    log.info("Aggregation DONE date=%s records=%d elapsed=%.2fs",
             target, processed, elapsed)


async def scheduled_run():
    await compute_for_date(date.today() - timedelta(days=1))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ch = await wait_for_clickhouse(
        os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        int(os.getenv("CLICKHOUSE_PORT", "8123")),
        os.getenv("CLICKHOUSE_USER", "cinema"),
        os.getenv("CLICKHOUSE_PASSWORD", "cinema_pass"),
    )
    app.state.pg = await wait_for_postgres(os.getenv("POSTGRES_DSN"))

    interval = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "3600"))
    scheduler.add_job(scheduled_run, "interval", seconds=interval, id="agg")
    scheduler.start()
    log.info("Scheduler started, interval=%ss", interval)
    yield
    scheduler.shutdown()
    await app.state.pg.close()


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.post("/recompute")
async def recompute(date_str: str = Query(..., alias="date")):
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(400, "invalid date format, expected YYYY-MM-DD")
    try:
        await compute_for_date(d)
    except Exception as e:
        log.exception("Recompute failed")
        raise HTTPException(500, str(e))
    return {"status": "ok", "date": d.isoformat()}
