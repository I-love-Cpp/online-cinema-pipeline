import asyncpg


async def upsert_metric(pool: asyncpg.Pool, d, name, value):
    await pool.execute(
        """
        INSERT INTO metrics (metric_date, metric_name, value, computed_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (metric_date, metric_name)
        DO UPDATE SET value = EXCLUDED.value, computed_at = now()
        """,
        d, name, float(value),
    )


async def upsert_top_movies(pool, d, rows):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM top_movies WHERE metric_date = $1", d)
            for rank, (movie_id, view_count) in enumerate(rows, start=1):
                await conn.execute(
                    """INSERT INTO top_movies
                       (metric_date, rank, movie_id, view_count, computed_at)
                       VALUES ($1, $2, $3, $4, now())""",
                    d, rank, movie_id, int(view_count),
                )


async def upsert_retention(pool, d, d1, d7):
    await pool.execute(
        """
        INSERT INTO retention (metric_date, d1, d7, computed_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (metric_date)
        DO UPDATE SET d1 = EXCLUDED.d1, d7 = EXCLUDED.d7, computed_at = now()
        """,
        d, d1, d7,
    )
