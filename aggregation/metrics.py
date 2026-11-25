QUERIES = {
    "dau": """
        SELECT uniqExact(user_id)
        FROM movie_events
        WHERE toDate(timestamp) = {date:Date}
    """,
    "avg_watch_time": """
        SELECT avg(progress_seconds)
        FROM movie_events
        WHERE event_type = 'VIEW_FINISHED'
          AND toDate(timestamp) = {date:Date}
          AND progress_seconds IS NOT NULL
    """,
    "conversion": """
        SELECT
            countIf(event_type = 'VIEW_FINISHED')
              / nullIf(countIf(event_type = 'VIEW_STARTED'), 0)
        FROM movie_events
        WHERE toDate(timestamp) = {date:Date}
    """,
    "top_movies": """
        SELECT movie_id, count() AS view_count
        FROM movie_events
        WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) = {date:Date}
        GROUP BY movie_id
        ORDER BY view_count DESC
        LIMIT 10
    """,
    "retention": """
        WITH first_seen AS (
            SELECT user_id, min(toDate(timestamp)) AS first_day
            FROM movie_events
            GROUP BY user_id
        ),
        cohort_users AS (
            SELECT user_id FROM first_seen WHERE first_day = {date:Date}
        ),
        activity AS (
            SELECT DISTINCT user_id, toDate(timestamp) AS day
            FROM movie_events
            WHERE user_id IN (SELECT user_id FROM cohort_users)
        )
        SELECT
            uniqExactIf(user_id, day = addDays({date:Date}, 1))
              / nullIf(uniqExact(user_id), 0) AS d1,
            uniqExactIf(user_id, day = addDays({date:Date}, 7))
              / nullIf(uniqExact(user_id), 0) AS d7
        FROM activity
    """,
}
