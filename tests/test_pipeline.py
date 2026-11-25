import os
import time
import uuid

import clickhouse_connect
import pytest
import requests


def _ch():
    last = None
    for _ in range(30):
        try:
            client = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                username=os.getenv("CLICKHOUSE_USER", "cinema"),
                password=os.getenv("CLICKHOUSE_PASSWORD", "cinema_pass"),
            )
            client.query("SELECT 1")
            return client
        except Exception as e:
            last = e
            time.sleep(2)
    raise RuntimeError(f"ClickHouse not ready: {last}")


def test_event_flows_kafka_to_clickhouse():
    ch = _ch()
    event_id = str(uuid.uuid4())
    payload = {
        "event_id": event_id,
        "user_id": f"test_user_{uuid.uuid4().hex[:6]}",
        "movie_id": "test_movie",
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": str(uuid.uuid4()),
        "progress_seconds": 0,
    }

    r = requests.post(
        os.getenv("PRODUCER_URL", "http://producer:8000") + "/events",
        json=payload, timeout=10,
    )
    assert r.status_code == 200, r.text
    assert r.json()["event_id"] == event_id

    for _ in range(30):
        rows = ch.query(
            "SELECT event_id, user_id, movie_id, event_type "
            "FROM movie_events WHERE event_id = {eid:UUID}",
            parameters={"eid": event_id},
        ).result_rows
        if rows:
            got = rows[0]
            assert str(got[0]) == event_id
            assert got[1] == payload["user_id"]
            assert got[2] == "test_movie"
            assert got[3] == "VIEW_STARTED"
            return
        time.sleep(1)

    pytest.fail("Event did not appear in ClickHouse within 30s")
