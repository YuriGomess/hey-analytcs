import logging
import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extensions import connection

load_dotenv()

logger = logging.getLogger(__name__)

_db_pool: pool.ThreadedConnectionPool | None = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS campaigns (
    id SERIAL PRIMARY KEY,
    campaign_id VARCHAR UNIQUE,
    campaign_name VARCHAR,
    adset_id VARCHAR,
    adset_name VARCHAR,
    ad_id VARCHAR,
    ad_name VARCHAR,
    lp_url VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaign_metrics (
    id SERIAL PRIMARY KEY,
    campaign_id VARCHAR REFERENCES campaigns(campaign_id),
    date DATE,
    impressions INTEGER DEFAULT 0,
    cpm NUMERIC(10,4) DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    link_clicks INTEGER DEFAULT 0,
    ctr NUMERIC(10,4) DEFAULT 0,
    cpc NUMERIC(10,4) DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    cost_per_page_view NUMERIC(10,4) DEFAULT 0,
    spend NUMERIC(10,2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(campaign_id, date)
);

ALTER TABLE campaign_metrics
ADD COLUMN IF NOT EXISTS meta_forms INTEGER DEFAULT 0;

CREATE TABLE IF NOT EXISTS leads (
    id SERIAL PRIMARY KEY,
    typeform_response_id VARCHAR UNIQUE,
    email VARCHAR,
    phone VARCHAR,
    name VARCHAR,
    utm_campaign VARCHAR,
    utm_adset VARCHAR,
    utm_ad VARCHAR,
    utm_source VARCHAR,
    utm_medium VARCHAR,
    lp_url VARCHAR,
    form_completed_at TIMESTAMP,
    campaign_id VARCHAR REFERENCES campaigns(campaign_id),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lead_status (
    id SERIAL PRIMARY KEY,
    lead_id INTEGER REFERENCES leads(id),
    monday_item_id VARCHAR,
    responded BOOLEAN DEFAULT FALSE,
    meeting_scheduled BOOLEAN DEFAULT FALSE,
    meeting_scheduled_at TIMESTAMP,
    meeting_done BOOLEAN DEFAULT FALSE,
    meeting_done_at TIMESTAMP,
    sale BOOLEAN DEFAULT FALSE,
    sale_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);
"""


def _get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


def _get_pool() -> pool.ThreadedConnectionPool:
    global _db_pool
    if _db_pool is None:
        database_url = _get_database_url()
        _db_pool = pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=database_url)
        logger.info("database_pool_initialized", extra={"minconn": 1, "maxconn": 10})
    return _db_pool


def get_connection() -> connection:
    return _get_pool().getconn()


def return_connection(conn: connection) -> None:
    _get_pool().putconn(conn)


@contextmanager
def get_db_cursor() -> Generator:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield conn, cur
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_connection(conn)


def init_db() -> None:
    try:
        with get_db_cursor() as (_, cur):
            cur.execute(SCHEMA_SQL)
        logger.info("database_initialized")
    except Exception as exc:
        logger.exception("database_init_failed", extra={"error": str(exc)})
        raise


def close_db_pool() -> None:
    global _db_pool
    if _db_pool is not None:
        _db_pool.closeall()
        _db_pool = None
        logger.info("database_pool_closed")
