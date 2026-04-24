#!/usr/bin/env python3
"""univapay_webhook_raw.sqlite のデータを univapay_webhook.sqlite へ移植するスクリプト。"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_RAW_DB_PATH = ROOT_DIR / "data" / "univapay_webhook_raw.sqlite"
DEFAULT_DEST_DB_PATH = ROOT_DIR / "data" / "univapay_webhook.sqlite"

CREATE_DEST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS webhook_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    payment_date TEXT,
    request_method TEXT,
    remote_addr TEXT,
    user_agent TEXT,
    authorization_header TEXT,
    content_type TEXT,
    event_type TEXT,
    status TEXT,
    status_raw TEXT,
    transaction_id TEXT,
    charge_id TEXT,
    store_id TEXT,
    customer_id TEXT,
    amount INTEGER,
    currency TEXT,
    livemode INTEGER,
    source TEXT NOT NULL DEFAULT 'WEBHOOK',
    raw_json TEXT NOT NULL
)
"""

INDEX_SQLS = [
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_received_at ON webhook_events(received_at)",
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_event_type ON webhook_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_transaction_id ON webhook_events(transaction_id)",
]


def normalize_status(status_raw: str | None) -> str | None:
    if status_raw is None:
        return None

    normalized = status_raw.strip().lower()
    if not normalized:
        return None

    if any(word in normalized for word in ("success", "succeeded", "completed", "paid", "captured", "approved")):
        return "成功"

    if any(word in normalized for word in ("pending", "processing", "in_progress", "authorized", "awaiting")):
        return "処理中"

    if any(word in normalized for word in ("refund", "chargeback", "reversed")):
        return "返金/取消"

    if any(word in normalized for word in ("fail", "cancel", "error", "expired", "declined", "voided")):
        return "失敗"

    return status_raw


def ensure_destination_schema(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_DEST_TABLE_SQL)
    for sql in INDEX_SQLS:
        conn.execute(sql)

    columns = {row[1] for row in conn.execute("PRAGMA table_info(webhook_events)").fetchall()}

    if "status_raw" not in columns:
        conn.execute("ALTER TABLE webhook_events ADD COLUMN status_raw TEXT")
        conn.execute("UPDATE webhook_events SET status_raw = status WHERE status_raw IS NULL")

    if "source" not in columns:
        conn.execute("ALTER TABLE webhook_events ADD COLUMN source TEXT NOT NULL DEFAULT 'WEBHOOK'")
        conn.execute("UPDATE webhook_events SET source = 'WEBHOOK' WHERE source IS NULL OR TRIM(source) = ''")

    if "payment_date" not in columns:
        conn.execute("ALTER TABLE webhook_events ADD COLUMN payment_date TEXT")
        conn.execute("UPDATE webhook_events SET payment_date = received_at WHERE payment_date IS NULL OR TRIM(payment_date) = ''")


def fetch_raw_rows(raw_conn: sqlite3.Connection) -> list[sqlite3.Row]:
    raw_conn.row_factory = sqlite3.Row
    return raw_conn.execute(
        """
        SELECT
            received_at,
            request_method,
            remote_addr,
            user_agent,
            authorization_header,
            content_type,
            event_type,
            status_raw,
            transaction_id,
            charge_id,
            store_id,
            customer_id,
            amount,
            currency,
            livemode,
            raw_json
        FROM webhook_raw_events
        ORDER BY id ASC
        """
    ).fetchall()


def resolve_db_path(path: str, *, default: Path) -> Path:
    p = Path(path) if path else default
    if not p.is_absolute():
        p = ROOT_DIR / p
    return p


def main() -> int:
    parser = argparse.ArgumentParser(description="univapay_webhook_raw.sqlite から univapay_webhook.sqlite へ移植します")
    parser.add_argument("--raw-db", default=str(DEFAULT_RAW_DB_PATH), help="移植元 SQLite ファイル")
    parser.add_argument("--dest-db", default=str(DEFAULT_DEST_DB_PATH), help="移植先 SQLite ファイル")
    parser.add_argument("--dry-run", action="store_true", help="移植件数の確認のみ (書き込みなし)")
    args = parser.parse_args()

    raw_db_path = resolve_db_path(args.raw_db, default=DEFAULT_RAW_DB_PATH)
    dest_db_path = resolve_db_path(args.dest_db, default=DEFAULT_DEST_DB_PATH)

    if not raw_db_path.exists():
        raise FileNotFoundError(f"移植元DBが見つかりません: {raw_db_path}")

    with sqlite3.connect(raw_db_path) as raw_conn:
        rows = fetch_raw_rows(raw_conn)

    print(f"移植元: {raw_db_path}")
    print(f"移植先: {dest_db_path}")
    print(f"移植対象件数: {len(rows)}")

    if args.dry_run:
        print("dry-run のため書き込みは行っていません。")
        return 0

    dest_db_path.parent.mkdir(parents=True, exist_ok=True)

    to_insert = [
        {
            "received_at": row["received_at"],
            "payment_date": row["received_at"],
            "request_method": row["request_method"],
            "remote_addr": row["remote_addr"],
            "user_agent": row["user_agent"],
            "authorization_header": row["authorization_header"],
            "content_type": row["content_type"],
            "event_type": row["event_type"],
            "status": normalize_status(row["status_raw"]),
            "status_raw": row["status_raw"],
            "transaction_id": row["transaction_id"],
            "charge_id": row["charge_id"],
            "store_id": row["store_id"],
            "customer_id": row["customer_id"],
            "amount": row["amount"],
            "currency": row["currency"],
            "livemode": row["livemode"],
            "source": "RAW_MIGRATION",
            "raw_json": row["raw_json"],
        }
        for row in rows
    ]

    with sqlite3.connect(dest_db_path) as dest_conn:
        ensure_destination_schema(dest_conn)
        dest_conn.executemany(
            """
            INSERT INTO webhook_events (
                received_at,
                payment_date,
                request_method,
                remote_addr,
                user_agent,
                authorization_header,
                content_type,
                event_type,
                status,
                status_raw,
                transaction_id,
                charge_id,
                store_id,
                customer_id,
                amount,
                currency,
                livemode,
                source,
                raw_json
            ) VALUES (
                :received_at,
                :payment_date,
                :request_method,
                :remote_addr,
                :user_agent,
                :authorization_header,
                :content_type,
                :event_type,
                :status,
                :status_raw,
                :transaction_id,
                :charge_id,
                :store_id,
                :customer_id,
                :amount,
                :currency,
                :livemode,
                :source,
                :raw_json
            )
            """,
            to_insert,
        )
        dest_conn.commit()

    print(f"移植完了: {len(to_insert)} 件")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
