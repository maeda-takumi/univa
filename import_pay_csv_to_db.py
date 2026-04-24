#!/usr/bin/env python3
"""pay.csv を SQLite の 3テーブル構成に取り込む。"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT_DIR / "data" / "univapay_webhook.sqlite"
CSV_CANDIDATES = [ROOT_DIR / "data" / "pay.csv", ROOT_DIR / "date" / "pay.csv"]

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS csv_raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at TEXT NOT NULL,
    occurred_at_raw TEXT,
    event_type_raw TEXT,
    status_raw TEXT,
    transaction_id TEXT,
    charge_id TEXT,
    store_id TEXT,
    customer_ref TEXT,
    amount_raw TEXT,
    currency_raw TEXT,
    livemode_raw TEXT,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS webhook_raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    request_method TEXT,
    remote_addr TEXT,
    user_agent TEXT,
    authorization_header TEXT,
    content_type TEXT,
    event_type_raw TEXT,
    status_raw TEXT,
    transaction_id TEXT,
    charge_id TEXT,
    store_id TEXT,
    customer_ref TEXT,
    amount_raw TEXT,
    currency_raw TEXT,
    livemode_raw TEXT,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK(source IN ('CSV', 'WEBHOOK')),
    source_event_id INTEGER,
    payment_date_jst TEXT NOT NULL,    payer_name TEXT,
    amount INTEGER,
    email TEXT,
    event_type_norm TEXT,
    status_norm TEXT,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, source_event_id, payment_date_jst)
);
CREATE INDEX IF NOT EXISTS idx_payment_facts_payment_date ON payment_facts(payment_date_jst);
CREATE INDEX IF NOT EXISTS idx_payment_facts_status ON payment_facts(status_norm);
CREATE INDEX IF NOT EXISTS idx_payment_facts_source ON payment_facts(source);

DROP TABLE IF EXISTS webhook_events;
DROP VIEW IF EXISTS webhook_events;
CREATE VIEW webhook_events AS
SELECT
    id,
    payment_date_jst AS received_at,
    payment_date_jst AS payment_date,
    NULL AS request_method,
    NULL AS remote_addr,
    NULL AS user_agent,
    NULL AS authorization_header,
    NULL AS content_type,
    event_type_norm AS event_type,
    status_norm AS status,
    NULL AS status_raw,
    NULL AS transaction_id,
    NULL AS charge_id,
    NULL AS store_id,
    NULL AS customer_id,
    amount,
    NULL AS currency,
    NULL AS livemode,
    payer_name,
    email,
    source,
    raw_json
FROM payment_facts;
"""


def detect_csv_path(explicit_path: str | None) -> Path:
    if explicit_path:
        p = Path(explicit_path)
        if not p.is_absolute():
            p = ROOT_DIR / p
        return p
    for c in CSV_CANDIDATES:
        if c.exists():
            return c

    return CSV_CANDIDATES[0]

def parse_dt_jst(value: str | None) -> str:
    if value:
        t = value.strip()
        if t:
            for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(t, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    s = value.strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def normalize_status(raw: str | None) -> str | None:
    if raw is None:
        return None
    n = raw.strip().lower()
    if not n:
        return None
    if any(w in n for w in ("success", "succeeded", "completed", "paid", "captured", "approved", "成功", "完了")):
        return "成功"
    if any(w in n for w in ("pending", "processing", "in_progress", "authorized", "awaiting", "処理中", "保留")):
        return "処理中"
    if any(w in n for w in ("refund", "chargeback", "reversed", "返金", "取消")):
        return "返金/取消"
    if any(w in n for w in ("fail", "cancel", "error", "expired", "declined", "voided", "失敗", "エラー", "キャンセル")):
        return "失敗"
    return raw


def normalize_event(raw: str | None) -> str | None:
    if raw is None:
        return None
    n = raw.strip().lower()
    if not n:
        return None
    direct = {
        "charge_finished": "売上",
        "charge_pending": "処理待ち",
        "charge_canceled": "キャンセル",
        "charge_cancelled": "キャンセル",
        "charge_refunded": "赤伝返金",
        "chargeback_created": "チャージバック",
        "token_created": "リカーリングトークン発行",
        "token_three_ds_updated": "3-Dセキュア認証",
    }
    if n in direct:
        return direct[n]
    mapping = [
        (("three_ds", "3ds"), "3-Dセキュア認証"),
        (("token",), "リカーリングトークン発行"),
        (("chargeback",), "チャージバック"),
        (("refund",), "赤伝返金"),
        (("cancel", "canceled", "cancelled", "void"), "キャンセル"),
        (("pending", "processing"), "処理待ち"),
        (("failed", "failure", "error", "decline"), "売上失敗"),
        (("payment", "charge", "capture", "売上"), "売上"),
    ]
    for keywords, out in mapping:
        if any(k in n for k in keywords):
            return out
    return raw


def ensure_schema(conn: sqlite3.Connection, reset_all: bool) -> None:
    if reset_all:
        conn.executescript(
            """
            DROP TABLE IF EXISTS webhook_events;
            DROP VIEW IF EXISTS webhook_events;
            DROP TABLE IF EXISTS payment_facts;
            DROP TABLE IF EXISTS csv_raw_events;
            DROP TABLE IF EXISTS webhook_raw_events;
            """
        )
    conn.executescript(SCHEMA_SQL)

def parse_livemode(mode: str | None) -> int | None:
    if mode is None:
        return None
    n = mode.strip().lower()
    if n in {"live", "本番", "prod", "production"}:
        return 1
    if n in {"test", "sandbox", "開発"}:
        return 0
    return None

def main() -> int:
    parser = argparse.ArgumentParser(description="pay.csv を 3テーブル構成へ投入")
    parser.add_argument("--csv", dest="csv_path")
    parser.add_argument("--db", dest="db_path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset-all", action="store_true", help="既存データを破棄して作り直す")
    args = parser.parse_args()

    csv_path = detect_csv_path(args.csv_path)
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path

    if not csv_path.exists():
        raise FileNotFoundError(f"CSVファイルが見つかりません: {csv_path}")

    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))


    print(f"CSV: {csv_path}")
    print(f"読込件数: {len(rows)}")

    if args.dry_run:
        print("dry-run のため書き込みなし")
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(db_path) as conn:
        ensure_schema(conn, args.reset_all)

        for row in rows:
            event_type_raw = (row.get("イベント") or "").strip() or None
            status_raw = (row.get("課金ステータス") or row.get("返金ステータス") or "").strip() or None
            occurred_raw = (row.get("課金作成日時") or row.get("イベント作成日時") or "").strip() or None
            amount_raw = (row.get("イベント金額") or row.get("課金金額") or "").strip() or None
            currency_raw = (row.get("イベント通貨") or row.get("課金通貨") or row.get("返金通貨") or "").strip() or None

            cur = conn.execute(
                """
                INSERT INTO csv_raw_events (
                    imported_at, occurred_at_raw, event_type_raw, status_raw,
                    transaction_id, charge_id, store_id, customer_ref,
                    amount_raw, currency_raw, livemode_raw, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    occurred_raw,
                    event_type_raw,
                    status_raw,
                    (row.get("トークンID") or "").strip() or None,
                    (row.get("課金ID") or "").strip() or None,
                    (row.get("店舗") or "").strip() or None,
                    (row.get("メールアドレス") or row.get("電話番号") or "").strip() or None,
                    amount_raw,
                    currency_raw,
                    (row.get("モード") or "").strip() or None,
                    json.dumps(row, ensure_ascii=False),
                ),
            )
            source_event_id = cur.lastrowid

            conn.execute(
                """
                INSERT INTO payment_facts (
                    source, source_event_id, payment_date_jst, payer_name, amount, email,
                    event_type_norm, status_norm, raw_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "CSV",
                    source_event_id,
                    parse_dt_jst(occurred_raw),
                    (row.get("入金者名") or row.get("氏名") or row.get("カード名義") or "").strip() or None,
                    to_int_or_none(amount_raw),
                    (row.get("メールアドレス") or "").strip() or None,
                    normalize_event(event_type_raw),
                    normalize_status(status_raw),
                    json.dumps(row, ensure_ascii=False),
                    now,
                    now,
                ),
            )

        conn.commit()

    print(f"保存先DB: {db_path}")
    print(f"追加件数: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())