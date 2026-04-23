#!/usr/bin/env python3
"""pay.csv を webhook 用 SQLite(DB)へ初期投入するスクリプト。"""

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
CSV_CANDIDATES = [
    ROOT_DIR / "data" / "pay.csv",
    ROOT_DIR / "date" / "pay.csv",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS webhook_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    request_method TEXT,
    remote_addr TEXT,
    user_agent TEXT,
    authorization_header TEXT,
    content_type TEXT,
    event_type TEXT,
    status TEXT,
    transaction_id TEXT,
    charge_id TEXT,
    store_id TEXT,
    customer_id TEXT,
    amount INTEGER,
    currency TEXT,
    livemode INTEGER,
    raw_json TEXT NOT NULL
)
"""

INDEX_SQLS = [
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_received_at ON webhook_events(received_at)",
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_event_type ON webhook_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_webhook_events_transaction_id ON webhook_events(transaction_id)",
]



def detect_csv_path(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = ROOT_DIR / path
        return path

    for candidate in CSV_CANDIDATES:
        if candidate.exists():
            return candidate

    return CSV_CANDIDATES[0]



def to_int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip().replace(",", "")
    if stripped == "":
        return None
    try:
        return int(float(stripped))
    except ValueError:
        return None



def parse_received_at(value: str | None) -> str:
    if value:
        text = value.strip()
        if text:
            for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def parse_livemode(mode: str | None) -> int | None:
    if mode is None:
        return None
    normalized = mode.strip().lower()
    if normalized in {"live", "本番", "prod", "production"}:
        return 1
    if normalized in {"test", "sandbox", "開発"}:
        return 0
    return None



def build_insert_values(row: dict[str, str]) -> dict[str, Any]:
    return {
        "received_at": parse_received_at(row.get("イベント作成日時") or row.get("課金作成日時")),
        "request_method": "CSV_IMPORT",
        "remote_addr": None,
        "user_agent": "import_pay_csv_to_db.py",
        "authorization_header": None,
        "content_type": "text/csv",
        "event_type": (row.get("イベント") or "").strip() or None,
        "status": (row.get("課金ステータス") or row.get("返金ステータス") or "").strip() or None,
        "transaction_id": (row.get("トークンID") or "").strip() or None,
        "charge_id": (row.get("課金ID") or "").strip() or None,
        "store_id": (row.get("店舗") or "").strip() or None,
        "customer_id": (row.get("メールアドレス") or row.get("電話番号") or "").strip() or None,
        "amount": to_int_or_none(row.get("イベント金額")) or to_int_or_none(row.get("課金金額")),
        "currency": (row.get("イベント通貨") or row.get("課金通貨") or row.get("返金通貨") or "").strip() or None,
        "livemode": parse_livemode(row.get("モード")),
        "raw_json": json.dumps(row, ensure_ascii=False),
    }



def main() -> int:
    parser = argparse.ArgumentParser(description="pay.csv を webhook_events テーブルに投入します")
    parser.add_argument("--csv", dest="csv_path", help="CSVファイルパス (未指定時は data/pay.csv, date/pay.csv を探索)")
    parser.add_argument("--db", dest="db_path", default=str(DEFAULT_DB_PATH), help="SQLite DBファイルパス")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まず件数だけ確認")
    args = parser.parse_args()

    csv_path = detect_csv_path(args.csv_path)
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path

    if not csv_path.exists():
        raise FileNotFoundError(f"CSVファイルが見つかりません: {csv_path}")

    rows_to_insert: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_to_insert.append(build_insert_values(row))

    print(f"CSV: {csv_path}")
    print(f"読込件数: {len(rows_to_insert)}")

    if args.dry_run:
        print("dry-run のため DB には保存していません。")
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        for sql in INDEX_SQLS:
            conn.execute(sql)

        conn.executemany(
            """
            INSERT INTO webhook_events (
                received_at,
                request_method,
                remote_addr,
                user_agent,
                authorization_header,
                content_type,
                event_type,
                status,
                transaction_id,
                charge_id,
                store_id,
                customer_id,
                amount,
                currency,
                livemode,
                raw_json
            ) VALUES (
                :received_at,
                :request_method,
                :remote_addr,
                :user_agent,
                :authorization_header,
                :content_type,
                :event_type,
                :status,
                :transaction_id,
                :charge_id,
                :store_id,
                :customer_id,
                :amount,
                :currency,
                :livemode,
                :raw_json
            )
            """,
            rows_to_insert,
        )
        conn.commit()

    print(f"保存先DB: {db_path}")
    print(f"追加件数: {len(rows_to_insert)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
