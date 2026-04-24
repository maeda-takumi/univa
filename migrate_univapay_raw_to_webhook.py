#!/usr/bin/env python3
"""webhook_raw_events から payment_facts を再生成する。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT_DIR / "data" / "univapay_webhook.sqlite"

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
        (("payment", "charge", "capture"), "売上"),
    ]

    for keys, out in mapping:
        if any(k in n for k in keys):
            return out
    return raw


def to_jst(value: str | None) -> str:
    jst = timezone(timedelta(hours=9))
    try:
        from zoneinfo import ZoneInfo
        jst = ZoneInfo("Asia/Tokyo")
    except Exception:
        jst = timezone(timedelta(hours=9))
    if value:
        t = value.strip()
        if t:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(t, fmt).replace(tzinfo=timezone.utc)
                    return dt.astimezone(jst).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
            try:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                return dt.astimezone(jst).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def payload_value(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="webhook_raw_events から payment_facts を再構築")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--truncate-facts", action="store_true", help="payment_facts から WEBHOOK ソースを先に削除")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM webhook_raw_events ORDER BY id ASC").fetchall()

        print(f"DB: {db_path}")
        print(f"対象Webhook件数: {len(rows)}")
        if args.dry_run:
            print("dry-run のため書き込みなし")
            return 0

        if args.truncate_facts:
            conn.execute("DELETE FROM payment_facts WHERE source = 'WEBHOOK'")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        inserted = 0
        for row in rows:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            if not isinstance(payload, dict):
                payload = {}

            occurred = payload_value(payload, "入金日", "イベント作成日時", "課金作成日時") or row["received_at"]
            payer_name = payload_value(payload, "入金者名", "氏名", "カード名義")
            email = payload_value(payload, "メールアドレス", "email")
            amount = None
            raw_amount = row["amount_raw"]
            if raw_amount is not None:
                try:
                    amount = int(float(str(raw_amount).replace(",", "")))
                except ValueError:
                    amount = None

            conn.execute(
                """
                INSERT INTO payment_facts (
                    source, source_event_id, payment_date_jst, payer_name, amount, email,
                    event_type_norm, status_norm, raw_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_event_id, payment_date_jst) DO UPDATE SET
                    payment_date_jst=excluded.payment_date_jst,
                    payer_name=excluded.payer_name,
                    amount=excluded.amount,
                    email=excluded.email,
                    event_type_norm=excluded.event_type_norm,
                    status_norm=excluded.status_norm,
                    raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                (
                    "WEBHOOK",
                    row["id"],
                    to_jst(occurred),
                    payer_name,
                    amount,
                    email,
                    normalize_event(row["event_type_raw"]),
                    normalize_status(row["status_raw"]),
                    row["payload_json"],
                    now,
                    now,
                ),
            )
            inserted += 1

        conn.commit()
        print(f"反映件数: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
