"""
cron_runner.py — Daily batch runner
يُشغَّل كل يوم الساعة 3:00 صباحاً عبر Railway Cron.
يقرأ قائمة الأسهم النشطة، يُشغّل Worker، يحفظ النتائج في PostgreSQL.
"""

import os
import sys
import json
import psycopg2
from datetime import datetime, timezone
from pathlib import Path

# ── إضافة مسار الـ worker ───────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from analysis_worker.worker import AnalysisWorker

# ── الأسهم المدعومة في Phase 1 ────────────────────────────────────
# يُضاف سهم جديد فقط عند توفر seed file له
STOCKS = [
    ("7010", "FY2025"),   # STC — seed جاهز
    ("2222", "FY2024"),   # أرامكو — seed مطلوب
    ("2010", "FY2024"),   # سابك — seed مطلوب
    ("1180", "FY2024"),   # البنك الأهلي السعودي (SNB) — seed مطلوب
    ("1120", "FY2024"),   # مصرف الراجحي — seed جاهز
    ("5110", "FY2024"),   # الكهرباء — seed مطلوب
]


def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def save_report(conn, symbol: str, period: str, report: dict) -> None:
    """
    يحفظ التقرير في PostgreSQL.
    عند التكرار: يُحدَّث التقرير القديم (UPSERT).
    """
    meta = report.get("meta", {})
    l4   = report.get("l4_output") or {}

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO reports
            (symbol, period, generated_at, qa_status, stance, report_json, worker_version)
        VALUES (%s, %s, NOW(), %s, %s, %s::jsonb, %s)
        ON CONFLICT (symbol, period) DO UPDATE SET
            generated_at   = NOW(),
            qa_status      = EXCLUDED.qa_status,
            stance         = EXCLUDED.stance,
            report_json    = EXCLUDED.report_json,
            worker_version = EXCLUDED.worker_version
    """, (
        symbol,
        period,
        meta.get("qa_status", "UNKNOWN"),
        l4.get("stance"),
        json.dumps(report, ensure_ascii=False),
        meta.get("worker_version"),
    ))
    conn.commit()
    cur.close()


def run():
    # ── التحقق من متغيرات البيئة ─────────────────────────────────
    api_key  = os.environ.get("ANTHROPIC_API_KEY", "")
    db_url   = os.environ.get("DATABASE_URL", "")

    if not api_key:
        print("[FATAL] ANTHROPIC_API_KEY مفقود — لا يمكن تشغيل L4")
        sys.exit(1)
    if not db_url:
        print("[FATAL] DATABASE_URL مفقود")
        sys.exit(1)

    # ── إنشاء Worker و DB connection ─────────────────────────────
    worker = AnalysisWorker(anthropic_api_key=api_key)
    conn   = get_conn()

    ts    = datetime.now(timezone.utc).isoformat()
    total = len(STOCKS)
    print(f"\n{'='*60}")
    print(f"  Cron Run — {ts}")
    print(f"  Stocks: {total}")
    print(f"{'='*60}\n")

    results = []
    for symbol, period in STOCKS:
        print(f"▶ {symbol} / {period} ...", end=" ", flush=True)
        try:
            report = worker.run(symbol, period, triggered_by="CRON")
            qa     = report.get("meta", {}).get("qa_status", "?")

            if report.get("error"):
                # error_report — يُحفظ لتوثيق الفشل لكن لا يُعرض للمستخدم
                save_report(conn, symbol, period, report)
                code = report["error"]["code"]
                print(f"FAIL [{code}]")
                results.append({"symbol": symbol, "status": "FAIL", "code": code})
            else:
                save_report(conn, symbol, period, report)
                stance = (report.get("l4_output") or {}).get("stance", "?")
                print(f"OK [{qa}] stance={stance}")
                results.append({"symbol": symbol, "status": "OK", "qa": qa})

        except Exception as e:
            print(f"ERROR [{type(e).__name__}]")
            print(f"  {e}")
            results.append({"symbol": symbol, "status": "ERROR", "error": str(e)})

    conn.close()

    # ── ملخص ────────────────────────────────────────────────────
    ok   = sum(1 for r in results if r["status"] == "OK")
    fail = total - ok
    print(f"\n{'='*60}")
    print(f"  Done: {ok}/{total} succeeded | {fail} failed")
    print(f"{'='*60}\n")

    # إذا كل الأسهم فشلت → exit code 1 لتنبيه Railway
    if ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
