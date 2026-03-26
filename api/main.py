"""
main.py — FastAPI Backend
endpoints:
  GET /api/reports/{symbol}        → آخر تقرير ناجح للسهم
  GET /api/stocks                  → قائمة الأسهم النشطة
  GET /health                      → health check
  GET /admin/trigger/{symbol}      → تشغيل يدوي محمي بـ token
  GET /                            → frontend (stock.html)
"""

import os
import sys
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from .database import get_conn, init_db

sys.path.insert(0, str(Path(__file__).parent.parent / "worker"))
from analysis_worker.worker import AnalysisWorker

app = FastAPI(title="منصة تحليل الأسهم السعودية", version="1.0")
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

PERIOD_MAP = {
    "7010": "FY2025",
    "1120": "FY2024",
    "2222": "FY2024",
    "2010": "FY2024",
    "1180": "FY2024",
    "5110": "FY2024",
}


@app.on_event("startup")
def startup():
    try:
        init_db()
        print("[DB] Schema initialized")
    except Exception as e:
        print(f"[DB] Init warning: {e}")


@app.get("/api/reports/{symbol}")
def get_report(symbol: str):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT report_json, generated_at
        FROM   reports
        WHERE  symbol = %s
          AND  report_json->>'error' IS NULL
          AND  qa_status != 'FAIL'
        ORDER BY generated_at DESC
        LIMIT 1
    """, (symbol.upper(),))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"لا يوجد تقرير ناجح للرمز {symbol} — جاري التحليل"
        )
    report, generated_at = row
    report["_fetched_at"] = generated_at.isoformat() if generated_at else None
    return report


@app.get("/api/stocks")
def get_stocks():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            s.symbol, s.company_name, s.sector,
            r.qa_status, r.stance, r.generated_at
        FROM stocks s
        LEFT JOIN LATERAL (
            SELECT qa_status, stance, generated_at
            FROM   reports
            WHERE  symbol = s.symbol
              AND  report_json->>'error' IS NULL
            ORDER BY generated_at DESC
            LIMIT 1
        ) r ON true
        WHERE s.is_active = true
        ORDER BY s.symbol
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "symbol":       r[0],
            "company_name": r[1],
            "sector":       r[2],
            "qa_status":    r[3],
            "stance":       r[4],
            "last_report":  r[5].isoformat() if r[5] else None,
        }
        for r in rows
    ]


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "degraded", "db": str(e)}


@app.get("/admin/trigger/{symbol}")
def trigger_analysis(symbol: str, token: str = ""):
    expected = os.environ.get("MANUAL_TRIGGER_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set")
    sym    = symbol.upper()
    period = PERIOD_MAP.get(sym, "FY2024")
    worker = AnalysisWorker(anthropic_api_key=api_key)
    report = worker.run(sym, period, triggered_by="MANUAL")
    meta = report.get("meta", {})
    l4   = report.get("l4_output") or {}
    conn = get_conn()
    cur  = conn.cursor()
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
        sym, period,
        meta.get("qa_status", "UNKNOWN"),
        l4.get("stance"),
        json.dumps(report, ensure_ascii=False),
        meta.get("worker_version"),
    ))
    conn.commit()
    cur.close()
    conn.close()
    return {
        "symbol":    sym,
        "period":    period,
        "qa_status": meta.get("qa_status"),
        "stance":    l4.get("stance"),
        "error":     report.get("error"),
    }


@app.get("/admin/raw-keys/{symbol}")
def raw_keys(symbol: str, token: str = ""):
    expected = os.environ.get("MANUAL_TRIGGER_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    sahm_key = os.environ.get("SAHM_API_KEY", "")
    if not sahm_key:
        raise HTTPException(status_code=500, detail="SAHM_API_KEY not set")
    from sahmk import SahmkClient
    client = SahmkClient(sahm_key)
    sym = symbol.upper()
    result = {}
    try:
        q = client.quote(sym)
        result["quote_keys"] = list(q.keys()) if isinstance(q, dict) else str(type(q))
    except Exception as e:
        result["quote_keys"] = f"ERROR: {e}"
    try:
        c = client.company(sym)
        result["company_keys"] = list(c.keys()) if isinstance(c, dict) else str(type(c))
        # تحقق من nested dicts
        result["company_nested"] = {
            k: list(v.keys()) if isinstance(v, dict) else type(v).__name__
            for k, v in (c.items() if isinstance(c, dict) else {}.items())
        }
    except Exception as e:
        result["company_keys"] = f"ERROR: {e}"
    try:
        f = client.financials(sym)
        result["financials_keys"] = list(f.keys()) if isinstance(f, dict) else str(type(f))
        if isinstance(f, dict):
            for section in ["income_statement", "balance_sheet", "cash_flow",
                            "cashflow", "cashflow_statement", "income", "balance"]:
                if section in f:
                    v = f[section]
                    result[f"financials_{section}_keys"] = (
                        list(v.keys()) if isinstance(v, dict) else str(type(v))
                    )
    except Exception as e:
        result["financials_keys"] = f"ERROR: {e}"
    try:
        d = client.dividends(sym)
        result["dividends_keys"] = list(d.keys()) if isinstance(d, dict) else str(type(d))
    except Exception as e:
        result["dividends_keys"] = f"ERROR: {e}"
    return result


@app.get("/admin/raw-structure/{symbol}")
def raw_structure(symbol: str, token: str = ""):
    """
    Debug endpoint مؤقت — يكشف النوع الفعلي (dict/list) لكل قسم في financials + company.
    الهدف: حسم هل financials nested as list أو dict، وهل يوجد period fields.
    """
    expected = os.environ.get("MANUAL_TRIGGER_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    sahm_key = os.environ.get("SAHM_API_KEY", "")
    if not sahm_key:
        raise HTTPException(status_code=500, detail="SAHM_API_KEY not set")
    from sahmk import SahmkClient
    client = SahmkClient(sahm_key)
    sym = symbol.upper()

    def _describe(val):
        """يصف قيمة: نوعها، وإذا كانت list يعطي تفاصيل العناصر."""
        t = type(val).__name__
        if isinstance(val, dict):
            return {"type": "dict", "keys": list(val.keys())}
        if isinstance(val, list):
            result = {"type": "list", "length": len(val)}
            if len(val) > 0:
                first = val[0]
                result["first_item_type"] = type(first).__name__
                if isinstance(first, dict):
                    result["first_item_keys"] = list(first.keys())
                    # هل فيه حقول period؟
                    period_fields = {k: first[k] for k in first
                                     if any(p in k.lower() for p in
                                            ["period", "year", "fiscal", "date", "quarter"])}
                    result["first_item_period_fields"] = period_fields
            if len(val) > 1:
                second = val[1]
                if isinstance(second, dict):
                    result["second_item_keys"] = list(second.keys())
            return result
        return {"type": t, "value_preview": str(val)[:100]}

    result = {}

    # ── company ──────────────────────────────────────────────────────
    try:
        c = client.company(sym)
        result["company_type"] = type(c).__name__
        if isinstance(c, dict):
            result["company_top_keys"] = list(c.keys())
            # فحص كل قسم nested
            for section in ["fundamentals", "analysts", "valuation",
                             "technicals", "dividends", "analyst_consensus",
                             "consensus", "price_ratios"]:
                if section in c:
                    result[f"company__{section}"] = _describe(c[section])
    except Exception as e:
        result["company_error"] = str(e)

    # ── financials ───────────────────────────────────────────────────
    try:
        f = client.financials(sym)
        result["financials_type"] = type(f).__name__
        if isinstance(f, dict):
            result["financials_top_keys"] = list(f.keys())
            # فحص المفاتيح الجمع والمفردة
            for section in ["income_statements", "income_statement",
                             "balance_sheets",   "balance_sheet",
                             "cash_flows",        "cash_flow",
                             "cashflow",          "cashflow_statement"]:
                if section in f:
                    result[f"financials__{section}"] = _describe(f[section])
    except Exception as e:
        result["financials_error"] = str(e)

    return result


if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
