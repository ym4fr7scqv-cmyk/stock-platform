"""
main.py — FastAPI Backend
endpoints:
  GET /api/reports/{symbol}   → آخر تقرير ناجح للسهم
  GET /api/stocks             → قائمة الأسهم النشطة
  GET /health                 → health check
  GET /                       → frontend (stock.html)
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from .database import get_conn, init_db

app = FastAPI(title="منصة تحليل الأسهم السعودية", version="1.0")
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@app.on_event("startup")
def startup():
    """تهيئة قاعدة البيانات عند أول تشغيل."""
    try:
        init_db()
        print("[DB] Schema initialized")
    except Exception as e:
        print(f"[DB] Init warning: {e}")


# ── API Endpoints ──────────────────────────────────────────────────

@app.get("/api/reports/{symbol}")
def get_report(symbol: str):
    """
    يُرجع آخر تقرير ناجح للسهم.
    - إذا لا يوجد تقرير: 404
    - إذا آخر تقرير فاشل (error_report): يُرجع آخر نسخة ناجحة مع تحذير
    """
    conn = get_conn()
    cur  = conn.cursor()

    # أولاً: آخر تقرير ناجح (بدون error)
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

    # إضافة generated_at كـ top-level للـ frontend
    report["_fetched_at"] = generated_at.isoformat() if generated_at else None
    return report


@app.get("/api/stocks")
def get_stocks():
    """قائمة الأسهم النشطة مع حالة آخر تقرير."""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            s.symbol,
            s.company_name,
            s.sector,
            r.qa_status,
            r.stance,
            r.generated_at
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
    """Railway health check."""
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "degraded", "db": str(e)}


# ── Frontend ───────────────────────────────────────────────────────
# يجب أن تكون آخر route حتى لا تتعارض مع /api/*
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
