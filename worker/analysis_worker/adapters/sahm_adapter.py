"""
SahmAdapter — Phase 2
=====================
يجلب بيانات حية من سهمك API ويحوّلها إلى تنسيق report_json v1.1.
Interface مطابق لـ SeedAdapter — لا يحتاج worker.py أي تعديل آخر.

المتطلبات:
    pip install sahmk==0.2.1
    SAHM_API_KEY في environment variables

الأخطاء المُعالَجة:
    401 → INVALID_API_KEY
    403 → PLAN_LIMIT
    429 → RATE_LIMIT
    network/timeout → NETWORK_ERROR
"""

from __future__ import annotations

import logging
import os
import datetime
from typing import Any

log = logging.getLogger(__name__)


# ── Custom Errors ──────────────────────────────────────────────────

class SahmAPIError(Exception):
    """خطأ من سهمك API — يحمل code ورسالة."""
    def __init__(self, code: str, message: str):
        self.code    = code
        self.message = message
        super().__init__(f"[{code}] {message}")


# ── Constants ──────────────────────────────────────────────────────

SAHMK_SOURCE     = "sahmk_api_v1"
CONSENSUS_FIELDS = {"buy", "hold", "sell", "target_price", "recommendation"}


# ── Helpers ───────────────────────────────────────────────────────

def _safe_float(d: dict, *keys: str, default=None):
    """يجرب مفاتيح متعددة ويُعيد أول قيمة رقمية."""
    for key in keys:
        val = d.get(key) if isinstance(d, dict) else None
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return default


def _field(value, base=None, yoy_pct=None,
           status=None, note=None, source=SAHMK_SOURCE, as_of=None):
    # إذا لم يُمرَّر status صريح: None تعني missing، غير ذلك confirmed
    if status is None:
        status = "missing" if value is None else "confirmed"
    return {
        "value":   value,
        "base":    base,
        "yoy_pct": yoy_pct,
        "status":  status,
        "note":    note,
        "source":  source,
        "as_of":   as_of,
    }


def _missing(note=None):
    return _field(None, status="missing", note=note, source=None)


def _kpi(id_, label, value, unit, yoy_pct=None,
         status="confirmed", source=SAHMK_SOURCE, as_of=None):
    return {
        "id":      id_,
        "label":   label,
        "value":   value,
        "unit":    unit,
        "yoy_pct": yoy_pct,
        "status":  status,
        "source":  source,
        "as_of":   as_of,
    }


def _yoy(current, prior):
    if current is None or prior is None or prior == 0:
        return None
    return round((current - prior) / abs(prior) * 100, 1)


def _sub(a, b):
    if a is not None and b is not None:
        return a - b
    return None


# ── SahmAdapter ───────────────────────────────────────────────────

class SahmAdapter:
    """
    Adapter حي يجلب البيانات من سهمك API.
    Interface مطابق لـ SeedAdapter.
    """

    def __init__(self, api_key: str | None = None):
        key = api_key or os.environ.get("SAHM_API_KEY", "")
        if not key:
            raise SahmAPIError(
                "MISSING_API_KEY",
                "SAHM_API_KEY غير موجود في environment variables"
            )
        try:
            from sahmk import SahmkClient
            self._client = SahmkClient(key)
            log.info("[SahmAdapter] تم تهيئة SahmkClient")
        except ImportError:
            raise SahmAPIError(
                "MISSING_DEPENDENCY",
                "مكتبة sahmk غير مثبتة — شغّل: pip install sahmk==0.2.1"
            )

    # ── Main Entry Point ──────────────────────────────────────────

    def load(self, symbol: str, period: str) -> dict:
        """يجلب البيانات ويحوّلها إلى report_json v1.1."""
        log.info(f"[SahmAdapter] {symbol} / {period}")
        price_data   = self._fetch_price_data(symbol)
        company_data = self._fetch_company(symbol)
        fin_data     = self._fetch_financials(symbol, period)
        return self._map_to_schema(symbol, period, price_data, company_data, fin_data)

    # ── Fetch Methods ─────────────────────────────────────────────

    def _fetch_price_data(self, symbol: str) -> dict:
        """يجلب السعر اللحظي — يعيد {} عند الفشل."""
        try:
            data = self._client.quote(symbol)
            log.debug(f"[quote] {symbol}: {data}")
            return data if isinstance(data, dict) else {}
        except Exception as e:
            return self._handle_api_error(e, "quote", symbol, fatal=False)

    def _fetch_company(self, symbol: str) -> dict:
        """
        يجلب fundamentals من company endpoint.
        Starter: P/E, P/B, EPS, book value, beta.
        Pro فقط: analyst_consensus.
        """
        try:
            data = self._client.company(symbol)
            log.debug(f"[company] {symbol}: keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
            return data if isinstance(data, dict) else {}
        except Exception as e:
            return self._handle_api_error(e, "company", symbol, fatal=False)

    def _fetch_financials(self, symbol: str, period: str) -> dict:
        """
        يجلب القوائم المالية.
        الـ API يُعيد: {income_statement: {...}, balance_sheet: {...}}

        ملاحظة: client.financials() لا يقبل period كمعامل —
        البيانات المُعادة هي آخر فترة متاحة في API.
        حقل period في المخرج = requested_period فقط، لا يُمثّل تحققاً فعلياً.
        """
        try:
            data = self._client.financials(symbol)
            log.debug(f"[financials] {symbol}: keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
            if not isinstance(data, dict):
                log.warning(f"[financials] تنسيق غير متوقع: {type(data)}")
                return {}
            return data
        except Exception as e:
            return self._handle_api_error(e, "financials", symbol, fatal=True)

    # ── Schema Mapping ────────────────────────────────────────────

    def _map_to_schema(self, symbol: str, period: str,
                       price: dict, company: dict, fin: dict) -> dict:
        """يحوّل استجابات API إلى report_json v1.1."""

        today_str = datetime.date.today().isoformat()
        warnings  = []

        # ── meta ──────────────────────────────────────────────────
        company_name = (
            price.get("name") or
            company.get("name") or
            company.get("company_name") or
            symbol
        )
        sector = (
            company.get("sector") or
            company.get("industry") or
            "غير محدد"
        )

        meta = {
            "symbol":       symbol,
            "company_name": company_name,
            "sector":       sector,
            "sector_type":  self._guess_sector_type(sector),
            "period":       period,
            "period_type":  "annual",
            "filing_date":  today_str,
            "filing_type":  "live_api",
            "source_id":    f"sahmk_{symbol}_{today_str}",
            "source_url":   "https://app.sahmk.sa/api/v1/",
            "unit":         "ريال سعودي",
            "unit_code":    "SAR_THOUSANDS",
            "base_period":  self._prior_period(period),
        }

        # ── price & valuation ─────────────────────────────────────
        current_price = _safe_float(price, "price", "last_price", "close")

        fundamentals = company.get("fundamentals") or company.get("valuation") or {}
        pe  = _safe_float(company, "pe_ratio", "pe", "price_to_earnings") or \
              _safe_float(fundamentals, "pe_ratio", "pe")
        pb  = _safe_float(company, "pb_ratio", "pb", "price_to_book")     or \
              _safe_float(fundamentals, "pb_ratio", "pb")
        eps = _safe_float(company, "eps", "earnings_per_share")           or \
              _safe_float(fundamentals, "eps")

        # ── analyst_consensus — Pro فقط ───────────────────────────
        consensus_raw = company.get("analyst_consensus") or company.get("consensus") or {}
        has_consensus = any(consensus_raw.get(f) is not None for f in CONSENSUS_FIELDS)
        analyst_consensus_field = (
            consensus_raw if has_consensus
            else {
                "status":         "unavailable_by_plan",
                "reason":         "يحتاج Pro plan — غير متاح في الخطة الحالية",
                "buy":            None,
                "hold":           None,
                "sell":           None,
                "target_price":   None,
                "recommendation": None,
            }
        )
        if not has_consensus:
            warnings.append({
                "code":    "PLAN_LIMITATION",
                "field":   "analyst_consensus",
                "message": "إجماع المحللين غير متاح في الخطة الحالية (unavailable_by_plan) — يحتاج Pro plan"
            })

        # ── financials ────────────────────────────────────────────
        # financials() يُعيد مباشرة: {income_statement:{...}, balance_sheet:{...}}
        inc = fin.get("income_statement") or {}
        bal = fin.get("balance_sheet")    or {}
        cf  = fin.get("cash_flow")        or fin.get("cashflow") or {}

        # income — الحقول المؤكدة من API: revenue, net_income, eps
        revenue_v   = _safe_float(inc, "revenue", "total_revenue", "total_income")
        op_income_v = _safe_float(inc, "operating_income", "ebit", "operating_profit")
        ni_v        = _safe_float(inc, "net_income", "net_profit", "profit_after_tax")
        eps         = eps or _safe_float(inc, "eps", "earnings_per_share")

        # prior year — غير متاح مباشرة من API في الإصدار الحالي
        revenue_b   = None
        op_income_b = None
        ni_b        = None

        # balance — الحقول المؤكدة: total_assets, total_equity
        assets_v = _safe_float(bal, "total_assets", "assets")
        equity_v = _safe_float(bal, "total_equity", "shareholders_equity", "equity")
        liab_v   = _safe_float(bal, "total_liabilities", "liabilities")
        assets_b = None
        equity_b = None

        ocf_v   = _safe_float(cf, "operating_cash_flow", "ocf", "cash_from_operations")
        capex_v = _safe_float(cf, "capex", "capital_expenditure", "capital_expenditures")

        revenue_yoy = _yoy(revenue_v, revenue_b)
        ni_yoy      = _yoy(ni_v,      ni_b)
        assets_yoy  = _yoy(assets_v,  assets_b)
        equity_yoy  = _yoy(equity_v,  equity_b)

        # حسابات صريحة — is not None لا truthy check
        net_margin = (
            round(ni_v / revenue_v * 100, 1)
            if ni_v is not None and revenue_v is not None and revenue_v != 0
            else None
        )
        roe = None
        if ni_v is not None and equity_v is not None and equity_b is not None:
            avg_eq = (equity_v + equity_b) / 2
            roe = round(ni_v / avg_eq * 100, 1) if avg_eq != 0 else None
        elif ni_v is not None and equity_v is not None and equity_v != 0:
            roe = round(ni_v / equity_v * 100, 1)

        if revenue_v is None:
            warnings.append({
                "code":    "DATA_COMPLETENESS_WARNING",
                "field":   "income_statement.revenue",
                "message": "الإيرادات غير متاحة من API"
            })

        # ── kpi_cards (6 بالترتيب) ────────────────────────────────
        kpi_cards = [
            _kpi("current_price", "السعر الحالي",
                 current_price, "ريال",
                 status="confirmed" if current_price is not None else "missing",
                 as_of=today_str),

            _kpi("pe_ratio", "P/E",
                 pe, "x",
                 status="confirmed" if pe is not None else "missing",
                 as_of=today_str),

            _kpi("pb_ratio", "P/B",
                 pb, "x",
                 status="confirmed" if pb is not None else "missing",
                 as_of=today_str),

            _kpi("revenue_growth", "نمو الإيرادات",
                 revenue_yoy, "%",
                 yoy_pct=revenue_yoy,
                 status="calculated" if revenue_yoy is not None else "missing"),

            _kpi("net_margin", "هامش صافي الربح",
                 net_margin, "%",
                 status="calculated" if net_margin is not None else "missing"),

            _kpi("roe", "العائد على حقوق الملكية (ROE)",
                 roe, "%",
                 status="calculated" if roe is not None else "missing"),
        ]

        # ── financials dict ───────────────────────────────────────
        liab_calc = liab_v or _sub(assets_v, equity_v)
        liab_status = (
            "confirmed"  if liab_v                             else
            "calculated" if assets_v is not None and equity_v is not None else
            "missing"
        )

        financials = {
            "income_statement": {
                "revenue":                 _field(revenue_v,   revenue_b,   revenue_yoy),
                "gross_profit":            _missing("غير متاح مباشرة من API"),
                "operating_income":        _field(op_income_v, op_income_b, _yoy(op_income_v, op_income_b)),
                "net_income_continuing":   _field(ni_v,        ni_b,        ni_yoy),
                "net_income_discontinued": _missing("لا عمليات متوقفة"),
                "net_income":              _field(ni_v,        ni_b,        ni_yoy),
                "eps":                     _field(eps),
            },
            "balance_sheet": {
                "total_assets":      _field(assets_v, assets_b, assets_yoy),
                "total_equity":      _field(equity_v, equity_b, equity_yoy),
                "total_liabilities": _field(liab_calc, status=liab_status),
            },
            "cash_flow": {
                "ocf":   _field(ocf_v),
                "capex": _field(capex_v),
            },
        }

        # ── delta (يُعالَج في L3) ─────────────────────────────────
        delta = {
            "type":         None,
            "what":         None,
            "why":          None,
            "will_persist": None,
            "confidence":   "medium",
        }

        # ── provenance ────────────────────────────────────────────
        provenance = {
            "primary_source":  SAHMK_SOURCE,
            "source_type":     "live_api",
            "fallback_used":   False,
            "fetched_at":      today_str,
            "endpoints_used":  ["quote", "company", "financials"],
            "consensus_note":  "unavailable_by_plan" if not has_consensus else "available",
            "period_note":     "requested_period_only — API does not filter by period",
        }

        return {
            "symbol":            symbol,
            "period":            period,
            "meta":              meta,
            "kpi_cards":         kpi_cards,
            "financials":        financials,
            "analyst_consensus": analyst_consensus_field,
            "delta":             delta,
            "data_quality":      {"warnings": warnings},
            "provenance":        provenance,
        }

    # ── Error Handler ─────────────────────────────────────────────

    def _handle_api_error(self, exc: Exception, endpoint: str,
                          symbol: str, fatal: bool = False) -> dict:
        """
        يُصنّف الخطأ — يرفع SahmAPIError للأخطاء الحرجة.
        يعيد {} للأخطاء غير الحرجة (graceful degradation).
        """
        exc_str  = str(exc).lower()
        exc_type = type(exc).__name__

        if "401" in exc_str or "unauthorized" in exc_str:
            raise SahmAPIError("INVALID_API_KEY",
                               f"مفتاح API غير صالح — endpoint: {endpoint}")

        if "429" in exc_str or "rate limit" in exc_str:
            raise SahmAPIError("RATE_LIMIT",
                               f"تجاوز حد الطلبات — حاول لاحقاً")

        if "403" in exc_str or "forbidden" in exc_str or "plan" in exc_str:
            log.warning(f"[{endpoint}:{symbol}] PLAN_LIMIT — endpoint غير متاح في الخطة الحالية")
            if fatal:
                raise SahmAPIError("PLAN_LIMIT",
                                   f"endpoint {endpoint} يحتاج باقة أعلى")
            return {}

        if "timeout" in exc_str or "connection" in exc_str:
            raise SahmAPIError("NETWORK_ERROR",
                               f"خطأ في الشبكة — {endpoint}: {exc_type}")

        log.warning(f"[{endpoint}:{symbol}] {exc_type}: {str(exc)[:200]}")
        if fatal:
            raise SahmAPIError("UNEXPECTED_ERROR", f"{endpoint}: {exc_type}: {str(exc)[:100]}")
        return {}

    # ── Utilities ─────────────────────────────────────────────────

    @staticmethod
    def _guess_sector_type(sector: str) -> str:
        s = (sector or "").lower()
        if any(w in s for w in ["بنك", "مصرف", "bank", "banking"]):
            return "banking"
        if any(w in s for w in ["تأمين", "insurance"]):
            return "insurance"
        return "standard"

    @staticmethod
    def _prior_period(period: str) -> str:
        try:
            year = int(period.replace("FY", "").strip())
            return f"FY{year - 1}"
        except (ValueError, AttributeError):
            return ""
