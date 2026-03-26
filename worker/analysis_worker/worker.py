"""
AnalysisWorker — Phase 1
منصة تحليل الأسهم السعودية

Pipeline: L1 (Source) → L2 (Multi-Period) → L3 (Delta) → L4 (Claude)
Output:   report_json v1.1 — contract موثَّق في report_json_schema.md

Phase 1 قيود:
  - data_source = "seed"  (JSON محلي — لا سهمك API)
  - recommendation = null دائماً
  - pre-generated فقط — لا on-demand
"""

import os
import json
import re
import uuid
import datetime
from pathlib import Path

import anthropic


# ─────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────

POLICY_VERSION = "1.3"
WORKER_VERSION = "1.0"
SCHEMA_VERSION = "1.1"

SEEDS_DIR = Path(__file__).parent / "seeds"

VALID_DELTA_TYPES = {"STRUCTURAL", "CYCLICAL", "ONE-OFF", "ACCOUNTING", "MIX-DRIVEN"}
VALID_MAGNITUDES  = {"low", "medium", "high"}
VALID_STANCES     = {"BULLISH", "NEUTRAL_POSITIVE", "NEUTRAL",
                     "NEUTRAL_NEGATIVE", "BEARISH", "DATA_INSUFFICIENT"}
VALID_UNIT_CODES  = {"SAR_THOUSANDS", "SAR_BILLIONS"}

STANCE_LABELS = {
    "BULLISH":           "إيجابي",
    "NEUTRAL_POSITIVE":  "محايد إيجابي",
    "NEUTRAL":           "محايد",
    "NEUTRAL_NEGATIVE":  "محايد سلبي",
    "BEARISH":           "سلبي",
    "DATA_INSUFFICIENT": "بيانات غير كافية",
}


# ─────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────

class AnalysisError(Exception):
    pass

class SourceRecencyFailure(AnalysisError):
    """L1 BLOCKING — مصدر قديم أو غير موجود."""
    pass

class AnalysisFailed(AnalysisError):
    """L4 BLOCKING — Claude API فشل بعد retry."""
    pass

class SchemaValidationError(AnalysisError):
    """VR-XX — مخرج لا يطابق الـ schema."""
    pass


# ─────────────────────────────────────────────────────
# SeedAdapter — Phase 1 Data Source
# ─────────────────────────────────────────────────────

class SeedAdapter:
    """
    يقرأ البيانات من ملف JSON محلي.
    يُستبدل بـ SahmAdapter في Phase 2.
    """

    MAX_FILING_AGE_DAYS = 180

    def load(self, symbol: str, period: str) -> dict:
        seed_path = SEEDS_DIR / f"{symbol}.json"

        if not seed_path.exists():
            raise SourceRecencyFailure(
                f"SOURCE_NOT_FOUND: لا يوجد seed data للرمز {symbol}"
            )

        data = json.loads(seed_path.read_text(encoding="utf-8"))

        # تحقق من الفترة
        if data.get("period") != period:
            raise SourceRecencyFailure(
                f"PERIOD_MISMATCH: البيانات للفترة {data.get('period')} "
                f"لكن الطلب للفترة {period}"
            )

        # L1: تحقق من عمر الإيداع
        filing_date = datetime.date.fromisoformat(
            data["meta"]["filing_date"]
        )
        age_days = (datetime.date.today() - filing_date).days

        max_age = int(os.environ.get("MAX_FILING_AGE_DAYS", str(self.MAX_FILING_AGE_DAYS)))
        if age_days > max_age:
            raise SourceRecencyFailure(
                f"SOURCE_RECENCY_FAILURE: تاريخ الإيداع {filing_date} "
                f"عمره {age_days} يوماً — يتجاوز الحد ({max_age} يوم)"
            )

        return data


# ─────────────────────────────────────────────────────
# AnalysisWorker
# ─────────────────────────────────────────────────────

class AnalysisWorker:

    def __init__(self, anthropic_api_key: str, data_source: str = "seed"):
        self.client  = anthropic.Anthropic(api_key=anthropic_api_key)
        self.adapter = SeedAdapter()  # Phase 2: SahmAdapter()

    # ── Main Entry Point ───────────────────────────────

    def run(self, symbol: str, period: str,
            mode: str = "FULL", triggered_by: str = "BATCH") -> dict:
        """
        يُشغِّل الـ pipeline كاملاً ويُعيد report_json.
        عند الفشل الـ Blocking يُعيد error_report بدلاً من رفع exception.
        """
        run_id = str(uuid.uuid4())

        try:
            raw = self._l1_load(symbol, period)
            l2  = self._l2_validate(raw)
            l3  = self._l3_classify_delta(raw, l2)
            l4  = self._l4_generate(raw, l2, l3)

            report = self._build_report(
                run_id, symbol, period, mode, triggered_by,
                raw, l2, l3, l4
            )
            self._validate_schema(report)
            return report

        except SourceRecencyFailure as e:
            return self._error_report(run_id, symbol, period,
                                      "SOURCE_RECENCY_FAILURE", str(e))
        except AnalysisFailed as e:
            return self._error_report(run_id, symbol, period,
                                      "ANALYSIS_FAIL", str(e))

    # ── L1: Source Load & Validation ───────────────────

    def _l1_load(self, symbol: str, period: str) -> dict:
        """
        يحمِّل البيانات ويتحقق من حداثة المصدر.
        Blocking Failure إذا فشل.
        """
        return self.adapter.load(symbol, period)

    # ── L2: Multi-Period Validation ────────────────────

    def _l2_validate(self, raw: dict) -> dict:
        """
        يتحقق من اكتمال البيانات متعددة الفترات.
        Non-blocking — يُضيف warnings فقط.
        """
        income   = raw.get("financials", {}).get("income_statement", {})
        warnings = list(raw.get("data_quality", {}).get("warnings", []))

        # هل البيانات الأساسية (base) موجودة؟
        fields_with_base = [f for f, v in income.items() if v.get("base") is not None]
        base_available   = len(fields_with_base) > 0

        if not base_available:
            warnings.append({
                "code":    "DATA_COMPLETENESS_WARNING",
                "field":   "base_period",
                "message": f"بيانات الفترة الأساسية ({raw['meta'].get('base_period')}) مفقودة بالكامل"
            })

        # حقول بتغير > 20% بدون تفسير delta
        unexplained = []
        delta_seed  = raw.get("delta", {})

        for fname, fdata in income.items():
            yoy = fdata.get("yoy_pct")
            if yoy is not None and abs(yoy) > 20 and not delta_seed.get("type"):
                unexplained.append(fname)

        if unexplained:
            warnings.append({
                "code":    "ANALYSIS_QUALITY_WARNING",
                "field":   ", ".join(unexplained),
                "message": f"تغيير > 20% في {unexplained} بدون تصنيف delta"
            })

        return {
            "base_period_available":      base_available,
            "unexplained_large_deltas":   unexplained,
            "warnings":                   warnings
        }

    # ── L3: Delta Classification ───────────────────────

    def _l3_classify_delta(self, raw: dict, l2: dict) -> dict:
        """
        يقرأ التصنيف من الـ seed (Phase 1).
        Phase 2: تصنيف ديناميكي بمساعدة LLM.
        """
        delta_seed = raw.get("delta", {})

        # حساب magnitude من net_income
        income         = raw.get("financials", {}).get("income_statement", {})
        net_income_yoy = abs(income.get("net_income", {}).get("yoy_pct") or 0)

        if net_income_yoy > 50:
            magnitude = "high"
        elif net_income_yoy > 20:
            magnitude = "medium"
        else:
            magnitude = "low"

        return {
            "type":                delta_seed.get("type"),
            "magnitude":           magnitude,
            "what":                delta_seed.get("what"),
            "why":                 delta_seed.get("why"),
            "will_persist":        delta_seed.get("will_persist"),
            "confidence":          delta_seed.get("confidence", "high"),
            "threshold_triggered": net_income_yoy > 5
        }

    # ── L4: Analysis Generation (Claude API) ───────────

    def _l4_generate(self, raw: dict, l2: dict, l3: dict) -> dict:
        """
        يستدعي Claude API لتوليد:
          stance, stance_label, analysis_text, signals, risks
        recommendation = null دائماً (Phase 1).
        Retry مرة واحدة عند الفشل.
        """
        prompt = self._build_l4_prompt(raw, l2, l3)

        for attempt in range(2):
            try:
                response = self.client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=2000,
                    system=self._l4_system_prompt(),
                    messages=[{"role": "user", "content": prompt}]
                )
                return self._parse_l4_response(response.content[0].text)

            except AnalysisFailed:
                raise
            except Exception as e:
                if attempt == 1:
                    raise AnalysisFailed(
                        f"Claude API فشل بعد محاولتين: {type(e).__name__}: {e}"
                    )

    def _l4_system_prompt(self) -> str:
        return (
            "أنت محلل مالي متخصص في السوق السعودي.\n"
            "مهمتك: تحليل البيانات المالية وإنتاج مخرج JSON منظم.\n\n"
            "النبرة: محايد، تحليلي، مبني على الأرقام — لا مبالغة ولا تهوين.\n\n"
            "قواعد صارمة:\n"
            "1. لا توصية صريحة بالشراء أو البيع — Phase 1\n"
            "2. لا تستند إلى أي حقل مُدرج في قسم 'الحقول غير المتاحة'\n"
            "3. كل signal يستند لرقم محدد من البيانات المُعطاة\n"
            "4. كل risk يستند لمؤشر ملموس من البيانات المُعطاة\n"
            "5. analysis_text: فقرة واحدة باللغة العربية (100-150 كلمة)\n"
            "6. signals: من 2 إلى 4 عناصر فقط — لا حشو\n"
            "7. risks: من 1 إلى 3 عناصر فقط — مرتبة من الأعلى خطورة للأدنى\n\n"
            "الـ stance من هذه القيم فقط:\n"
            "  BULLISH / NEUTRAL_POSITIVE / NEUTRAL / NEUTRAL_NEGATIVE / BEARISH / DATA_INSUFFICIENT\n\n"
            "أنتج هذا JSON فقط — لا نص قبله أو بعده:\n"
            "{\n"
            '  "stance": "...",\n'
            '  "stance_label": "...",\n'
            '  "analysis_text": "...",\n'
            '  "signals": [{"type": "positive|negative|neutral", "text": "..."}],\n'
            '  "risks": [{"severity": "high|medium|low", "text": "..."}]\n'
            "}"
        )

    def _build_l4_prompt(self, raw: dict, l2: dict, l3: dict) -> str:
        meta   = raw["meta"]
        income = raw.get("financials", {}).get("income_statement", {})

        def fmt(field_data: dict) -> str:
            if not field_data or field_data.get("value") is None:
                return "غير متاح"
            v      = field_data["value"]
            yoy    = field_data.get("yoy_pct")
            status = field_data.get("status", "")
            note   = field_data.get("note")

            yoy_str  = f" ({yoy:+.1f}%)" if yoy is not None else ""
            tag      = " [تقدير]" if status == "estimated" else ""
            note_str = f" — {note}" if note else ""
            return f"{v:,.0f}{yoy_str}{tag}{note_str}"

        discontinued_line = ""
        if income.get("net_income_discontinued", {}).get("value") is not None:
            discontinued_line = (
                f"\nعمليات غير مستمرة:  {fmt(income.get('net_income_discontinued'))}"
            )

        warnings_text = (
            "\n".join(w["message"] for w in l2.get("warnings", []))
            or "لا تحذيرات"
        )

        # حقول kpi_cards المفقودة — لإبلاغ Claude بعدم الاستناد إليها
        missing_kpi_labels = [
            c["label"]
            for c in raw.get("kpi_cards", [])
            if c.get("status") == "missing"
        ]
        missing_kpi_text = "، ".join(missing_kpi_labels) if missing_kpi_labels else "لا توجد"

        will_persist_map = {True: "نعم", False: "لا", None: "غير محدد"}

        return (
            f"شركة: {meta['company_name']} ({meta['symbol']})\n"
            f"القطاع: {meta['sector']} | الفترة: {meta['period']}\n"
            f"تاريخ الإيداع: {meta['filing_date']} | الوحدة: {meta['unit']}\n\n"
            "── البيانات المالية ──\n"
            f"الإيرادات:           {fmt(income.get('revenue'))}\n"
            f"إجمالي الربح:        {fmt(income.get('gross_profit'))}\n"
            f"الربح التشغيلي:      {fmt(income.get('operating_income'))}\n"
            f"صافي ربح مستمر:      {fmt(income.get('net_income_continuing'))}"
            f"{discontinued_line}\n"
            f"صافي الربح الإجمالي: {fmt(income.get('net_income'))}\n\n"
            "── تصنيف Delta ──\n"
            f"النوع:       {l3.get('type')} | الشدة: {l3.get('magnitude')}\n"
            f"ما تغيّر:    {l3.get('what')}\n"
            f"لماذا:       {l3.get('why')}\n"
            f"هل يستمر:    {will_persist_map.get(l3.get('will_persist'), 'غير محدد')}\n\n"
            "── الحقول غير المتاحة (لا تستند إليها في التحليل) ──\n"
            f"{missing_kpi_text}\n\n"
            "── تحذيرات ──\n"
            f"{warnings_text}\n\n"
            "حلل هذه البيانات وأنتج الـ JSON المطلوب."
        )

    def _parse_l4_response(self, text: str) -> dict:
        """يستخرج JSON من رد Claude ويتحقق منه."""
        # استخراج JSON
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise AnalysisFailed("رد Claude لا يحتوي JSON صحيح")

        try:
            result = json.loads(match.group())
        except json.JSONDecodeError as e:
            raise AnalysisFailed(f"JSON parse error: {e}")

        # تحقق من الحقول الإلزامية
        required = ["stance", "stance_label", "analysis_text", "signals", "risks"]
        missing  = [k for k in required if k not in result]
        if missing:
            raise AnalysisFailed(f"L4 response ناقص: {missing}")

        # تحقق من stance
        if result["stance"] not in VALID_STANCES:
            raise AnalysisFailed(f"stance غير صالح: {result['stance']}")

        # Phase 1: recommendation = null دائماً
        result["recommendation"] = None
        return result

    # ── Report Builder ─────────────────────────────────

    def _build_report(self, run_id, symbol, period, mode, triggered_by,
                      raw, l2, l3, l4) -> dict:

        meta     = raw["meta"]
        income   = raw.get("financials", {}).get("income_statement", {})
        balance  = raw.get("financials", {}).get("balance_sheet", {})
        cashflow = raw.get("financials", {}).get("cash_flow", {})
        all_fields = {**income, **balance, **cashflow}

        # حقول kpi_cards — بـ prefix "kpi:" لتمييزها عن financials
        kpi_fields = {
            f"kpi:{c['id']}": {"status": c.get("status")}
            for c in raw.get("kpi_cards", [])
            if c.get("status")
        }
        all_fields_full = {**all_fields, **kpi_fields}

        # تصنيف الحقول — شامل financials + kpi_cards
        confirmed  = [k for k, v in all_fields_full.items() if v.get("status") == "confirmed"]
        calculated = [k for k, v in all_fields_full.items() if v.get("status") == "calculated"]
        estimated  = [k for k, v in all_fields_full.items() if v.get("status") == "estimated"]
        missing    = [k for k, v in all_fields_full.items() if v.get("status") == "missing"]

        # تحديد QA status
        if estimated:
            qa_status = "PASS_WITH_ESTIMATES"
        elif any(w["code"] == "DATA_COMPLETENESS_WARNING"
                 for w in l2.get("warnings", [])):
            qa_status = "PASS_WITH_ESTIMATES"
        else:
            qa_status = "FULL_PASS"

        return {
            "schema_version": SCHEMA_VERSION,
            "meta": {
                **meta,
                "period":          period,
                "policy_version":  POLICY_VERSION,
                "worker_version":  WORKER_VERSION,
                "generated_at":    datetime.datetime.utcnow().isoformat() + "Z",
                "qa_status":       qa_status,
                "triggered_by":    triggered_by,
                "run_id":          run_id,
            },
            "kpi_cards":  raw.get("kpi_cards", []),
            "financials": raw.get("financials", {}),
            "delta":      l3,
            "l4_output":  l4,
            "data_quality": {
                "confirmed_fields":  confirmed,
                "calculated_fields": calculated,
                "estimated_fields":  estimated,
                "missing_fields":    missing,
                "warnings":          l2.get("warnings", []),
            },
            "provenance": raw.get("provenance", {}),
        }

    # ── Schema Validation (VR-01 → VR-10) ─────────────

    def _validate_schema(self, report: dict):
        errors = []
        meta   = report.get("meta", {})
        l4     = report.get("l4_output", {})
        delta  = report.get("delta", {})

        if not report.get("schema_version"):
            errors.append("VR-01: schema_version مفقود")

        cards = report.get("kpi_cards", [])
        if len(cards) != 6:
            errors.append(f"VR-02: kpi_cards = {len(cards)} — يجب أن تكون 6")

        for card in cards:
            if card.get("status") == "missing" and card.get("value") is not None:
                errors.append(
                    f"VR-04: بطاقة '{card.get('id')}' — status=missing لكن value ليس null"
                )

        if l4.get("recommendation") is not None:
            errors.append("VR-05: recommendation يجب أن يكون null في Phase 1")

        if delta.get("type") and delta["type"] not in VALID_DELTA_TYPES:
            errors.append(f"VR-06: delta.type غير صالح: {delta.get('type')}")

        if l4.get("stance") and l4["stance"] not in VALID_STANCES:
            errors.append(f"VR-07: stance غير صالح: {l4.get('stance')}")

        if meta.get("unit_code") not in VALID_UNIT_CODES:
            errors.append(f"VR-08: unit_code غير صالح: {meta.get('unit_code')}")

        if not meta.get("generated_at"):
            errors.append("VR-09: generated_at مفقود")

        if not meta.get("qa_status"):
            errors.append("VR-10: qa_status مفقود")

        if errors:
            raise SchemaValidationError(
                "Schema validation فشل:\n" + "\n".join(errors)
            )

    # ── Error Report ───────────────────────────────────

    def _error_report(self, run_id, symbol, period, error_code, message) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "meta": {
                "symbol":          symbol,
                "period":          period,
                "policy_version":  POLICY_VERSION,
                "worker_version":  WORKER_VERSION,
                "generated_at":    datetime.datetime.utcnow().isoformat() + "Z",
                "qa_status":       "FAIL",
                "run_id":          run_id,
            },
            "error": {
                "code":    error_code,
                "message": message
            }
        }
