""" AnalysisWorker â Phase 2
ÙÙØµØ© ØªØ­ÙÙÙ Ø§ÙØ£Ø³ÙÙ Ø§ÙØ³Ø¹ÙØ¯ÙØ©
Pipeline: L1 (Source) â L2 (Multi-Period) â L3 (Delta) â L4 (Claude)
Output: report_json v1.1 â contract ÙÙØ«ÙÙÙ ÙÙ report_json_schema.md
Phase 2:
- Ø¥Ø°Ø§ SAHM_API_KEY ÙÙØ¬ÙØ¯ â SahmAdapter (Ø¨ÙØ§ÙØ§Øª Ø­ÙØ©)
- ÙØ¥ÙØ§ â SeedAdapter (JSON ÙØ­ÙÙ â Phase 1 fallback)
"""
import os
import json
import re
import uuid
import datetime
from pathlib import Path

import anthropic

# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Constants
# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
POLICY_VERSION  = "1.3"
WORKER_VERSION  = "1.0"
SCHEMA_VERSION  = "1.1"
SEEDS_DIR       = Path(__file__).parent / "seeds"

VALID_DELTA_TYPES = {"STRUCTURAL", "CYCLICAL", "ONE-OFF", "ACCOUNTING", "MIX-DRIVEN"}
VALID_MAGNITUDES  = {"low", "medium", "high"}
VALID_STANCES     = {"BULLISH", "NEUTRAL_POSITIVE", "NEUTRAL", "NEUTRAL_NEGATIVE", "BEARISH", "DATA_INSUFFICIENT"}
VALID_UNIT_CODES  = {"SAR_THOUSANDS", "SAR_BILLIONS"}

STANCE_LABELS = {
    "BULLISH":           "Ø¥ÙØ¬Ø§Ø¨Ù",
    "NEUTRAL_POSITIVE":  "ÙØ­Ø§ÙØ¯ Ø¥ÙØ¬Ø§Ø¨Ù",
    "NEUTRAL":           "ÙØ­Ø§ÙØ¯",
    "NEUTRAL_NEGATIVE":  "ÙØ­Ø§ÙØ¯ Ø³ÙØ¨Ù",
    "BEARISH":           "Ø³ÙØ¨Ù",
    "DATA_INSUFFICIENT": "Ø¨ÙØ§ÙØ§Øª ØºÙØ± ÙØ§ÙÙØ©",
}

# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Exceptions
# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
class AnalysisError(Exception): pass

class SourceRecencyFailure(AnalysisError):
    """L1 BLOCKING â ÙØµØ¯Ø± ÙØ¯ÙÙ Ø£Ù ØºÙØ± ÙÙØ¬ÙØ¯."""
    pass

class AnalysisFailed(AnalysisError):
    """L4 BLOCKING â Claude API ÙØ´Ù Ø¨Ø¹Ø¯ retry."""
    pass

class SchemaValidationError(AnalysisError):
    """VR-XX â ÙØ®Ø±Ø¬ ÙØ§ ÙØ·Ø§Ø¨Ù Ø§ÙÙ schema."""
    pass

# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
# SeedAdapter â Phase 1 Data Source
# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
class SeedAdapter:
    """
    ÙÙØ±Ø£ Ø§ÙØ¨ÙØ§ÙØ§Øª ÙÙ ÙÙÙ JSON ÙØ­ÙÙ.
    ÙÙØ³ØªØ¨Ø¯Ù Ø¨Ù SahmAdapter ÙÙ Phase 2.
    """
    MAX_FILING_AGE_DAYS = 180

    def load(self, symbol: str, period: str) -> dict:
        seed_path = SEEDS_DIR / f"{symbol}.json"
        if not seed_path.exists():
            raise SourceRecencyFailure(
                f"SOURCE_NOT_FOUND: ÙØ§ ÙÙØ¬Ø¯ seed data ÙÙØ±ÙØ² {symbol}"
            )
        data = json.loads(seed_path.read_text(encoding="utf-8"))

        if data.get("period") != period:
            raise SourceRecencyFailure(
                f"PERIOD_MISMATCH: Ø§ÙØ¨ÙØ§ÙØ§Øª ÙÙÙØªØ±Ø© {data.get('period')} "
                f"ÙÙÙ Ø§ÙØ·ÙØ¨ ÙÙÙØªØ±Ø© {period}"
            )

        filing_date = datetime.date.fromisoformat(
            data["meta"]["filing_date"]
        )
        age_days = (datetime.date.today() - filing_date).days
        max_age  = int(os.environ.get("MAX_FILING_AGE_DAYS", str(self.MAX_FILING_AGE_DAYS)))
        if age_days > max_age:
            raise SourceRecencyFailure(
                f"SOURCE_RECENCY_FAILURE: ØªØ§Ø±ÙØ® Ø§ÙØ¥ÙØ¯Ø§Ø¹ {filing_date} "
                f"Ø¹ÙØ±Ù {age_days} ÙÙÙØ§Ù â ÙØªØ¬Ø§ÙØ² Ø§ÙØ­Ø¯ ({max_age} ÙÙÙ)"
            )
        return data

# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
# AnalysisWorker
# âââââââââââââââââââââââââââââââââââââââââââââââââââââ
class AnalysisWorker:

    def __init__(self, anthropic_api_key: str, data_source: str = "seed"):
        self.client = anthropic.Anthropic(api_key=anthropic_api_key)
        if os.environ.get("SAHM_API_KEY"):
            from analysis_worker.adapters.sahm_adapter import SahmAdapter
            self.adapter = SahmAdapter(api_key=os.environ["SAHM_API_KEY"])
        else:
            self.adapter = SeedAdapter()

    # ââ Main Entry Point âââââââââââââââââââââââââââ
    def run(self, symbol: str, period: str, mode: str = "FULL",
            triggered_by: str = "BATCH") -> dict:
        run_id = str(uuid.uuid4())

        try:
            from analysis_worker.adapters.sahm_adapter import SahmAPIError
        except ImportError:
            SahmAPIError = None

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
            return self._error_report(run_id, symbol, period, "SOURCE_RECENCY_FAILURE", str(e))
        except AnalysisFailed as e:
            return self._error_report(run_id, symbol, period, "ANALYSIS_FAIL", str(e))
        except Exception as e:
            if SahmAPIError is not None and isinstance(e, SahmAPIError):
                return self._error_report(run_id, symbol, period, e.code, e.message)
            return self._error_report(run_id, symbol, period, "UNEXPECTED_ERROR",
                                       f"{type(e).__name__}: {e}")

    # ââ L1: Source Load & Validation ââââââââââââââ
    def _l1_load(self, symbol: str, period: str) -> dict:
        return self.adapter.load(symbol, period)

    # ââ L2: Multi-Period Validation âââââââââââââââ
    def _l2_validate(self, raw: dict) -> dict:
        income   = raw.get("financials", {}).get("income_statement", {})
        warnings = list(raw.get("data_quality", {}).get("warnings", []))

        fields_with_base = [f for f, v in income.items() if v.get("base") is not None]
        base_available   = len(fields_with_base) > 0

        if not base_available:
            warnings.append({
                "code":    "DATA_COMPLETENESS_WARNING",
                "field":   "base_period",
                "message": f"Ø¨ÙØ§ÙØ§Øª Ø§ÙÙØªØ±Ø© Ø§ÙØ£Ø³Ø§Ø³ÙØ© ({raw['meta'].get('base_period')}) ÙÙÙÙØ¯Ø© Ø¨Ø§ÙÙØ§ÙÙ"
            })

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
                "message": f"ØªØºÙÙØ± > 20% ÙÙ {unexplained} Ø¨Ø¯ÙÙ ØªØµÙÙÙ delta"
            })

        return {
            "base_period_available":    base_available,
            "unexplained_large_deltas": unexplained,
            "warnings":                 warnings,
        }

    # ââ L3: Delta Classification ââââââââââââââââââ
    def _l3_classify_delta(self, raw: dict, l2: dict) -> dict:
        delta_seed = raw.get("delta", {})

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
            "threshold_triggered": net_income_yoy > 5,
        }

    # ââ L4: Analysis Generation (Claude API) ââââââ
    def _l4_generate(self, raw: dict, l2: dict, l3: dict) -> dict:
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
                        f"Claude API ÙØ´Ù Ø¨Ø¹Ø¯ ÙØ­Ø§ÙÙØªÙÙ: {type(e).__name__}: {e}"
                    )

    def _l4_system_prompt(self) -> str:
        return (
            "Ø£ÙØª ÙØ­ÙÙ ÙØ§ÙÙ ÙØªØ®ØµØµ ÙÙ Ø§ÙØ³ÙÙ Ø§ÙØ³Ø¹ÙØ¯Ù.\n"
            "ÙÙÙØªÙ: ØªØ­ÙÙÙ Ø§ÙØ¨ÙØ§ÙØ§Øª Ø§ÙÙØ§ÙÙØ© ÙØ¥ÙØªØ§Ø¬ ÙØ®Ø±Ø¬ JSON ÙÙØ¸Ù.\n\n"
            "Ø§ÙÙØ¨Ø±Ø©: ÙØ­Ø§ÙØ¯Ø ØªØ­ÙÙÙÙØ ÙØ¨ÙÙ Ø¹ÙÙ Ø§ÙØ£Ø±ÙØ§Ù â ÙØ§ ÙØ¨Ø§ÙØºØ© ÙÙØ§ ØªÙÙÙÙ.\n\n"
            "ÙÙØ§Ø¹Ø¯ ØµØ§Ø±ÙØ©:\n"
            "1. ÙØ§ ØªÙØµÙØ© ØµØ±ÙØ­Ø© Ø¨Ø§ÙØ´Ø±Ø§Ø¡ Ø£Ù Ø§ÙØ¨ÙØ¹ â Phase 1\n"
            "2. ÙØ§ ØªØ³ØªÙØ¯ Ø¥ÙÙ Ø£Ù Ø­ÙÙ ÙÙØ¯Ø±Ø¬ ÙÙ ÙØ³Ù 'Ø§ÙØ­ÙÙÙ ØºÙØ± Ø§ÙÙØªØ§Ø­Ø©'\n"
            "3. ÙÙ signal ÙØ³ØªÙØ¯ ÙØ±ÙÙ ÙØ­Ø¯Ø¯ ÙÙ Ø§ÙØ¨ÙØ§ÙØ§Øª Ø§ÙÙÙØ¹Ø·Ø§Ø©\n"
            "4. ÙÙ risk ÙØ³ØªÙØ¯ ÙÙØ¤Ø´Ø± ÙÙÙÙØ³ ÙÙ Ø§ÙØ¨ÙØ§ÙØ§Øª Ø§ÙÙÙØ¹Ø·Ø§Ø©\n"
            "5. analysis_text: ÙÙØ±Ø© ÙØ§Ø­Ø¯Ø© Ø¨Ø§ÙÙØºØ© Ø§ÙØ¹Ø±Ø¨ÙØ© (100-150 ÙÙÙØ©)\n"
            "6. signals: ÙÙ 2 Ø¥ÙÙ 4 Ø¹ÙØ§ØµØ± ÙÙØ· â ÙØ§ Ù­ØªÙ\n"
            "7. risks: ÙÙ 1 Ø¥ÙÙ 3 Ø¹ÙØ§ØµØ± ÙÙØ· â ÙØ±ØªØ¨Ø© ÙÙ Ø§ÙØ£Ø¹ÙÙ Ø®Ø·ÙØ±Ø© ÙÙØ£Ø¯ÙÙ\n\n"
            "ÙØ§Ø¹Ø¯Ø© DATA_INSUFFICIENT â ÙÙÙØ© Ø¬Ø¯Ø§Ù:\n"
            " Ø§Ø³ØªØ®Ø¯Ù DATA_INSUFFICIENT ÙÙØ· Ø¥Ø°Ø§ ÙØ§ÙØª Ø§ÙØ­ÙÙÙ Ø§ÙØ¬ÙÙØ±ÙØ© Ø§ÙØªØ§ÙÙØ© ÙÙÙØ§ ÙÙÙÙØ¯Ø©:\n"
            " (Ø§ÙØ¥ÙØ±Ø§Ø¯Ø§Øª + ØµØ§ÙÙ Ø§ÙØ±Ø¨Ø­ + Ø¥Ø¬ÙØ§ÙÙ Ø§ÙØ£Ø¹ÙÙ).\n"
            " Ø¥Ø°Ø§ ØªÙÙØ±Øª ÙØ°Ù Ø§ÙØ­ÙÙÙ Ø§ÙØ«ÙØ§Ø«Ø© ÙØ¹ Ø§ÙØ³Ø¹Ø± Ø§ÙØ­Ø§ÙÙ â ÙØ§ ØªØ³ØªØ®Ø¯Ù DATA_INSUFFICIENT Ø£Ø¨Ø¯Ø§Ù.\n"
            " Ø¹ÙØ¯ ØºÙØ§Ø¨ Ø¨Ø¹Ø¶ Ø§ÙÙÙØ§Ø±ÙØ§Øª Ø£Ù YoY â Ø§Ø³ØªØ®Ø¯Ù NEUTRAL Ø£Ù NEUTRAL_NEGATIVE Ø¨Ø¯ÙØ§Ù ÙÙ DATA_INSUFFICIENT.\n"
            " ØºÙØ§Ø¨ ØªØµÙÙÙ delta Ø£Ù ØªÙØ³ÙØ± Ø§ÙØ³Ø¨Ø© ÙØ§ ÙØ¨Ø±Ø± DATA_INSUFFICIENT Ø¥Ø°Ø§ ÙØ§ÙØª Ø§ÙØ£Ø±ÙØ§Ù ÙØªØ§Ø­Ø©.\n\n"
            "ÙØ§Ø¹Ø¯Ø© ANALYST DATA â ØµØ§Ø±ÙØ©:\n"
            " ÙØ§ ØªØ°ÙØ± Ø¹Ø¯Ø¯ Ø§ÙÙØ­ÙÙÙÙ Ø£Ù Ø¥Ø¬ÙØ§Ø¹ Ø§ÙÙØ­ÙÙÙÙ Ø£Ù Ø§ÙØ³Ø¹Ø± Ø§ÙÙØ³ØªÙØ¯Ù\n"
            " Ø¥ÙØ§ Ø¥Ø°Ø§ ÙØ§ÙØª ÙØ°Ù Ø§ÙØ­ÙÙÙ ÙÙØ¹Ø·Ø§Ø© ØµØ±Ø§Ø­Ø©Ù ÙÙ ÙØ³Ù 'Ø§ÙÙØ­ÙÙÙÙ' Ø¨ÙÙÙ ØºÙØ± 'ØºÙØ± ÙØ­Ø¯Ø¯' ÙØºÙØ± 'ØºÙØ± ÙØªØ§Ø­'.\n"
            " Ø¥Ø°Ø§ ÙØ§ÙØª ÙÙÙ Ø§ÙÙØ­ÙÙÙÙ ØºÙØ± ÙØªØ§Ø­Ø© â ÙØ§ ØªØ°ÙØ±ÙØ§ ÙÙØ§Ø¦ÙØ§Ù ÙÙ Ø§ÙØªØ­ÙÙÙ.\n\n"
            "Ø§ÙÙ stance ÙÙ ÙØ°Ù Ø§ÙÙÙÙ ÙÙØ·:\n"
            " BULLISH / NEUTRAL_POSITIVE / NEUTRAL / NEUTRAL_NEGATIVE / BEARISH / DATA_INSUFFICIENT\n\n"
            "Ø£ÙØªØ¬ ÙØ°Ø§ JSON ÙÙØ· â ÙØ§ ÙØµ ÙØ¨ÙÙ Ø£Ù Ø¨Ø¹Ø¯Ù:\n"
            "{\n"
            ' "stance": "...",\n'
            ' "stance_label": "...",\n'
            ' "analysis_text": "...",\n'
            ' "signals": [{"type": "positive|negative|neutral", "text": "..."}],\n'
            ' "risks": [{"severity": "high|medium|low", "text": "..."}]\n'
            "}"
        )

    def _build_l4_prompt(self, raw: dict, l2: dict, l3: dict) -> str:
        meta     = raw["meta"]
        income   = raw.get("financials", {}).get("income_statement",  {})
        balance  = raw.get("financials", {}).get("balance_sheet",     {})
        cashflow = raw.get("financials", {}).get("cash_flow",         {})

        def na(v) -> str:
            if v is None or v == "" or str(v).strip() in ("None", "null", ""):
                return "ØºÙØ± ÙØ­Ø¯Ø¯"
            return str(v)

        def fmt(field_data: dict) -> str:
            if not field_data or field_data.get("value") is None:
                return "ØºÙØ± ÙØªØ§Ø­"
            v    = field_data["value"]
            yoy  = field_data.get("yoy_pct")
            status   = field_data.get("status", "")
            note     = field_data.get("note")
            yoy_str  = f" ({yoy:+.1f}%)" if yoy is not None else ""
            tag      = " [ØªÙØ¯ÙØ±]" if status == "estimated" else ""
            note_str = f" â {note}" if note else ""
            return f"{v:,.0f}{yoy_str}{tag}{note_str}"

        def fmt_kpi(kpi_id: str) -> str:
            for c in raw.get("kpi_cards", []):
                if c.get("id") == kpi_id:
                    v = c.get("value")
                    return "ØºÙØ± ÙØªØ§Ø­" if v is None else f"{v:,.2f}"
            return "ØºÙØ± ÙØªØ§Ø­"

        current_price = fmt_kpi("current_price")
        pe_ratio      = fmt_kpi("pe_ratio")
        pb_ratio      = fmt_kpi("pb_ratio")

        ac = raw.get("analyst_consensus") or {}
        analysts_line = "ØºÙØ± ÙØªØ§Ø­"
        if isinstance(ac, dict) and ac.get("status") != "unavailable_by_plan":
            consensus = na(ac.get("consensus"))
            score     = na(ac.get("consensus_score"))
            num       = na(ac.get("num_analysts"))
            t_mean    = na(ac.get("target_mean"))
            t_high    = na(ac.get("target_high"))
            t_low     = na(ac.get("target_low"))
            analysts_line = (
                f"Ø§ÙØªÙØµÙØ©: {consensus} | Ø§ÙÙÙØ§Ø·: {score} | Ø¹Ø¯Ø¯ Ø§ÙÙØ­ÙÙÙÙ: {num} | "
                f"ÙØªÙØ³Ø· Ø§ÙØ³Ø¹Ø± Ø§ÙÙØ³ØªÙØ¯Ù: {t_mean} (Ø£Ø¹ÙÙ: {t_high} / Ø£Ø¯ÙÙ: {t_low})"
            )

        will_persist_map = {True: "ÙØ¹Ù", False: "ÙØ§", None: "ØºÙØ± ÙØ­Ø¯Ø¯"}

        discontinued_line = ""
        if income.get("net_income_discontinued", {}).get("value") is not None:
            discontinued_line = (
                f"\nØ¹ÙÙÙØ§Øª ØºÙØ± ÙØ³ØªÙØ±Ø©: {fmt(income.get('net_income_discontinued'))}"
            )

        warnings_text = (
            "\n".join(w["message"] for w in l2.get("warnings", []))
            or "ÙØ§ ØªØ­Ø°ÙØ±Ø§Øª"
        )

        missing_kpi_labels = [
            c["label"] for c in raw.get("kpi_cards", []) if c.get("status") == "missing"
        ]
        missing_kpi_text = "Ø ".join(missing_kpi_labels) if missing_kpi_labels else "ÙØ§ ØªÙØ¬Ø¯"

        return (
            f"Ø´Ø±ÙØ©: {meta['company_name']} ({meta['symbol']})\n"
            f"Ø§ÙÙØ·Ø§Ø¹: {meta['sector']} | Ø§ÙÙØªØ±Ø©: {meta['period']}\n"
            f"ØªØ§Ø±ÙØ® Ø§ÙØ¥ÙØ¯Ø§Ø¹: {meta['filing_date']} | Ø§ÙÙØ­Ø¯Ø©: {meta['unit']}\n\n"
            "ââ Ø§ÙØ³Ø¹Ø± ÙØ§ÙØªÙÙÙÙ ââ\n"
            f"Ø§ÙØ³Ø¹Ø± Ø§ÙØ­Ø§ÙÙ: {current_price} Ø±ÙØ§Ù\n"
            f"P/E: {pe_ratio}x\n"
            f"P/B: {pb_ratio}x\n\n"
            "ââ ÙØ§Ø¦ÙØ© Ø§ÙØ¯Ø®Ù ââ\n"
            f"Ø§ÙØ¥ÙØ±Ø§Ø¯Ø§Øª: {fmt(income.get('revenue'))}\n"
            f"Ø¥Ø¬ÙØ§ÙÙ Ø§ÙØ±Ø¨Ø­: {fmt(income.get('gross_profit'))}\n"
            f"Ø§ÙØ±Ø¨Ø­ Ø§ÙØªØ´ØºÙÙÙ: {fmt(income.get('operating_income'))}\n"
            f"ØµØ§ÙÙ Ø±Ø¨Ø­ ÙØ³ØªÙØ±: {fmt(income.get('net_income_continuing'))}"
            f"{discontinued_line}\n"
            f"ØµØ§ÙÙ Ø§ÙØ±Ø¨Ø­ Ø§ÙØ¥Ø¬ÙØ§ÙÙ: {fmt(income.get('net_income'))}\n\n"
            "ââ Ø§ÙÙÙØ²Ø§ÙÙØ© Ø§ÙØ¹ÙÙÙÙØ© ââ\n"
            f"Ø¥Ø¬ÙØ§ÙÙ Ø§ÙØ£ØµÙÙ: {fmt(balance.get('total_assets'))}\n"
            f"Ø­ÙÙÙ Ø§ÙÙØ³Ø§ÙÙÙÙ: {fmt(balance.get('total_equity'))}\n"
            f"Ø¥Ø¬ÙØ§ÙÙ Ø§ÙØ§ÙØªØ²Ø§ÙØ§Øª: {fmt(balance.get('total_liabilities'))}\n\n"
            "ââ Ø§ÙØªØ¯ÙÙØ§Øª Ø§ÙÙÙØ¯ÙØ© ââ\n"
            f"Ø§ÙØªØ¯ÙÙ Ø§ÙØªØ´ØºÙÙÙ: {fmt(cashflow.get('ocf'))}\n"
            f"Ø§ÙØªØ¯ÙÙ Ø§ÙØ­Ø±: {fmt(cashflow.get('free_cash_flow'))}\n\n"
            "ââ Ø§ÙÙØ­ÙÙÙÙ ââ\n"
            f"{analysts_line}\n\n"
            "ââ ØªØµÙÙÙ Delta ââ\n"
            f"Ø§ÙÙÙØ¹: {na(l3.get('type'))} | Ø§ÙØ´Ø¯Ø©: {na(l3.get('magnitude'))}\n"
            f"ÙØ§ ØªØºÙÙØ±: {na(l3.get('what'))}\n"
            f"ÙÙØ§Ø°Ø§: {na(l3.get('why'))}\n"
            f"ÙÙ ÙØ³ØªÙØ±: {will_persist_map.get(l3.get('will_persist'), 'ØºÙØ± ÙØ­Ø¯Ø¯')}\n\n"
            "ââ Ø§ÙØ­ÙÙÙ ØºÙØ± Ø§ÙÙØªØ§Ø­Ø© (ÙØ§ ØªØ³ØªÙØ¯ Ø¥ÙÙÙØ§ ÙÙ Ø§ÙØªØ­ÙÙÙ) ââ\n"
            f"{missing_kpi_text}\n\n"
            "ââ ØªØ­Ø°ÙØ±Ø§Øª ââ\n"
            f"{warnings_text}\n\n"
            "Ø­ÙÙ ÙØ°Ù Ø§ÙØ¨ÙØ§ÙØ§Øª ÙØ£ÙØªØ¬ Ø§ÙÙ JSON Ø§ÙÙØ·ÙÙØ¨."
        )

    def _parse_l4_response(self, text: str) -> dict:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise AnalysisFailed("Ø±Ø¯ Claude ÙØ§ ÙØ­ØªÙÙ JSON ØµØ­ÙØ­")
        try:
            result = json.loads(match.group())
        except json.JSONDecodeError as e:
            raise AnalysisFailed(f"JSON parse error: {e}")

        required = ["stance", "stance_label", "analysis_text", "signals", "risks"]
        missing  = [k for k in required if k not in result]
        if missing:
            raise AnalysisFailed(f"L4 response ÙØ§ÙØµ: {missing}")

        if result["stance"] not in VALID_STANCES:
            raise AnalysisFailed(f"stance ØºÙØ± ØµØ§ÙØ­: {result['stance']}")

        result["recommendation"] = None
        return result

    # ââ Report Builder ââââââââââââââââââââââââââââ
    def _build_report(self, run_id, symbol, period, mode, triggered_by,
                      raw, l2, l3, l4) -> dict:
        meta     = raw["meta"]
        income   = raw.get("financials", {}).get("income_statement",  {})
        balance  = raw.get("financials", {}).get("balance_sheet",     {})
        cashflow = raw.get("financials", {}).get("cash_flow",         {})

        all_fields      = {**income, **balance, **cashflow}
        kpi_fields      = {
            f"kpi:{c['id']}": {"status": c.get("status")}
            for c in raw.get("kpi_cards", []) if c.get("status")
        }
        all_fields_full = {**all_fields, **kpi_fields}

        confirmed  = [k for k, v in all_fields_full.items() if v.get("status") == "confirmed"]
        calculated = [k for k, v in all_fields_full.items() if v.get("status") == "calculated"]
        estimated  = [k for k, v in all_fields_full.items() if v.get("status") == "estimated"]
        missing    = [k for k, v in all_fields_full.items() if v.get("status") == "missing"]

        if estimated:
            qa_status = "PASS_WITH_ESTIMATES"
        elif any(w["code"] == "DATA_COMPLETENESS_WARNING" for w in l2.get("warnings", [])):
            qa_status = "PASS_WITH_ESTIMATES"
        else:
            qa_status = "FULL_PASS"

        return {
            "schema_version":   SCHEMA_VERSION,
            "meta": {
                **meta,
                "period":         period,
                "policy_version": POLICY_VERSION,
                "worker_version": WORKER_VERSION,
                "generated_at":   datetime.datetime.utcnow().isoformat() + "Z",
                "qa_status":      qa_status,
                "triggered_by":   triggered_by,
                "run_id":         run_id,
            },
            "kpi_cards":         raw.get("kpi_cards",         []),
            "analyst_consensus": raw.get("analyst_consensus", {}),
            "financials":        raw.get("financials",        {}),
            "delta":             l3,
            "l4_output":         l4,
            "data_quality": {
                "confirmed_fields":  confirmed,
                "calculated_fields": calculated,
                "estimated_fields":  estimated,
                "missing_fields":    missing,
                "warnings":          l2.get("warnings", []),
            },
            "provenance":        raw.get("provenance", {}),
        }

    # ââ Schema Validation (VR-01 â VR-10) âââââââââ
    def _validate_schema(self, report: dict):
        errors = []
        meta  = report.get("meta",      {})
        l4    = report.get("l4_output", {})
        delta = report.get("delta",     {})

        if not report.get("schema_version"):
            errors.append("VR-01: schema_version ÙÙÙÙØ¯")

        cards = report.get("kpi_cards", [])
        if len(cards) != 6:
            errors.append(f"VR-02: kpi_cards = {len(cards)} â ÙØ¬Ø¨ Ø£Ù ØªÙÙÙ 6")

        for card in cards:
            if card.get("status") == "missing" and card.get("value") is not None:
                errors.append(
                    f"VR-04: Ø¨Ø·Ø§ÙØ© '{card.get('id')}' â status=missing ÙÙÙ value ÙÙØ³ null"
                )

        if l4.get("recommendation") is not None:
            errors.append("VR-05: recommendation ÙØ¬Ø¨ Ø£Ù ÙÙÙÙ null ÙÙ Phase 1")

        if delta.get("type") and delta["type"] not in VALID_DELTA_TYPES:
            errors.append(f"VR-06: delta.type ØºÙØ± ØµØ§ÙØ­: {delta.get('type')}")

        if l4.get("stance") and l4["stance"] not in VALID_STANCES:
            errors.append(f"VR-07: stance ØºÙØ± ØµØ§ÙØ­: {l4.get('stance')}")

        if meta.get("unit_code") not in VALID_UNIT_CODES:
            errors.append(f"VR-08: unit_code ØºÙØ± ØµØ§ÙØ­: {meta.get('unit_code')}")

        if not meta.get("generated_at"):
            errors.append("VR-09: generated_at ÙÙÙÙØ¯")

        if not meta.get("qa_status"):
            errors.append("VR-10: qa_status ÙÙÙÙØ¯")

        if errors:
            raise SchemaValidationError(
                "Schema validation ÙØ´Ù:\n" + "\n".join(errors)
            )

    # ââ Error Report ââââââââââââââââââââââââââââââ
    def _error_report(self, run_id, symbol, period, error_code, message) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "meta": {
                "symbol":         symbol,
                "period":         period,
                "policy_version": POLICY_VERSION,
                "worker_version": WORKER_VERSION,
                "generated_at":   datetime.datetime.utcnow().isoformat() + "Z",
                "qa_status":      "FAIL",
                "run_id":         run_id,
            },
            "error": {
                "code":    error_code,
                "message": message,
            }
        }
