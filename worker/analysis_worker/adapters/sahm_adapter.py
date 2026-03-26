"""
SahmAdapter — Phase 2 Stub
===========================
Interface مطابق لـ SeedAdapter.
في Phase 2 يتم استبدال بيانات الـ seed بجلب حي من سهمك API.

الحالة: STUB — غير مفعّل، يرفع NotImplementedError عند الاستخدام.
"""

from __future__ import annotations

from datetime import datetime


class SahmAdapter:
    """
    Adapter حي يجلب البيانات من سهمك API.

    Interface:
        load(symbol: str, period: str) -> dict
            يعيد بيانات مهيكلة متوافقة مع report_json_schema v1.1
            (نفس الشكل الذي يُنتجه SeedAdapter)

    الفرق عن SeedAdapter:
        - يجلب السعر الحالي و P/E و P/B من API لحظياً
        - يجلب القوائم المالية المحدَّثة بدلاً من JSON ثابت
        - يملأ حقول: current_price, pe_ratio, pb_ratio, ev_ebitda
        - يحدّث as_of بالوقت الفعلي للجلب

    المتطلبات (Phase 2):
        - SAHM_API_KEY في environment variables
        - حزمة: requests>=2.31.0
        - endpoint: https://api.sahm.com.sa/v1/  (تحتاج تأكيد)
    """

    # ------------------------------------------------------------------ #
    #  Phase 2 — NOT IMPLEMENTED YET                                       #
    # ------------------------------------------------------------------ #

    def __init__(self, api_key: str | None = None):
        """
        Args:
            api_key: مفتاح سهمك API.
                     إذا لم يُمرَّر يُقرأ من SAHM_API_KEY في environment.
        """
        self._api_key = api_key  # سيُستخدم في Phase 2

    def load(self, symbol: str, period: str) -> dict:
        """
        يجلب بيانات الشركة من سهمك API ويحوّلها إلى
        تنسيق report_json متوافق مع schema v1.1.

        Args:
            symbol: رمز السهم (مثال: "7010")
            period: الفترة المالية (مثال: "FY2025")

        Returns:
            dict: بيانات بتنسيق report_json (meta + kpi_cards + financials + delta + provenance)

        Raises:
            NotImplementedError: دائماً في Phase 1 (stub)
        """
        raise NotImplementedError(
            "SahmAdapter غير مفعّل في Phase 1. "
            "استخدم SeedAdapter بدلاً منه. "
            "SahmAdapter سيُفعَّل في Phase 2 عند ربط سهمك API."
        )

    # ------------------------------------------------------------------ #
    #  Helpers — سيُكمَل في Phase 2                                        #
    # ------------------------------------------------------------------ #

    def _fetch_price_data(self, symbol: str) -> dict:
        """يجلب السعر الحالي و P/E و P/B من سهمك API."""
        raise NotImplementedError

    def _fetch_financials(self, symbol: str, period: str) -> dict:
        """يجلب القوائم المالية السنوية/الربعية."""
        raise NotImplementedError

    def _map_to_schema(self, raw_price: dict, raw_financials: dict, symbol: str, period: str) -> dict:
        """
        يحوّل استجابة API الخام إلى تنسيق report_json.
        سيطبّق نفس منطق SeedAdapter لكن مع بيانات حية.
        """
        raise NotImplementedError

    def _validate_recency(self, filing_date_str: str, max_days: int = 180) -> None:
        """
        يتحقق أن تاريخ الإيداع لا يتجاوز max_days.
        نفس منطق SeedAdapter._check_filing_recency().
        """
        raise NotImplementedError


# ------------------------------------------------------------------ #
#  Phase 2 Migration Guide                                             #
# ------------------------------------------------------------------ #
"""
للتفعيل في Phase 2:

1. تثبيت الحزم:
   pip install requests python-dotenv

2. تعيين API key:
   export SAHM_API_KEY="your_key_here"
   # أو في .env:
   SAHM_API_KEY=your_key_here

3. في worker.py، استبدل:
   from analysis_worker.worker import SeedAdapter
   adapter = SeedAdapter(seeds_dir=SEEDS_DIR)

   بـ:
   from analysis_worker.adapters.sahm_adapter import SahmAdapter
   adapter = SahmAdapter()   # يقرأ SAHM_API_KEY تلقائياً

4. واجهة load() متطابقة — لا يحتاج worker.py أي تعديل آخر.

5. الحقول التي ستنتقل من missing → live:
   - current_price  (kpi_cards)
   - pe_ratio       (kpi_cards)
   - pb_ratio       (kpi_cards)
   - ev_ebitda      (kpi_cards)
   - eps            (financials.income_statement)
   - ocf / capex    (financials.cash_flow)

6. QA_STATUS المتوقع بعد التفعيل: LIVE_DATA (بدلاً من SEED_DATA)
"""
