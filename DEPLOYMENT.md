# دليل النشر على Railway — منصة تحليل الأسهم السعودية

## المتطلبات الأولية
- حساب على [railway.app](https://railway.app)
- الكود مرفوع على GitHub (أي repo خاص)
- `ANTHROPIC_API_KEY` جاهز

---

## الخطوة 1 — رفع الكود على GitHub

```bash
# من مجلد platform/
git init
git add .
git commit -m "initial: Saudi stock analysis platform MVP"
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

هيكل الملفات النهائي:
```
platform/
├── api/
│   ├── __init__.py
│   ├── database.py
│   └── main.py
├── worker/
│   ├── __init__.py
│   ├── cron_runner.py
│   └── analysis_worker/
│       ├── __init__.py
│       ├── worker.py
│       ├── seeds/
│       │   └── 7010.json
│       └── adapters/
│           ├── __init__.py
│           └── sahm_adapter.py
├── frontend/
│   └── index.html
├── requirements.txt
├── railway.toml
├── nixpacks.toml
└── Procfile
```

---

## الخطوة 2 — إنشاء PostgreSQL على Railway

1. افتح [railway.app/new](https://railway.app/new)
2. اختر **"Add a Service"** → **"Database"** → **"PostgreSQL"**
3. بعد الإنشاء، انقر على قاعدة البيانات → تبويب **"Variables"**
4. انسخ قيمة `DATABASE_URL` — ستحتاجها لاحقاً

---

## الخطوة 3 — إنشاء خدمة الـ Web (FastAPI)

1. في نفس المشروع → **"Add a Service"** → **"GitHub Repo"**
2. اربط الـ repo الذي رفعته في الخطوة 1
3. Railway سيكتشف `railway.toml` تلقائياً ويستخدم:
   - Build: nixpacks
   - Start: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`

### متغيرات البيئة للـ Web Service:
انتقل إلى **Variables** في خدمة الـ web وأضف:

| المتغير | القيمة |
|---------|--------|
| `DATABASE_URL` | (من الخطوة 2) |
| `ANTHROPIC_API_KEY` | مفتاح Claude API الخاص بك |

> Railway يضيف `PORT` تلقائياً — لا تضفه يدوياً.

---

## الخطوة 4 — إنشاء خدمة الـ Cron (Worker اليومي)

1. في نفس المشروع → **"Add a Service"** → **"GitHub Repo"**
2. اربط **نفس الـ repo** مرة ثانية
3. بعد الإنشاء، انتقل إلى **Settings** للخدمة الجديدة:
   - **Service Name**: `cron-worker`
   - **Start Command**: `python -m worker.cron_runner`
   - **Build Command**: `pip install -r requirements.txt`

4. انتقل إلى تبويب **"Cron"** في الخدمة:
   - فعّل **"Cron Job"**
   - أدخل الجدولة: `0 3 * * *` (كل يوم 3:00 صباحاً UTC = 6:00 صباحاً بتوقيت الرياض)

### متغيرات البيئة للـ Cron Service:
| المتغير | القيمة |
|---------|--------|
| `DATABASE_URL` | (نفس قيمة الخطوة 2) |
| `ANTHROPIC_API_KEY` | نفس المفتاح |

---

## الخطوة 5 — التحقق من النشر

### فحص الـ Web Service:
```
https://YOUR-APP.railway.app/health
```
الرد المتوقع:
```json
{"status": "ok", "db": "connected"}
```

### فحص قائمة الأسهم:
```
https://YOUR-APP.railway.app/api/stocks
```
يجب أن يُرجع 5 أسهم (بدون تقارير بعد).

### فتح الواجهة:
```
https://YOUR-APP.railway.app/
```
ستظهر صفحة اختر سهم مع القائمة — لن تظهر بيانات حتى يشتغل الـ Cron.

---

## الخطوة 6 — تشغيل Cron يدوياً (أول مرة)

لتوليد أول تقرير لـ STC (7010) بدون انتظار 3:00 صباحاً:

1. انتقل إلى خدمة `cron-worker` في Railway
2. انقر **"Run Now"** أو **"Trigger Run"**
3. راقب الـ Logs — يجب أن ترى:
   ```
   ▶ 7010 / FY2025 ... OK [PASS] stance=...
   Done: 1/5 succeeded | 4 failed
   ```
   > الـ 4 أسهم الأخرى ستفشل لأن seeds غير موجودة بعد — هذا متوقع.

4. بعد اكتمال التشغيل، افتح:
   ```
   https://YOUR-APP.railway.app/api/reports/7010
   ```

---

## Troubleshooting شائع

### الخطأ: `ModuleNotFoundError: No module named 'api'`
**السبب**: Railway يشغّل من الـ root الخاطئ.
**الحل**: تأكد أن Start Command يُشغَّل من مجلد `platform/` وليس المجلد الأب. في Railway Settings → **Root Directory** → اضبطه على `platform`.

### الخطأ: `DATABASE_URL not set`
**الحل**: تأكد من إضافة المتغير في Variables الخاصة بالخدمة (Web أو Cron).

### الخطأ: Cron لا يشتغل
**الحل**: Railway يحتاج الـ service أن تكون "deployed" أولاً. شغّلها مرة يدوياً ثم فعّل Cron.

### الخطأ: `seed not found` في الـ Cron logs
**السبب طبيعي**: الأسهم 2222, 2010, 1180, 5110 ليس لها seeds بعد.
**الحل**: أنشئ seed files لها في `worker/analysis_worker/seeds/`.

---

## تكلفة Railway التقديرية (Phase 1)

| الخدمة | التكلفة |
|--------|---------|
| Web Service (Hobby) | ~$5/شهر |
| PostgreSQL (Hobby) | ~$5/شهر |
| Cron Worker | ~$0-2/شهر (تشغيل يومي ≈5 دقائق) |
| **المجموع** | **~$10-12/شهر** |

---

## متغيرات البيئة الكاملة

```bash
# مطلوب لكلا الخدمتين
DATABASE_URL=postgresql://user:pass@host:5432/dbname
ANTHROPIC_API_KEY=sk-ant-...

# يُضاف تلقائياً من Railway — لا تضفه
PORT=...
```

---

## الخطوات التالية (Phase 2)

- [ ] إضافة seed files للأسهم 2222, 2010, 1180, 5110
- [ ] تفعيل SahmAdapter بديلاً عن Seeds
- [ ] إضافة صفحة dashboard بقائمة الأسهم
- [ ] إضافة Auth (Supabase أو Railway Auth)
