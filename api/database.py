"""
database.py — PostgreSQL connection + schema init
"""

import os
import psycopg2


def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def init_db():
    """
    ينشئ الجداول ويُدرج الأسهم الخمسة عند أول تشغيل.
    آمن للاستدعاء المتكرر (IF NOT EXISTS / ON CONFLICT DO NOTHING).
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";

        CREATE TABLE IF NOT EXISTS stocks (
            symbol       TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            sector       TEXT,
            is_active    BOOLEAN DEFAULT true
        );

        CREATE TABLE IF NOT EXISTS reports (
            id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol         TEXT NOT NULL REFERENCES stocks(symbol),
            period         TEXT NOT NULL,
            generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            qa_status      TEXT NOT NULL,
            stance         TEXT,
            report_json    JSONB NOT NULL,
            worker_version TEXT,
            UNIQUE (symbol, period)
        );

        CREATE INDEX IF NOT EXISTS idx_reports_symbol ON reports(symbol);
        CREATE INDEX IF NOT EXISTS idx_reports_generated ON reports(generated_at DESC);

        INSERT INTO stocks (symbol, company_name, sector) VALUES
    ('7010', 'شركة الاتصالات السعودية', 'اتصالات'),
    ('2010', 'شركة سابك',                 'بتروكيماويات'),
    ('2222', 'أرامكو السعودية',           'طاقة'),
    ('1180', 'البنك الأهلي السعودي (SNB)',  'بنوك'),
    ('1120', 'مصرف الراجحي',                'بنوك'),
    ('5110', 'شركة الكهرباء السعودية',      'طاقة كهربائية')
ON CONFLICT (symbol) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    sector       = EXCLUDED.sector;

    conn.commit()
    cur.close()
    conn.close()
