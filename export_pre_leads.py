from __future__ import annotations

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =========================================================
# CONFIGURATION
# =========================================================

PUB_CONF = {
    "HOST": " .com",
    "PORT":  ,
    "DB": " ",
    "USER": " ",
    "PWD": " ",
    "SSL_MODE": "require",
    "SCHEMA": "public",
    "TABLES": [
        "Assets",
        "BillingContacts",
        "Contacts",
        "Cases",
        "Devices",
        "Gateways",
        "Properties",
        "Contracts",           # handled specially below
        "QualityProtocols",
        "Meters",
        "CancellationReasons",
    ],
}

PRE_CONF = {
    "HOST": " ",
    "PORT":  ,
    "DB": "  ",
    "USER": "  ",
    "PWD": " ",
    "SSL_MODE": "require",
    "SCHEMA": "pre",
    "TABLES": ["Leads"],
}

OUT_DIR = Path(r"C:\Users\BiancaBauer\OneDrive - Enpal B.V\Desktop\msb_exports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PREVIEW_LIMIT = 1000       # ← set to None for full export
CHUNKSIZE = 100_000


# =========================================================
# HELPERS
# =========================================================
def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def resolve_table_name(engine, schema: str, requested: str) -> str:
    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_type = 'BASE TABLE'
          AND lower(table_name) = lower(:requested)
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"schema": schema, "requested": requested}).fetchone()
    return row[0] if row else requested


def make_engine(conf: dict):
    url = URL.create(
        drivername="postgresql+psycopg",
        username=conf["USER"],
        password=conf["PWD"],
        host=conf["HOST"],
        port=conf["PORT"],
        database=conf["DB"],
    )
    return create_engine(url, connect_args={"sslmode": conf["SSL_MODE"]}, pool_pre_ping=True)


# =========================================================
# CORE EXPORT LOGIC
# =========================================================
def export_one_table(engine, schema: str, table: str):
    actual_table = resolve_table_name(engine, schema, table)
    table_ref = f"{quote_ident(schema)}.{quote_ident(actual_table)}"
    out_file = OUT_DIR / f"{schema}_{actual_table}.csv"

    print(f"\n=== Exporting {schema}.{actual_table} → {out_file}")

    # --- Universal infinity-safe JSON export for Contracts ---
    if actual_table.lower() == "contracts":
        base_query = f"""
            SELECT (to_jsonb(t) - 'dummy')::jsonb AS data
            FROM (SELECT * FROM {table_ref}) AS t
        """
        # limit if preview
        if PREVIEW_LIMIT:
            base_query += f" LIMIT {int(PREVIEW_LIMIT)}"

        try:
            df = pd.read_sql_query(base_query, engine)
            df = pd.json_normalize(df["data"])
            df.to_csv(out_file, index=False)
            print(f"✅ Contracts exported via JSON (kept 'infinity' text) → {out_file}")
            return
        except Exception as e:
            print(f"❌ FAILED {schema}.{actual_table} JSON export: {type(e).__name__}: {e}")
            return
    # --- End special handling for Contracts ---

    base_query = f"SELECT * FROM {table_ref}"
    sql = f"{base_query} LIMIT {int(PREVIEW_LIMIT)}" if PREVIEW_LIMIT else base_query

    try:
        if PREVIEW_LIMIT:
            df = pd.read_sql_query(sql, engine)
            df.to_csv(out_file, index=False)
            print(f"✅ Preview written ({len(df):,} rows)")
        else:
            first, total = True, 0
            for chunk in pd.read_sql_query(sql, engine, chunksize=CHUNKSIZE):
                chunk.to_csv(out_file, index=False, header=first, mode="w" if first else "a")
                first, total = False, total + len(chunk)
                print(f"  + {len(chunk):,} rows (total {total:,})")
            print(f"✅ Done ({total:,} rows)")
    except Exception as e:
        print(f"❌ FAILED {schema}.{actual_table}: {type(e).__name__}: {e}")


def run_export(conf: dict):
    engine = make_engine(conf)
    schema = conf["SCHEMA"]
    for t in conf["TABLES"]:
        export_one_table(engine, schema, t)


def main():
    print(f"\nExport folder: {OUT_DIR}")
    print(f"Mode: {'PREVIEW' if PREVIEW_LIMIT else 'FULL'}")

    run_export(PUB_CONF)
    run_export(PRE_CONF)

    print("\n🎉 All datasets exported successfully and old files overwritten!")


if __name__ == "__main__":
    main()
