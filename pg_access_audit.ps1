# ==============================
# Postgres Access Audit (PS)
# - Lists whether you can use schema "pre"
# - Writes CSVs with schemas + readable tables
# ==============================

# --- CONFIG ---
$HostName = "enpal-msb-core-prd-msb-core-sc-postgres.postgres.database.azure.com"
$Port     = 5432
$Database = "msb-core"
$UserName = "bianca.bauer.mex"
$SslMode  = "require"

# Use the venv Python you set up in C:\work\msb_export
$PythonExe = "C:\work\msb_export\.venv\Scripts\python.exe"

# Output folder on Desktop
$OutDir = "$env:USERPROFILE\OneDrive - Enpal B.V\Desktop\pg_access_audit"

# >>> PASTE PASSWORD HERE (locally) <<<
$Password =  "vT4IwrGOUD_FTL7gjtUc!XvJRmYzWG821c5f7xjeN-FsVoX4xJL7Swh2IaBxPY28"

# --- END CONFIG ---

if ($Password -eq "PASTE_PASSWORD_HERE" -or [string]::IsNullOrWhiteSpace($Password)) {
  throw "Edit `$Password in this script first."
}

if (!(Test-Path -LiteralPath $PythonExe)) {
  throw "Python not found at: $PythonExe"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Pass connection settings via environment variables to Python
$env:PGHOST     = $HostName
$env:PGPORT     = "$Port"
$env:PGDATABASE = $Database
$env:PGUSER     = $UserName
$env:PGPASSWORD = $Password
$env:PGSSLMODE  = $SslMode
$env:OUT_DIR    = $OutDir

# Write a temporary Python file (avoids quoting issues with python -c)
$tmpPy = Join-Path $env:TEMP ("pg_access_audit_{0}.py" -f ([guid]::NewGuid().ToString("N")))

@'
import os, csv
from pathlib import Path

import psycopg

out_dir = Path(os.environ["OUT_DIR"])
out_dir.mkdir(parents=True, exist_ok=True)

schemas_csv = out_dir / "schemas_privileges.csv"
tables_csv  = out_dir / "readable_tables.csv"

q_can_use_pre = "SELECT has_schema_privilege(current_user, 'pre', 'USAGE') AS can_use_pre;"
q_schemas = """
SELECT
  nspname AS schema,
  has_schema_privilege(current_user, nspname, 'USAGE') AS can_use
FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%'
  AND nspname <> 'information_schema'
ORDER BY 1;
"""
q_tables = """
SELECT
  table_schema,
  table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND has_table_privilege(
        current_user,
        format('%I.%I', table_schema, table_name),
        'SELECT'
      )
ORDER BY 1, 2;
"""

with psycopg.connect(
    host=os.environ["PGHOST"],
    port=os.environ["PGPORT"],
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"],
    sslmode=os.environ.get("PGSSLMODE", "require"),
) as conn:
    with conn.cursor() as cur:
        cur.execute("select current_user, current_database()")
        who = cur.fetchone()

        cur.execute(q_can_use_pre)
        can_use_pre = cur.fetchone()[0]

        cur.execute(q_schemas)
        schemas = cur.fetchall()

        cur.execute(q_tables)
        tables = cur.fetchall()

with schemas_csv.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["schema", "can_use"])
    w.writerows(schemas)

with tables_csv.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["table_schema", "table_name"])
    w.writerows(tables)

print(f"Connected as: {who[0]}")
print(f"Database:      {who[1]}")
print(f"Can use schema 'pre' (USAGE)?: {can_use_pre}")
print(f"Schemas listed: {len(schemas)}")
print(f"Readable tables: {len(tables)}")
print("")
print("Wrote:")
print(" -", schemas_csv)
print(" -", tables_csv)
'@ | Set-Content -LiteralPath $tmpPy -Encoding UTF8

try {
  # Ensure psycopg exists in this venv (install if missing)
  & $PythonExe -c "import psycopg" 2>$null
  if ($LASTEXITCODE -ne 0) {
    & $PythonExe -m pip install -U --only-binary=:all: "psycopg[binary]"
    if ($LASTEXITCODE -ne 0) { throw "Failed to install psycopg[binary] into the venv." }
  }

  & $PythonExe $tmpPy
}
finally {
  # Cleanup temp file + clear password env var
  Remove-Item -LiteralPath $tmpPy -ErrorAction SilentlyContinue
  Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}
