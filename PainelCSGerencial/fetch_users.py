"""
Puxa ODBC_Users + ODBC_Roles do Lakehouse Fabric e gera users_data.json,
com o telefone normalizado pra permitir join com as respostas do NPS.
"""

import json
import re
import struct
import subprocess
import sys
import io
from pathlib import Path

import pyodbc

SQL_COPT_SS_ACCESS_TOKEN = 1256
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
OUT = ROOT / "users_data.json"
SHEETS = ROOT / "sheets_data.json"
CONFIG = json.loads((ROOT / "fabric_config.json").read_text(encoding="utf-8"))
DRIVER = "{ODBC Driver 18 for SQL Server}"


def _token() -> bytes:
    is_windows = sys.platform.startswith("win")
    out = subprocess.run(
        ["az", "account", "get-access-token",
         "--resource", "https://database.windows.net/",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True, shell=is_windows,
    )
    enc = out.stdout.strip().encode("utf-16-le")
    return struct.pack("=i", len(enc)) + enc


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", str(raw or ""))
    if len(digits) > 11 and digits.startswith("55"):
        digits = digits[2:]
    return digits


def _last10(phone: str) -> str:
    n = _normalize_phone(phone)
    return n[-10:] if len(n) >= 10 else n


# SQL Server nao tem regex; faz normalizacao com nested REPLACE
NORMALIZE_EXPR = (
    "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
    "ISNULL(u.phone,''),"
    "'(',''),')',''),'-',''),' ',''),'+',''),'.',''),'/',''),CHAR(9),'')"
)


def load_nps_phones() -> list[str]:
    if not SHEETS.exists():
        print(f"ERRO: {SHEETS} nao existe. Rode fetch_sheets.py antes.", file=sys.stderr)
        sys.exit(1)
    data = json.loads(SHEETS.read_text(encoding="utf-8"))
    phones = {_last10(r.get("telefone", "")) for r in data.get("nps", [])}
    phones.discard("")
    return sorted(phones)


def main() -> None:
    phones = load_nps_phones()
    print(f"[users] {len(phones)} telefones distintos do NPS para buscar")
    if not phones:
        OUT.write_text("[]", encoding="utf-8")
        return

    print(f"[users] conectando em {CONFIG['sql_endpoint']}")
    conn_str = (
        f"Driver={DRIVER};Server={CONFIG['sql_endpoint']},1433;"
        f"Database={CONFIG['database']};Encrypt=yes;TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _token()})
    cur = conn.cursor()

    # Processa em lotes pra evitar IN clause gigante
    all_rows = []
    batch_size = 300
    for i in range(0, len(phones), batch_size):
        batch = phones[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        sql = f"""
            SELECT
                u.id,
                u.name,
                u.lastname,
                u.email,
                u.phone,
                u.domain_id,
                r.name AS role_name,
                r.identificador AS role_identifier,
                RIGHT({NORMALIZE_EXPR}, 10) AS phone_last10
            FROM dbo.ODBC_Users u
            LEFT JOIN dbo.ODBC_Roles r ON r.id = u.role_id
            WHERE RIGHT({NORMALIZE_EXPR}, 10) IN ({placeholders})
        """
        cur.execute(sql, batch)
        all_rows.extend(cur.fetchall())
        print(f"[users] lote {i // batch_size + 1}: {len(all_rows)} users acumulados")

    cols = [d[0] for d in cur.description]
    out = []
    for r in all_rows:
        d = dict(zip(cols, r))
        phone = str(d.get("phone") or "")
        out.append({
            "id": str(d.get("id") or ""),
            "domain_id": str(d.get("domain_id") or ""),
            "name": (str(d.get("name") or "") + " " + str(d.get("lastname") or "")).strip(),
            "email": str(d.get("email") or ""),
            "telefone": phone,
            "telefone_last10": str(d.get("phone_last10") or ""),
            "cargo": str(d.get("role_name") or "") or "Sem cargo",
            "cargo_id": str(d.get("role_identifier") or ""),
        })

    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"[write] {OUT.name} ({len(out)} users encontrados)")


if __name__ == "__main__":
    main()
