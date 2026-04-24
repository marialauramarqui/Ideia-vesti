"""
Puxa faturas/parcelas de cada pedido StarkBank direto da API Vesti.
Fonte unica do CR: substitui os valores do lake nesta aba.

Depende de dados.js (gerado por fetch_data.py) pra saber quais
(workspaceId, transactionId) consultar.

Auth: VESTIAPI_TOKEN como env var (JWT bearer de servico).
"""

import concurrent.futures as cf
import io
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
DADOS_JS = ROOT / "dados.js"
OUT_JS = ROOT / "invoices.js"
API = "https://apivesti.vesti.mobi/payment/v1/starkbank/workspace/{ws}/purchase/{pur}"
MAX_WORKERS = 5

# --- Fabric VestiHouse warehouse (escrita via pyodbc + MERGE) ---
# Reaproveita connect() de fetch_data.py. Se a conexao falhar (ex: ambiente
# sem az CLI / sem FABRIC_REFRESH_TOKEN), ignora a escrita e segue com o JS.

DDL_PURCHASES = """
IF OBJECT_ID('dbo.starkbank_purchases','U') IS NULL
CREATE TABLE dbo.starkbank_purchases (
    purchase_id          VARCHAR(32)   NOT NULL,
    workspace_id         VARCHAR(32),
    order_id             VARCHAR(64),
    order_number         BIGINT,
    company_id           VARCHAR(64),
    nome_fantasia        VARCHAR(256),
    antecipacao_enabled  BIT,
    amount_cents         BIGINT,
    fee_cents            BIGINT,
    currency_code        VARCHAR(8),
    status               VARCHAR(32),
    funding_type         VARCHAR(32),
    network              VARCHAR(32),
    installment_count    INT,
    card_id              VARCHAR(32),
    card_ending          VARCHAR(8),
    holder_id            VARCHAR(64),
    holder_name          VARCHAR(256),
    holder_email         VARCHAR(256),
    holder_phone         VARCHAR(64),
    billing_city         VARCHAR(128),
    billing_state_code   VARCHAR(8),
    billing_country_code VARCHAR(8),
    billing_zip_code     VARCHAR(16),
    billing_street1      VARCHAR(256),
    billing_street2      VARCHAR(256),
    challenge_mode       VARCHAR(32),
    challenge_url        VARCHAR(512),
    end_to_end_id        VARCHAR(64),
    soft_descriptor      VARCHAR(256),
    source               VARCHAR(256),
    tags                 VARCHAR(4000),
    transaction_ids      VARCHAR(4000),
    metadata_json        VARCHAR(4000),
    api_created          DATETIME2(6),
    api_updated          DATETIME2(6),
    snapshot_at          DATETIME2(6)
);
"""

DDL_INSTALLMENTS = """
IF OBJECT_ID('dbo.starkbank_installments','U') IS NULL
CREATE TABLE dbo.starkbank_installments (
    installment_id   VARCHAR(32) NOT NULL,
    purchase_id      VARCHAR(32),
    installment_number INT,
    amount_cents     BIGINT,
    fee_cents        BIGINT,
    funding_type     VARCHAR(32),
    network          VARCHAR(32),
    status           VARCHAR(32),
    due              DATETIME2(6),
    nominal_due      DATETIME2(6),
    is_protected     BIT,
    tags             VARCHAR(4000),
    transaction_ids  VARCHAR(4000),
    api_created      DATETIME2(6),
    api_updated      DATETIME2(6),
    snapshot_at      DATETIME2(6)
);
"""

COLS_PURCHASES = [
    "purchase_id","workspace_id","order_id","order_number","company_id","nome_fantasia",
    "antecipacao_enabled","amount_cents","fee_cents","currency_code","status","funding_type",
    "network","installment_count","card_id","card_ending","holder_id","holder_name",
    "holder_email","holder_phone","billing_city","billing_state_code","billing_country_code",
    "billing_zip_code","billing_street1","billing_street2","challenge_mode","challenge_url",
    "end_to_end_id","soft_descriptor","source","tags","transaction_ids","metadata_json",
    "api_created","api_updated","snapshot_at",
]
COLS_INSTALLMENTS = [
    "installment_id","purchase_id","installment_number","amount_cents","fee_cents",
    "funding_type","network","status","due","nominal_due","is_protected","tags",
    "transaction_ids","api_created","api_updated","snapshot_at",
]


def _parse_dt(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def row_purchase(fat: dict, snap: datetime) -> tuple:
    p = fat["purchase"]
    return (
        p.get("purchaseId") or "", fat.get("workspaceId"), fat.get("orderId"),
        fat.get("orderNumber"), fat.get("companyId"), fat.get("nomeFantasia"),
        bool(fat.get("antecipacaoEnabled")), p.get("amount"), p.get("fee"),
        p.get("currencyCode"), p.get("status"), p.get("fundingType"), p.get("network"),
        p.get("installmentCount"), p.get("cardId"), p.get("cardEnding"),
        p.get("holderId"), p.get("holderName"), p.get("holderEmail"), p.get("holderPhone"),
        p.get("billingCity"), p.get("billingStateCode"), p.get("billingCountryCode"),
        p.get("billingZipCode"), p.get("billingStreetLine1"), p.get("billingStreetLine2"),
        p.get("challengeMode"), p.get("challengeUrl"), p.get("endToEndId"),
        p.get("softDescriptor"), p.get("source"),
        json.dumps(p.get("tags") or [], ensure_ascii=False)[:4000],
        json.dumps(p.get("transactionIds") or [], ensure_ascii=False)[:4000],
        json.dumps(p.get("metadata") or {}, ensure_ascii=False)[:4000],
        _parse_dt(p.get("apiCreated") or p.get("created")),
        _parse_dt(p.get("apiUpdated") or p.get("updated")),
        snap,
    )


def row_installment(i: dict, snap: datetime, num: int) -> tuple:
    return (
        i.get("id") or "", i.get("purchaseId"), num,
        i.get("amount"), i.get("fee"), i.get("fundingType"), i.get("network"),
        i.get("status"), _parse_dt(i.get("due")), _parse_dt(i.get("nominalDue")),
        bool(i.get("isProtected")),
        json.dumps(i.get("tags") or [], ensure_ascii=False)[:4000],
        json.dumps(i.get("transactionIds") or [], ensure_ascii=False)[:4000],
        _parse_dt(i.get("apiCreated") or i.get("created")),
        _parse_dt(i.get("apiUpdated") or i.get("updated")),
        snap,
    )


def upsert(conn, table: str, cols: list[str], rows: list[tuple], key: str) -> None:
    """DELETE pelas keys que vao ser reescritas + INSERT novos. Simples e
    idempotente — a cada run substitui o snapshot. Se quiser history,
    basta nao deletar e adicionar snapshot_at na PK logica."""
    if not rows:
        return
    cur = conn.cursor()
    keys = [r[cols.index(key)] for r in rows]
    # delete em lotes de 500 pra nao estourar limite de parametros
    for i in range(0, len(keys), 500):
        batch = keys[i:i+500]
        ph = ",".join("?" for _ in batch)
        cur.execute(f"DELETE FROM dbo.{table} WHERE {key} IN ({ph})", batch)
    placeholders = ",".join("?" for _ in cols)
    cur.fast_executemany = True
    cur.executemany(
        f"INSERT INTO dbo.{table} ({','.join(cols)}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()


def _wh_connect():
    import struct, subprocess
    try:
        import pyodbc
    except ImportError:
        print("[warehouse] pyodbc nao instalado", file=sys.stderr)
        return None
    SRV = "7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com"
    base = (
        f"Driver={{ODBC Driver 18 for SQL Server}};Server={SRV},1433;"
        f"Database=VestiHouse;Encrypt=yes;TrustServerCertificate=no;"
    )
    # az CLI primeiro
    try:
        out = subprocess.run(
            ["az","account","get-access-token","--resource","https://database.windows.net/","--query","accessToken","-o","tsv"],
            capture_output=True, text=True, check=True, shell=sys.platform.startswith("win"),
        )
        tok = out.stdout.strip().encode("utf-16-le")
        ts = struct.pack("=i", len(tok)) + tok
        return pyodbc.connect(base, attrs_before={1256: ts})
    except Exception:
        pass
    # fallback FABRIC_REFRESH_TOKEN
    refresh = os.environ.get("FABRIC_REFRESH_TOKEN", "").strip()
    tenant = os.environ.get("FABRIC_TENANT_ID", "").strip()
    client = os.environ.get("FABRIC_CLIENT_ID", "").strip() or "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
    if not refresh or not tenant:
        return None
    import urllib.parse
    body = urllib.parse.urlencode({
        "client_id": client, "scope":"https://database.windows.net/.default offline_access",
        "grant_type":"refresh_token", "refresh_token": refresh,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        data=body, headers={"Content-Type":"application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        tok = json.loads(resp.read().decode("utf-8"))["access_token"].encode("utf-16-le")
    ts = struct.pack("=i", len(tok)) + tok
    return pyodbc.connect(base, attrs_before={1256: ts})


def write_warehouse(faturas: list[dict]) -> None:
    try:
        conn = _wh_connect()
        if conn is None:
            print("[warehouse] skip (sem auth)", file=sys.stderr)
            return
    except Exception as e:
        print(f"[warehouse] skip (connect): {e}", file=sys.stderr)
        return
    cur = conn.cursor()
    cur.execute(DDL_PURCHASES)
    cur.execute(DDL_INSTALLMENTS)
    conn.commit()
    snap = datetime.now(timezone.utc)
    p_rows = [row_purchase(f, snap) for f in faturas if f.get("purchase")]
    i_rows = []
    for f in faturas:
        insts = sorted((f.get("purchase") or {}).get("installments") or [],
                       key=lambda x: x.get("due") or "")
        for n, inst in enumerate(insts, start=1):
            i_rows.append(row_installment(inst, snap, n))
    print(f"[warehouse] upsert {len(p_rows)} purchases, {len(i_rows)} installments")
    upsert(conn, "starkbank_purchases", COLS_PURCHASES, p_rows, "purchase_id")
    upsert(conn, "starkbank_installments", COLS_INSTALLMENTS, i_rows, "installment_id")
    conn.close()
    print("[warehouse] ok")


def load_pedidos() -> list[dict]:
    text = DADOS_JS.read_text(encoding="utf-8")
    m = re.match(r"window\.DADOS\s*=\s*(.*);\s*$", text.strip(), re.DOTALL)
    if not m:
        print("dados.js em formato inesperado", file=sys.stderr)
        sys.exit(1)
    return json.loads(m.group(1))["pedidos"]


def fetch_one(ws: str, pur: str, token: str) -> dict | None:
    req = urllib.request.Request(
        API.format(ws=ws, pur=pur),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "relatoriostarkbank/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[api] {ws}/{pur} HTTP {e.code}", file=sys.stderr)
    except Exception as e:
        print(f"[api] {ws}/{pur} erro: {e}", file=sys.stderr)
    return None


def extract(resp: dict) -> dict | None:
    if not resp or not resp.get("success"):
        return None
    pur = (resp.get("message") or {}).get("purchase") or {}
    if not pur:
        return None
    insts = []
    for i in pur.get("installments") or []:
        insts.append({
            "id": str(i.get("id") or ""),
            "amount": int(i.get("amount") or 0),
            "fee": int(i.get("fee") or 0),
            "due": i.get("due") or "",
            "status": i.get("status") or "",
            "transactionIds": i.get("transactionIds") or [],
        })
    return {
        "purchaseId": str(pur.get("id") or ""),
        "status": pur.get("status") or "",
        "amount": int(pur.get("amount") or 0),
        "fee": int(pur.get("fee") or 0),
        "installmentCount": int(pur.get("installmentCount") or 0),
        "cardEnding": pur.get("cardEnding") or "",
        "holderName": pur.get("holderName") or "",
        "created": pur.get("created") or "",
        "installments": insts,
    }


def main() -> None:
    token = os.environ.get("VESTIAPI_TOKEN", "").strip()
    if not token:
        print("ERRO: defina VESTIAPI_TOKEN", file=sys.stderr)
        sys.exit(1)
    pedidos = load_pedidos()
    # monta tarefas (workspace, transactionId) unicos por pedido
    tarefas = []
    skipped = 0
    for p in pedidos:
        ws = (p.get("workspaceId") or "").strip()
        tids = {
            (pc.get("transactionId") or "").strip()
            for pc in (p.get("parcelas") or [])
            if pc.get("transactionId")
        }
        if not ws or not tids:
            skipped += 1
            continue
        for tid in tids:
            tarefas.append({
                "workspaceId": ws,
                "transactionId": tid,
                "orderId": p["orderId"],
                "orderNumber": p.get("orderNumber"),
                "companyId": p.get("companyId", ""),
                "nomeFantasia": p.get("nomeFantasia", ""),
                "orderDate": p.get("orderDate", ""),
                "customerName": p.get("customerName", ""),
                "antecipacaoEnabled": bool(p.get("antecipacaoEnabled")),
            })
    print(f"[api] {len(tarefas)} consultas ({skipped} pedidos sem workspace/transactionId)")

    faturas: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_one, t["workspaceId"], t["transactionId"], token): t for t in tarefas}
        for fut in cf.as_completed(futs):
            t = futs[fut]
            data = extract(fut.result())
            if not data:
                continue
            faturas.append({**t, "purchase": data})

    print(f"[api] {len(faturas)} faturas obtidas")
    payload = {
        "geradoEm": datetime.now(timezone.utc).isoformat(),
        "faturas": faturas,
    }
    OUT_JS.write_text(
        "window.INVOICES = " + json.dumps(payload, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    size_kb = OUT_JS.stat().st_size / 1024
    print(f"[write] {OUT_JS.name} ({len(faturas)} faturas, {size_kb:.1f}KB)")
    # escreve tb no VestiHouse (tabelas starkbank_purchases / starkbank_installments)
    write_warehouse(faturas)


if __name__ == "__main__":
    main()
