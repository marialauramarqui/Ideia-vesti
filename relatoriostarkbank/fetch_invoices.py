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
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
DADOS_JS = ROOT / "dados.js"
OUT_JS = ROOT / "invoices.js"
API = "https://apivesti.vesti.mobi/payment/v1/starkbank/workspace/{ws}/purchase/{pur}"
MAX_WORKERS = 5


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
    from datetime import datetime, timezone
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


if __name__ == "__main__":
    main()
