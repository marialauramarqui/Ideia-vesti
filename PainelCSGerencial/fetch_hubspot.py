"""
Extrai submissions do form do CSAT Plataforma no HubSpot.
Form ID: 9ec20b97-836d-4afc-acf3-8b80f6c65bd0

Requer Private App token com scope 'forms'.
Config: HUBSPOT_TOKEN em .env.local OU env var HUBSPOT_TOKEN.
"""

import json
import os
import sys
import io
from pathlib import Path

import urllib.request
import urllib.error

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent
OUT = ROOT / "hubspot_data.json"
ENV = ROOT / ".env.local"
FORM_ID = "9ec20b97-836d-4afc-acf3-8b80f6c65bd0"
BASE = "https://api.hubapi.com"


def load_token() -> str:
    if ENV.exists():
        for line in ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith("HUBSPOT_TOKEN="):
                return line.split("=", 1)[1].strip()
    tok = os.environ.get("HUBSPOT_TOKEN", "").strip()
    if not tok:
        print(
            "ERRO: HUBSPOT_TOKEN nao encontrado. Coloque em .env.local como:\n"
            "  HUBSPOT_TOKEN=pat-na1-xxxxx",
            file=sys.stderr,
        )
        sys.exit(1)
    return tok


def http_get(url: str, token: str) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = e.read().decode("utf-8", errors="replace")
        return e.code, body


def main() -> None:
    token = load_token()
    all_rows: list[dict] = []
    after = None
    page = 0
    while True:
        url = f"{BASE}/form-integrations/v1/submissions/forms/{FORM_ID}?limit=50"
        if after:
            url += f"&after={after}"
        status, body = http_get(url, token)
        if status != 200:
            print(f"[hubspot] ERRO status={status}: {body}", file=sys.stderr)
            if status == 403 and isinstance(body, dict):
                scopes = body.get("errors", [{}])[0].get("context", {}).get("requiredGranularScopes")
                if scopes:
                    print(
                        f"\n>>> ACAO NECESSARIA: o Private App do HubSpot precisa do scope {scopes}.\n"
                        "    Vai em Settings -> Integrations -> Private Apps -> (seu app) -> Scopes\n"
                        "    adiciona 'forms' e salva.\n",
                        file=sys.stderr,
                    )
            sys.exit(2)
        results = body.get("results", []) if isinstance(body, dict) else []
        all_rows.extend(results)
        page += 1
        print(f"[hubspot] pagina {page}: +{len(results)} (total {len(all_rows)})")
        paging = body.get("paging") if isinstance(body, dict) else None
        after = paging.get("next", {}).get("after") if paging else None
        if not after:
            break

    # Normaliza cada submission num formato simples
    out = []
    for r in all_rows:
        values = {v.get("name"): v.get("value") for v in r.get("values", [])}
        out.append({
            "submitted_at": r.get("submittedAt"),
            "page_url": r.get("pageUrl"),
            "values": values,
        })

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {OUT.name} ({len(out)} submissions)")


if __name__ == "__main__":
    main()
