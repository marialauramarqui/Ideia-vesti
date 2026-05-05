"""Detalha cada plano (via GET /plans/{id}) para ver os prices reais."""

import requests

BASE_URL = "https://api.iugu.com/v1"

SUBCONTAS = {
    "Vesti Starter": "B562088A237D7BC217D927E031182B5F0C7BE1B9C4A8D43FFB3CF6269856F718",
    "Vesti Uemtel": "5605D9B9C6CAF57B42D8496F3F2996D3DD8548C9A44BA554FFCBAF7E58A97F72",
}


def detalhar(nome, token):
    print(f"\n=== {nome} ===")
    r = requests.get(f"{BASE_URL}/plans", auth=(token, ""), params={"limit": 100}, timeout=30)
    if r.status_code >= 400:
        print(f"  erro: {r.text}")
        return
    for p in r.json().get("items") or []:
        pid = p.get("id")
        ident = p.get("identifier")
        nome_p = p.get("name")
        det = requests.get(f"{BASE_URL}/plans/{pid}", auth=(token, ""), timeout=30)
        if det.status_code >= 400:
            continue
        dj = det.json()
        prices = dj.get("prices") or []
        preco_str = ", ".join(f"{pr.get('currency')} {pr.get('value_cents', 0)/100:.2f}" for pr in prices) or "sem prices"
        print(f"  identifier={ident!r:<30} nome={nome_p!r:<30} prices=[{preco_str}]")


if __name__ == "__main__":
    for nome, token in SUBCONTAS.items():
        detalhar(nome, token)
