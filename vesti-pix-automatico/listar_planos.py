"""Helper rapido para listar os plan_identifier de cada subconta na iugu.

Uso:  python listar_planos.py
Imprime para cada subconta o identifier, nome e valor dos planos cadastrados.
Copie o identifier do plano correto para o campo plan_identifier em secrets.
"""

import requests

BASE_URL = "https://api.iugu.com/v1"

SUBCONTAS = {
    "Vesti Starter": "B562088A237D7BC217D927E031182B5F0C7BE1B9C4A8D43FFB3CF6269856F718",
    "Vesti Uemtel": "5605D9B9C6CAF57B42D8496F3F2996D3DD8548C9A44BA554FFCBAF7E58A97F72",
}


def listar(nome, token):
    print(f"\n=== {nome} ===")
    r = requests.get(f"{BASE_URL}/plans", auth=(token, ""), params={"limit": 100}, timeout=30)
    if r.status_code >= 400:
        print(f"  erro {r.status_code}: {r.text}")
        return
    items = r.json().get("items") or []
    if not items:
        print("  (nenhum plano encontrado)")
        return
    for p in items:
        ident = p.get("identifier")
        nome_p = p.get("name")
        valor = (p.get("value_cents") or 0) / 100
        freq = f"{p.get('interval')} {p.get('interval_type')}"
        print(f"  identifier={ident!r:<30} nome={nome_p!r:<30} R$ {valor:.2f}  ({freq})")


if __name__ == "__main__":
    for nome, token in SUBCONTAS.items():
        listar(nome, token)
