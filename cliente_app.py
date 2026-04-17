import re
from datetime import date, timedelta

import requests
import streamlit as st

BASE_URL = "https://api.iugu.com/v1"


def carregar_config():
    c = st.secrets["config"]
    return {
        "subconta_nome": c["subconta_nome"],
        "token": c["token"],
        "plan_identifier": c["plan_identifier"],
        "valor_cents": int(c["valor_cents"]),
        "descricao": c["descricao"],
        "titulo": c.get("titulo", "Assinatura"),
    }


def limpar_digitos(s):
    return re.sub(r"\D", "", s or "")


def separar_ddd(whatsapp_digits):
    d = whatsapp_digits
    if len(d) == 13 and d.startswith("55"):
        d = d[2:]
    if len(d) == 12 and d.startswith("0"):
        d = d[1:]
    if len(d) in (10, 11):
        return d[:2], d[2:]
    return "", d


def buscar_cliente_por_documento(token, doc):
    r = requests.get(
        f"{BASE_URL}/customers",
        auth=(token, ""),
        params={"query": doc, "limit": 20},
        timeout=30,
    )
    if r.status_code >= 400:
        return None
    for c in r.json().get("items") or []:
        if limpar_digitos(c.get("cpf_cnpj")) == doc:
            return c.get("id")
    return None


def criar_cliente(token, dados):
    ddd, numero = separar_ddd(dados["whatsapp"])
    payload = {
        "name": dados["nome_completo"],
        "email": dados["email"],
        "cpf_cnpj": dados["cnpj"],
        "phone_prefix": ddd,
        "phone": numero,
        "notes": dados["endereco"],
        "custom_variables": [
            {"name": "marca", "value": dados["marca"]},
            {"name": "razao_social", "value": dados["razao_social"]},
            {"name": "whatsapp", "value": dados["whatsapp"]},
            {"name": "instagram", "value": dados["instagram"]},
            {"name": "endereco", "value": dados["endereco"]},
        ],
    }
    r = requests.post(f"{BASE_URL}/customers", auth=(token, ""), json=payload, timeout=30)
    return r


def obter_ou_criar_cliente(token, dados):
    existente = buscar_cliente_por_documento(token, dados["cnpj"])
    if existente:
        return existente, True, None
    r = criar_cliente(token, dados)
    if r.status_code >= 400:
        return None, False, r
    return r.json().get("id"), False, r


def criar_subscription(token, customer_id, config):
    payload = {
        "customer_id": customer_id,
        "plan_identifier": config["plan_identifier"],
        "only_on_charge_success": False,
        "payable_with": "pix",
    }
    return requests.post(f"{BASE_URL}/subscriptions", auth=(token, ""), json=payload, timeout=30)


def cancelar_fatura(token, invoice_id):
    return requests.put(f"{BASE_URL}/invoices/{invoice_id}/cancel", auth=(token, ""), timeout=30)


def criar_fatura_automatic_pix(token, customer_id, subscription_id, config, dados, contract_number):
    hoje = date.today()
    due = (hoje + timedelta(days=3)).isoformat()
    payload = {
        "customer_id": customer_id,
        "subscription_id": subscription_id,
        "email": dados["email"],
        "due_date": due,
        "payable_with": ["pix"],
        "items": [
            {
                "description": config["descricao"],
                "quantity": 1,
                "price_cents": config["valor_cents"],
            }
        ],
        "payer": {
            "name": dados["nome_completo"],
            "cpf_cnpj": dados["cnpj"],
            "email": dados["email"],
        },
        "automatic_pix": {
            "journey": 3,
            "frequency": "monthly",
            "recurrence_beginning": due,
            "contract_number": contract_number[:35],
        },
    }
    r = requests.post(f"{BASE_URL}/invoices", auth=(token, ""), json=payload, timeout=30)
    return r


def render_form(config):
    st.markdown(
        f"### {config['titulo']}\n"
        f"Valor: **R$ {config['valor_cents']/100:.2f}** — cobrança mensal via Pix Automático."
    )
    st.caption(
        "Preencha seus dados abaixo. Ao pagar o Pix gerado, sua assinatura é ativada "
        "e as próximas cobranças são debitadas automaticamente todo mês — você não "
        "precisa gerar outro QR."
    )

    with st.form("cadastro_cliente"):
        col1, col2 = st.columns(2)
        with col1:
            nome = st.text_input("Qual é o seu nome?*")
            marca = st.text_input("Qual o nome da sua marca?*")
            cnpj = st.text_input("Qual o CNPJ da sua marca?* (só números)")
            whatsapp = st.text_input("Qual o seu número de WhatsApp?* (só números, com DDD)")
            instagram = st.text_input("Qual o Instagram da sua marca?*")
        with col2:
            sobrenome = st.text_input("Qual é o seu sobrenome?*")
            razao_social = st.text_input("Qual a razão social da sua empresa?*")
            email = st.text_input("Qual o seu e-mail?*")
            endereco = st.text_input("Qual o endereço da sua loja ou fábrica?*")

        submitted = st.form_submit_button(
            "Gerar Pix Automático", type="primary", use_container_width=True
        )

    if not submitted:
        return None
    return {
        "nome": nome, "sobrenome": sobrenome, "marca": marca,
        "razao_social": razao_social, "cnpj": cnpj, "email": email,
        "whatsapp": whatsapp, "endereco": endereco, "instagram": instagram,
    }


def validar(form):
    cnpj_limpo = limpar_digitos(form["cnpj"])
    whatsapp_limpo = limpar_digitos(form["whatsapp"])
    obrigatorios = [
        form["nome"], form["sobrenome"], form["marca"], form["razao_social"],
        cnpj_limpo, form["email"], whatsapp_limpo, form["endereco"], form["instagram"],
    ]
    if not all(obrigatorios):
        return None, "Preencha todos os campos obrigatórios."
    if len(cnpj_limpo) != 14:
        return None, "CNPJ inválido — precisa ter 14 dígitos."
    if len(whatsapp_limpo) not in (10, 11, 12, 13):
        return None, "WhatsApp inválido — informe com DDD (ex.: 11999998888)."
    if "@" not in form["email"]:
        return None, "E-mail inválido."

    dados = {
        "nome_completo": f"{form['nome'].strip()} {form['sobrenome'].strip()}",
        "email": form["email"].strip(),
        "cnpj": cnpj_limpo,
        "marca": form["marca"].strip(),
        "razao_social": form["razao_social"].strip(),
        "whatsapp": whatsapp_limpo,
        "endereco": form["endereco"].strip(),
        "instagram": form["instagram"].strip(),
    }
    return dados, None


def processar(config, dados):
    with st.spinner("Verificando seu cadastro na iugu..."):
        try:
            customer_id, reutilizado, r_cliente = obter_ou_criar_cliente(config["token"], dados)
        except requests.RequestException as e:
            st.error(f"Erro de conexão com a iugu: {e}")
            return

    if not customer_id:
        st.error("Não foi possível cadastrar seu CNPJ na iugu.")
        try:
            st.json(r_cliente.json())
        except Exception:
            st.code(r_cliente.text if r_cliente is not None else "")
        return

    if reutilizado:
        st.info("🔎 Identificamos seu CNPJ já cadastrado — vamos vincular a fatura ao seu cadastro existente.")

    with st.spinner("Criando sua assinatura..."):
        try:
            r_sub = criar_subscription(config["token"], customer_id, config)
        except requests.RequestException as e:
            st.error(f"Erro ao criar assinatura: {e}")
            return

    if r_sub.status_code >= 400:
        st.error("Não foi possível criar a assinatura. Tente novamente ou entre em contato.")
        try:
            st.json(r_sub.json())
        except Exception:
            st.code(r_sub.text)
        return

    subscription = r_sub.json()
    subscription_id = subscription.get("id")

    # Cancela a fatura auto-gerada pela subscription (vamos criar a nossa com automatic_pix)
    for inv_placeholder in (subscription.get("recent_invoices") or []):
        placeholder_id = inv_placeholder.get("id")
        if placeholder_id:
            try:
                cancelar_fatura(config["token"], placeholder_id)
            except requests.RequestException:
                pass

    with st.spinner("Gerando seu Pix Automático..."):
        try:
            r_inv = criar_fatura_automatic_pix(
                config["token"], customer_id, subscription_id, config, dados, f"CTR-{dados['cnpj']}"
            )
        except requests.RequestException as e:
            st.error(f"Erro ao gerar fatura: {e}")
            return

    if r_inv.status_code >= 400:
        st.error("Não foi possível gerar o Pix. Tente novamente ou entre em contato.")
        try:
            st.json(r_inv.json())
        except Exception:
            st.code(r_inv.text)
        return

    data = r_inv.json()
    pix = data.get("pix") or {}
    auto = data.get("automatic_pix") or {}
    qr_img = pix.get("qrcode")
    qr_text = pix.get("qrcode_text")

    st.success("✅ Pix Automático gerado! Pague abaixo para ativar sua assinatura.")
    st.markdown(f"**Valor:** R$ {(data.get('total_cents') or config['valor_cents'])/100:.2f}")

    if qr_img:
        st.image(qr_img, caption="QR Code Pix Automático", width=280)
    if qr_text:
        st.markdown("**Pix Copia e Cola:**")
        st.code(qr_text, language=None)
    if data.get("secure_url"):
        st.link_button(
            "Abrir página de pagamento iugu", data["secure_url"], use_container_width=True
        )

    st.info(
        "🔁 Ao pagar este QR Code, você autoriza a recorrência mensal automática — "
        "nas próximas cobranças o valor é debitado direto, sem precisar gerar novo Pix."
    )

    if auto.get("receiver_recurrence_id"):
        st.caption(f"ID da recorrência: `{auto['receiver_recurrence_id']}`")

    if not (qr_img or qr_text or data.get("secure_url")):
        st.warning("Não conseguimos exibir o Pix aqui. Entre em contato com o suporte.")
        st.json({"pix": pix, "automatic_pix": auto})


def main():
    config = carregar_config()
    st.set_page_config(page_title=config["titulo"], page_icon="💳", layout="centered")

    form = render_form(config)
    if form is None:
        return

    dados, erro = validar(form)
    if erro:
        st.error(erro)
        return

    processar(config, dados)


if __name__ == "__main__":
    main()
