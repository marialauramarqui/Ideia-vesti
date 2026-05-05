import re
from datetime import date, timedelta

import requests
import streamlit as st

BASE_URL = "https://api.iugu.com/v1"

FREQ_LABEL = {
    "weekly": "semanal",
    "monthly": "mensal",
    "quarterly": "trimestral",
    "semiannually": "semestral",
    "yearly": "anual",
}


def carregar_config():
    c = st.secrets["config"]
    return {
        "subconta_nome": c["subconta_nome"],
        "token": c["token"],
        "plan_identifier": c["plan_identifier"],
        "valor_cents": int(c["valor_cents"]),
        "descricao": c["descricao"],
        "titulo": c.get("titulo", "Assinatura"),
        "frequency": c.get("frequency", "monthly"),
        "financeiro_email": c.get("financeiro_email", ""),
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
        "cpf_cnpj": dados["documento"],
        "phone_prefix": ddd,
        "phone": numero,
        "notes": dados["endereco"],
        "custom_variables": [
            {"name": "marca", "value": dados["marca"]},
            {"name": "razao_social", "value": dados["razao_social"]},
            {"name": "whatsapp", "value": dados["whatsapp"]},
            {"name": "endereco", "value": dados["endereco"]},
        ],
    }
    r = requests.post(f"{BASE_URL}/customers", auth=(token, ""), json=payload, timeout=30)
    return r


def obter_ou_criar_cliente(token, dados):
    existente = buscar_cliente_por_documento(token, dados["documento"])
    if existente:
        return existente, True, None
    r = criar_cliente(token, dados)
    if r.status_code >= 400:
        return None, False, r
    return r.json().get("id"), False, r


def criar_subscription(token, customer_id, config):
    # expires_at longe no futuro: sem isso a Iugu cria a subscription com
    # expires_at = hoje, ela "morre" no dia da criacao e nunca cicla
    # (foi o bug que travou a recorrencia dos testes de 23/04 e 05/05).
    expires_at = (date.today() + timedelta(days=365 * 5)).isoformat()
    payload = {
        "customer_id": customer_id,
        "plan_identifier": config["plan_identifier"],
        "only_on_charge_success": False,
        "payable_with": "pix",
        "expires_at": expires_at,
        "subitems": [
            {
                "description": config["descricao"],
                "quantity": 1,
                "price_cents": config["valor_cents"],
                "recurrent": True,
            }
        ],
    }
    return requests.post(f"{BASE_URL}/subscriptions", auth=(token, ""), json=payload, timeout=30)


def cancelar_fatura(token, invoice_id):
    return requests.put(f"{BASE_URL}/invoices/{invoice_id}/cancel", auth=(token, ""), timeout=30)


def consultar_invoice(token, invoice_id):
    return requests.get(f"{BASE_URL}/invoices/{invoice_id}", auth=(token, ""), timeout=30)


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
            "cpf_cnpj": dados["documento"],
            "email": dados["email"],
        },
        "automatic_pix": {
            "journey": 3,
            "frequency": config.get("frequency", "monthly"),
            "recurrence_beginning": due,
            "contract_number": contract_number[:35],
        },
    }
    if config.get("financeiro_email"):
        payload["cc_emails"] = config["financeiro_email"]
    r = requests.post(f"{BASE_URL}/invoices", auth=(token, ""), json=payload, timeout=30)
    return r


def render_form(config, permitir_valor_manual=False):
    freq_txt = FREQ_LABEL.get(config.get("frequency", "monthly"), "recorrente")
    st.markdown(f"### {config['titulo']}")
    if not permitir_valor_manual:
        st.markdown(
            f"Valor: **R$ {config['valor_cents']/100:.2f}** — cobrança {freq_txt} via Pix Automático."
        )
    else:
        st.markdown("🧪 **Modo de teste** — informe abaixo o valor a pagar.")
    st.caption(
        "Preencha seus dados abaixo. Ao pagar o Pix gerado, sua assinatura é ativada "
        f"e as próximas cobranças são debitadas automaticamente ({freq_txt}) — você "
        "não precisa gerar outro QR."
    )

    with st.form("cadastro_cliente"):
        col1, col2 = st.columns(2)
        with col1:
            nome = st.text_input("Qual é o seu nome?*")
            marca = st.text_input("Qual o nome da sua marca?*")
            documento = st.text_input("Qual o CNPJ ou CPF da sua marca?* (só números)")
            whatsapp = st.text_input("Qual o seu número de WhatsApp?* (só números, com DDD)")
        with col2:
            sobrenome = st.text_input("Qual é o seu sobrenome?*")
            razao_social = st.text_input("Qual a razão social da sua empresa?*")
            email = st.text_input("Qual o seu e-mail?*")
            endereco = st.text_input("Qual o endereço da sua loja ou fábrica?*")

        valor_manual = None
        if permitir_valor_manual:
            valor_manual = st.number_input(
                "Valor a pagar (R$)*",
                min_value=0.01, value=1.00, step=0.50, format="%.2f",
            )

        submitted = st.form_submit_button(
            "Gerar Pix Automático", type="primary", use_container_width=True
        )

    if not submitted:
        return None
    return {
        "nome": nome, "sobrenome": sobrenome, "marca": marca,
        "razao_social": razao_social, "documento": documento, "email": email,
        "whatsapp": whatsapp, "endereco": endereco,
        "valor_manual": valor_manual,
    }


def validar(form):
    doc_limpo = limpar_digitos(form["documento"])
    whatsapp_limpo = limpar_digitos(form["whatsapp"])
    obrigatorios = [
        form["nome"], form["sobrenome"], form["marca"], form["razao_social"],
        doc_limpo, form["email"], whatsapp_limpo, form["endereco"],
    ]
    if not all(obrigatorios):
        return None, "Preencha todos os campos obrigatórios."
    if len(doc_limpo) not in (11, 14):
        return None, "CPF/CNPJ inválido — informe 11 dígitos (CPF) ou 14 dígitos (CNPJ)."
    if len(whatsapp_limpo) not in (10, 11, 12, 13):
        return None, "WhatsApp inválido — informe com DDD (ex.: 11999998888)."
    if "@" not in form["email"]:
        return None, "E-mail inválido."

    dados = {
        "nome_completo": f"{form['nome'].strip()} {form['sobrenome'].strip()}",
        "email": form["email"].strip(),
        "documento": doc_limpo,
        "marca": form["marca"].strip(),
        "razao_social": form["razao_social"].strip(),
        "whatsapp": whatsapp_limpo,
        "endereco": form["endereco"].strip(),
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
        st.error("Não foi possível cadastrar seu CPF/CNPJ na iugu.")
        try:
            st.json(r_cliente.json())
        except Exception:
            st.code(r_cliente.text if r_cliente is not None else "")
        return

    if reutilizado:
        st.info("🔎 Identificamos seu CPF/CNPJ já cadastrado — vamos vincular a fatura ao seu cadastro existente.")

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
                config["token"], customer_id, subscription_id, config, dados, f"CTR-{dados['documento']}"
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

    st.session_state["invoice_data"] = r_inv.json()
    st.session_state["invoice_id"] = r_inv.json().get("id")
    mostrar_pagamento(config)


def mostrar_pagamento(config):
    data = st.session_state.get("invoice_data") or {}
    invoice_id = st.session_state.get("invoice_id")
    pix = data.get("pix") or {}
    auto = data.get("automatic_pix") or {}
    qr_img = pix.get("qrcode")
    qr_text = pix.get("qrcode_text")
    status = (data.get("status") or "").lower()
    freq_txt = FREQ_LABEL.get(config.get("frequency", "monthly"), "recorrente")

    status_label = {
        "paid": "🟢 Pago",
        "pending": "⚪ Aguardando pagamento",
        "canceled": "⚫ Cancelada",
        "expired": "⚫ Expirada",
    }.get(status, f"❔ {status or 'desconhecido'}")

    if status == "paid":
        st.success("✅ Pagamento confirmado! Sua assinatura está ativa.")
    else:
        st.success("✅ Pix Automático gerado! Pague abaixo para ativar sua assinatura.")

    st.markdown(f"**Valor:** R$ {(data.get('total_cents') or config['valor_cents'])/100:.2f}")
    st.markdown(f"**Status:** {status_label}")

    if status != "paid":
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
            f"🔁 Ao pagar este QR Code, você autoriza a recorrência {freq_txt} automática — "
            "nas próximas cobranças o valor é debitado direto, sem precisar gerar novo Pix."
        )

    if auto.get("receiver_recurrence_id"):
        st.caption(f"ID da recorrência: `{auto['receiver_recurrence_id']}`")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Atualizar", type="primary", use_container_width=True):
            try:
                r = consultar_invoice(config["token"], invoice_id)
                if r.status_code < 400:
                    st.session_state["invoice_data"] = r.json()
                    st.rerun()
                else:
                    st.error("Não foi possível consultar o status. Tente novamente.")
            except requests.RequestException as e:
                st.error(f"Erro ao atualizar: {e}")
    with col2:
        if st.button("Gerar novo Pix", use_container_width=True):
            st.session_state.pop("invoice_id", None)
            st.session_state.pop("invoice_data", None)
            st.rerun()

    if status != "paid" and not (qr_img or qr_text or data.get("secure_url")):
        st.warning("Não conseguimos exibir o Pix aqui. Entre em contato com o suporte.")
        st.json({"pix": pix, "automatic_pix": auto})


def main(permitir_valor_manual=False):
    config = carregar_config()
    st.set_page_config(page_title=config["titulo"], page_icon="💳", layout="centered")

    if "invoice_id" in st.session_state:
        mostrar_pagamento(config)
        return

    form = render_form(config, permitir_valor_manual=permitir_valor_manual)
    if form is None:
        return

    dados, erro = validar(form)
    if erro:
        st.error(erro)
        return

    if permitir_valor_manual and form.get("valor_manual"):
        config["valor_cents"] = int(round(float(form["valor_manual"]) * 100))

    processar(config, dados)


if __name__ == "__main__":
    main()
