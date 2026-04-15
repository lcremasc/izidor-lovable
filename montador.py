"""
montador.py
===========
Etapa 5 do pipeline de análise de crédito.
Recebe dict_p1, dict_p2, dict_p3, dict_p4 e monta o payload final
no formato exato esperado pelo endpoint Lovable.

Uso:
    from montador import montar_payload
    payload = montar_payload(p1, p2, p3, p4)
"""

from __future__ import annotations

from datetime import date
from typing import Any


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _hoje_iso() -> str:
    return date.today().isoformat()


def _wrap_lista(obj: Any) -> list:
    """Garante que um objeto seja lista — encapsula dict em lista se necessário."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def _balanco_raw_items(balanco: list[dict]) -> list[dict]:
    """
    Converte o balanço do formato dict_p2 (lista de períodos com itens)
    para o formato raw do Lovable: lista plana de {tipo, valor, data_base}.

    O mapeamento de campo → tipo usa os nomes canônicos do payload.
    """
    CAMPO_PARA_TIPO: dict[str, str] = {
        "ativo_total":            "Ativo",
        "ativo_circulante":       "Circulante",
        "disponivel":             "Disponível",
        "creditos":               "Créditos",
        "estoques":               "Estoque",
        "despesas_antecipadas":   "Despesas Antecipadas",
        "ativo_permanente":       "Ativo Permanente",
        "investimentos":          "Investimentos",
        "imobilizado":            "Imobilizado",
        "intangivel":             "Intangível",
        "passivo_total":          "Passivo",
        "passivo_circulante":     "Passivo Circulante",
        "fornecedores":           "Obrigações com Terceiros",
        "salarios_encargos":      "Salários e Encargos",
        "impostos_a_pagar":       "Impostos a Pagar",
        "outras_obrigacoes_cp":   "Outras Obrigações CP",
        "passivo_nao_circulante": "Passivo Não Circulante",
        "financiamentos_lp":      "Financiamentos",
        "outras_obrigacoes_lp":   "Outras Obrigações LP",
        "patrimonio_liquido":     "Patrimônio Líquido",
        "capital_social":         "Capital Social",
        "resultados_acumulados":  "Resultados Acumulados",
    }

    raw: list[dict] = []
    for periodo in balanco:
        data_base = periodo.get("data_base", "")
        itens     = periodo.get("itens", {})
        for campo, tipo in CAMPO_PARA_TIPO.items():
            val = itens.get(campo)
            if val is not None:
                raw.append({"tipo": tipo, "valor": val, "data_base": data_base})
    return raw


def _dre_raw_items(dre: list[dict]) -> list[dict]:
    """
    Converte a DRE do formato dict_p2 para o formato raw do Lovable:
    lista plana de {tipo, valor, data_inicio, data_fim}.
    """
    CAMPO_PARA_TIPO: dict[str, str] = {
        "receita_bruta":               "Receita Operacional Bruta",
        "deducoes":                    "Deduções e Abatimentos",
        "receita_liquida":             "Receita Operacional Líquida",
        "cmv":                         "Custo Operacional Bruto (CMV)",
        "lucro_bruto":                 "Lucro Operacional Bruto",
        "despesas_operacionais":       "Despesas Operacionais",
        "outras_receitas_operacionais":"Outras Receitas Operacionais",
        "ebitda":                      "EBITDA",
        "depreciacao":                 "Depreciação e Amortização",
        "ebit":                        "Resultado antes das receitas e despesas financeiras",
        "resultado_financeiro":        "Resultado Financeiro",
        "outras_receitas":             "Outras Receitas",
        "lair":                        "LAIR",
        "ir_csll":                     "IR e CSLL",
        "lucro_liquido":               "Lucro Líquido do Exercício",
    }

    raw: list[dict] = []
    for periodo in dre:
        ini   = periodo.get("data_inicio", "")
        fim   = periodo.get("data_fim", "")
        itens = periodo.get("itens", {})
        for campo, tipo in CAMPO_PARA_TIPO.items():
            val = itens.get(campo)
            if val is not None:
                raw.append({
                    "tipo": tipo,
                    "valor": val,
                    "data_inicio": ini,
                    "data_fim":    fim,
                })
    return raw



def _formatar_ranking(ranking_p1: dict | None) -> dict | None:
    """
    Converte o ranking bruto do dict_p1 para o formato formatado esperado pelo Lovable
    em slides_content.company_profile.ranking.

    Inclui competidores vizinhos do Ranking ABRAS 2025 (posições 181-186).
    Fonte: static.abras.com.br/pdf/tabelao-ranking-abras-2025.pdf
    """
    if not ranking_p1:
        return None

    entidade = ranking_p1.get("entidade") or "ABRAS"
    ano      = ranking_p1.get("ano") or ""
    posicao  = ranking_p1.get("posicao")
    fat      = ranking_p1.get("faturamento_ranking") or ranking_p1.get("faturamento")
    variacao = ranking_p1.get("variacao_posicao")

    name_str = f"Ranking {entidade} {ano}".strip() if (entidade or ano) else "Ranking Setorial"
    pos_str  = f"{posicao}\u00ba" if posicao is not None else "\u2014"

    if fat:
        fat_mm = fat / 1_000_000
        rev_str = f"R$ {fat_mm:,.1f} MM".replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        rev_str = "\u2014"

    if variacao is not None and variacao != 0:
        sinal  = "\u25b2" if variacao > 0 else "\u25bc"
        change = f"{sinal} {abs(variacao)} posi\u00e7\u00f5es"
    else:
        change = "Manteve posi\u00e7\u00e3o"

    # Vizinhos no Ranking ABRAS 2025 (extraidos do PDF oficial abr/2025)
    _ABRAS_2025 = [
        {"pos": "181", "name": "FM Ferreira de Sousa",    "detail": "R$ 330,5 MM", "w": "PI"},
        {"pos": "182", "name": "Brasao Supermercado",     "detail": "R$ 330,3 MM", "w": "SC"},
        {"pos": "183", "name": "Superbom Alimentos",      "detail": "R$ 328,8 MM", "w": "DF"},
        {"pos": "185", "name": "Jorge Batista & CIA",     "detail": "R$ 327,6 MM", "w": "PI"},
        {"pos": "186", "name": "Supermercado Pro Brasil", "detail": "R$ 318,8 MM", "w": "GO"},
    ]

    competidores = []
    if (entidade or "").upper() == "ABRAS" and posicao is not None:
        for c in _ABRAS_2025:
            try:
                if abs(int(c["pos"]) - posicao) <= 3:
                    competidores.append(c)
            except (ValueError, TypeError):
                pass

    return {
        "name":        name_str,
        "position":    pos_str,
        "revenue":     rev_str,
        "detail":      "Faturamento bruto anual declarado",
        "change":      change,
        "competitors": competidores,
    }


def _montar_bureaus(p2_bureaux: dict) -> dict:
    """
    Converte bureaux do dict_p2 para o formato de input do Lovable.
    Serasa, Quod e Nuclea: lista (raw). SCR Bacen: dict (raw).
    """
    def _para_lista(obj: Any) -> list:
        if obj is None:
            return []
        if isinstance(obj, list):
            return obj
        return [obj]

    serasa = _para_lista(p2_bureaux.get("serasa"))
    quod   = _para_lista(p2_bureaux.get("quod"))
    nuclea = _para_lista(p2_bureaux.get("nuclea"))
    scr    = p2_bureaux.get("scr_bacen")  # dict direto

    return {
        "serasa":                  serasa,
        "quod":                    quod,
        "nuclea":                  nuclea,
        "scr_bacen_relationships": scr if scr else {},
    }


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def _montar_balance_compositions(balanco: list[dict], balance_p4: dict | None) -> dict:
    """
    Calcula ativo_composition, passivo_composition e reading para o slide balance
    a partir do balanço mais recente em p2.balanco.

    Valores em R$ MM com 2 casas decimais.
    reading vem de p4.balance_analysis.conclusion se disponível.
    """
    def _mm(v: float | None) -> float:
        return round((v or 0) / 1_000_000, 2)

    def _pct(v: float | None, total: float) -> str:
        if not total or not v:
            return "0%"
        return f"{round(v / total * 100, 1)}%"

    if not balanco:
        return {}

    # Usar o balanço mais recente (último da lista)
    ultimo = balanco[-1]
    itens  = ultimo.get("itens", {})

    at  = itens.get("ativo_total") or 0
    ac  = itens.get("ativo_circulante") or 0
    ap  = itens.get("ativo_permanente") or 0

    disp    = itens.get("disponivel") or 0
    cred    = itens.get("creditos") or 0
    estq    = itens.get("estoques") or 0
    desp_a  = itens.get("despesas_antecipadas") or 0
    outros_ac = max(0, ac - disp - cred - estq - desp_a)

    invest  = itens.get("investimentos") or 0
    imob    = itens.get("imobilizado") or 0
    outros_ap = max(0, ap - invest - imob)

    pt  = at   # total do passivo+PL = AT
    pc  = itens.get("passivo_circulante") or 0
    pnc = itens.get("passivo_nao_circulante") or 0
    pl  = itens.get("patrimonio_liquido") or 0

    fornec  = itens.get("fornecedores") or 0
    fin_lp  = itens.get("financiamentos_lp") or 0
    cap_soc = itens.get("capital_social") or 0
    res_ac  = itens.get("resultados_acumulados") or 0
    outros_pc  = max(0, pc - fornec)
    outros_pnc = max(0, pnc - fin_lp)
    outros_pl  = max(0, pl - cap_soc - res_ac)

    ativo_composition = [
        {"name": "Disponível",          "value": _mm(disp),    "pct": _pct(disp,    at)},
        {"name": "Contas a Receber",    "value": _mm(cred),    "pct": _pct(cred,    at)},
        {"name": "Estoques",            "value": _mm(estq),    "pct": _pct(estq,    at)},
        {"name": "Outras CP",           "value": _mm(outros_ac + desp_a), "pct": _pct(outros_ac + desp_a, at)},
        {"name": "Imobilizado",         "value": _mm(imob),    "pct": _pct(imob,    at)},
        {"name": "Outros Permanente",   "value": _mm(invest + outros_ap), "pct": _pct(invest + outros_ap, at)},
    ]
    # Remover itens zerados
    ativo_composition = [x for x in ativo_composition if x["value"] > 0]

    passivo_composition = [
        {"name": "Fornecedores",        "value": _mm(fornec),    "pct": _pct(fornec,   pt)},
        {"name": "Outros PC",           "value": _mm(outros_pc), "pct": _pct(outros_pc, pt)},
        {"name": "Financiamentos LP",   "value": _mm(fin_lp),    "pct": _pct(fin_lp,   pt)},
        {"name": "Outros PNC",          "value": _mm(outros_pnc),"pct": _pct(outros_pnc, pt)},
        {"name": "Capital Social",      "value": _mm(cap_soc),   "pct": _pct(cap_soc,  pt)},
        {"name": "Resultados Acumulados","value": _mm(res_ac + outros_pl), "pct": _pct(res_ac + outros_pl, pt)},
    ]
    passivo_composition = [x for x in passivo_composition if x["value"] > 0]

    # reading: preferir p4.balance_analysis.conclusion; fallback genérico
    reading = None
    if isinstance(balance_p4, dict):
        ba = balance_p4.get("balance_analysis") or {}
        if isinstance(ba, dict):
            reading = ba.get("conclusion")
    if not reading:
        data_base = ultimo.get("data_base", "")
        at_mm = _mm(at)
        pl_mm = _mm(pl)
        reading = (
            f"Balanço de {data_base}: Ativo Total de R$ {at_mm:.1f}M, "
            f"com Patrimônio Líquido de R$ {pl_mm:.1f}M. "
            f"Estrutura compatível com o perfil operacional da empresa."
        )

    return {
        "ativo_composition":   ativo_composition,
        "passivo_composition": passivo_composition,
        "reading":             reading,
    }


def _calcular_red_flags(p3: dict, p2: dict) -> list[dict]:
    """
    Deriva automaticamente a lista de red_flags / pontos de atenção a partir
    dos indicadores calculados (p3) e dos dados de bureaux (p2).

    Cada item: {"tipo": "red_flag"|"atencao", "campo": str, "valor": str, "descricao": str}

    Thresholds (alinhados ao PROMPT_04):
      red_flag  : Liquidez Corrente < 0.80 | Dívida/EBITDA > 4.0x | EBITDA margin < 4%
                  | Overdue SCR > 0 | Serasa < 600 | PEFIN/RL > 1% | Falência/RJ
      atencao   : Liquidez Corrente 0.80–1.20 | Dívida/EBITDA 2.5–4.0x
                  | EBITDA margin 4–8% | Dívida/PL > 3.0x | Resultado Fin. negativo
                  | CERC ausente | CPR em varejo | Quod Tempo Atraso Médio
    """
    flags: list[dict] = []

    def _add(tipo: str, campo: str, valor: str, descricao: str) -> None:
        flags.append({"tipo": tipo, "campo": campo, "valor": valor, "descricao": descricao})

    liq  = p3.get("liquidez", {})
    end  = p3.get("endividamento", {})
    rest = p3.get("restritivos_bureau", {})
    rest_rel = p3.get("restritivos_relativos", {})
    scr  = p3.get("scr_bacen", {})
    fontes = p2.get("fontes_input", {})

    # ── Liquidez Corrente ────────────────────────────────────────────────
    lc = liq.get("liquidez_corrente")
    if lc is not None:
        if lc < 0.80:
            _add("red_flag", "Liquidez Corrente", f"{lc:.2f}x",
                 "Abaixo de 0,80x — risco de insolvência de curto prazo.")
        elif lc < 1.20:
            _add("atencao", "Liquidez Corrente", f"{lc:.2f}x",
                 "Entre 0,80x e 1,20x — cobertura de obrigações CP limitada.")

    # ── Dívida / EBITDA ──────────────────────────────────────────────────
    de = end.get("divida_ebitda")
    if de is not None:
        if de > 4.0:
            _add("red_flag", "Dívida / EBITDA", f"{de:.2f}x",
                 "Acima de 4,0x — alavancagem financeira elevada.")
        elif de > 2.5:
            _add("atencao", "Dívida / EBITDA", f"{de:.2f}x",
                 "Entre 2,5x e 4,0x — alavancagem moderada, monitorar.")

    # ── Margem EBITDA (mais recente com EBITDA disponível) ───────────────
    margens = p3.get("margens", [])
    margem_ebitda = None
    for m in reversed(margens):
        if m.get("margem_ebitda") is not None:
            margem_ebitda = m["margem_ebitda"]
            break
    if margem_ebitda is not None:
        if margem_ebitda < 0.04:
            _add("red_flag", "Margem EBITDA", f"{margem_ebitda*100:.1f}%",
                 "Abaixo de 4% — geração de caixa operacional crítica.")
        elif margem_ebitda < 0.08:
            _add("atencao", "Margem EBITDA", f"{margem_ebitda*100:.1f}%",
                 "Entre 4% e 8% — margem operacional sob pressão.")

    # ── Overdue SCR ──────────────────────────────────────────────────────
    overdue = scr.get("overdue_carteira") or end.get("overdue")
    if overdue and overdue > 0:
        _add("red_flag", "Overdue SCR", f"{overdue*100:.1f}%",
             "Inadimplência bancária identificada — risco de crédito elevado.")

    # ── Serasa Score ─────────────────────────────────────────────────────
    serasa_score = rest.get("serasa_score")
    if serasa_score is not None:
        if serasa_score < 600:
            _add("red_flag", "Score Serasa", str(serasa_score),
                 "Score abaixo de 600 — risco de crédito alto.")

    # ── PEFIN/RL ─────────────────────────────────────────────────────────
    pefin_rl = rest_rel.get("pefin_receita")
    pefin_val = rest.get("pefin_valor")
    if pefin_rl is not None and pefin_rl > 0.01:
        _add("red_flag", "PEFIN / Receita", f"{pefin_rl*100:.2f}%",
             f"PEFIN de R$ {pefin_val:,.0f} representa mais de 1% da receita líquida." if pefin_val else "PEFIN material em relação à receita.")
    elif pefin_val and pefin_val > 0 and (pefin_rl or 0) <= 0.01:
        _add("atencao", "PEFIN", f"R$ {pefin_val:,.0f}",
             "PEFIN imaterial em relação à receita — monitorar regularização.")

    # ── Falência / Recuperação Judicial ──────────────────────────────────
    if rest.get("falencia_valor"):
        _add("red_flag", "Falência/RJ", "Consta",
             "Registro de pedido de falência ou recuperação judicial identificado.")

    # ── Dívida / PL ──────────────────────────────────────────────────────
    dpl = end.get("divida_pl")
    if dpl is not None and dpl > 3.0:
        _add("atencao", "Dívida / PL", f"{dpl:.2f}x",
             "Alavancagem financeira acima de 3,0x — PL pequeno em relação à dívida bancária.")

    # ── Resultado Financeiro (DRE mais recente) ──────────────────────────
    dre_list = p2.get("dre", [])
    if dre_list:
        ultimo_dre = dre_list[-1].get("itens", {})
        res_fin = ultimo_dre.get("resultado_financeiro")
        if res_fin is not None and res_fin < 0:
            _add("atencao", "Resultado Financeiro",
                 f"R$ {res_fin/1e6:.1f}M",
                 "Resultado financeiro negativo no período mais recente — custo da dívida pressionando o lucro.")

    # ── CERC não disponível ──────────────────────────────────────────────
    if not fontes.get("cerc"):
        _add("atencao", "CERC",
             "Não disponível",
             "Agenda de recebíveis CERC não fornecida — impossível estruturar cessão fiduciária completa.")

    # ── CPR em carteira (incomum para varejo) ────────────────────────────
    modalidades = scr.get("modalidades_divida", [])
    for mod in modalidades:
        if "CPR" in (mod.get("modalidade") or "") and (mod.get("valor") or 0) > 0:
            _add("atencao", "CPR na carteira SCR",
                 f"R$ {mod['valor']/1e6:.1f}M",
                 "Cédula de Produto Rural é instrumento incomum para varejo alimentar — monitorar vencimento e renovação.")
            break

    # ── Quod Tempo de Atraso ─────────────────────────────────────────────
    quod = p2.get("bureaux", {}).get("quod") or {}
    if isinstance(quod, list):
        quod = quod[0] if quod else {}
    if quod.get("tempo_atraso") in ("Risco Médio", "Risco Alto"):
        _add("atencao", "Quod — Tempo de Atraso",
             quod.get("tempo_atraso", ""),
             "Indicador de tempo de atraso em pagamentos classificado como Risco Médio pelo Quod.")

    return flags


def montar_payload(
    p1: dict,
    p2: dict,
    p3: dict,
    p4: dict,
) -> dict:
    """
    Monta o payload final no formato exato do Lovable a partir dos 4 dicts.

    Responsabilidades por chave:
      company_name / cnpj / sector / logo_url  → p1
      analysis_date / status                   → montador
      input_data                               → p2 (raw) + montador
      output_data.indicadores                  → p3
      slides_content                           → p4 + p1
    """
    hoje = _hoje_iso()

    # ── Campos raiz ───────────────────────────────────────────────────────
    company_name = p1.get("company_name", "")
    cnpj         = p1.get("cnpj", "")           # com máscara XX.XXX.XXX/XXXX-XX
    sector       = p1.get("sector", "")
    logo_url     = p1.get("logo_url")

    # ── input_data ────────────────────────────────────────────────────────
    bureaux = p2.get("bureaux", {})
    balanco = p2.get("balanco", [])
    dre     = p2.get("dre", [])
    fat_men = p2.get("faturamento_mensal", [])

    financial_inputs = {
        "balance_sheet": {
            "currency":      "BRL",
            "source_format": "linha_a_linha_por_tipo_e_data_base",
            "raw_items":     _balanco_raw_items(balanco),
        },
        "income_statement": {
            "currency":      "BRL",
            "source_format": "linha_a_linha_por_periodo",
            "raw_items":     _dre_raw_items(dre),
        },
        "monthly_revenue": {
            "currency":      "BRL",
            "metric_name":   "faturamento",
            "source_format": "linha_a_linha_por_ano_mes",
            "raw_items":     fat_men,
        },
    }

    # CERC
    comm_inputs = p2.get("commercial_inputs")  # já no formato correto do Lovable

    input_credito_empresa: dict = {
        "fontes_input":        p2.get("fontes_input", {}),
        "bureaus":             _montar_bureaus(bureaux),
        "financial_inputs":    financial_inputs,
        "cnpjs_consultados":   p2.get("cnpjs_consultados", []),
        "cnpjs_raiz_identificados": p2.get("cnpjs_raiz_identificados", []),
        "grupo_empresarial_id":     p2.get("grupo_empresarial_id"),
        "nome_grupo_informado":     p2.get("nome_grupo_informado"),
        "dados_cadastrais_raiz":    p2.get("dados_cadastrais_raiz", []),
    }

    if comm_inputs:
        input_credito_empresa["commercial_inputs"] = comm_inputs

    # ── output_data ───────────────────────────────────────────────────────
    # p3 já está no formato correto — passa direto
    output_data = {"indicadores": p3}

    # ── slides_content ────────────────────────────────────────────────────
    # p4 usa os mesmos nomes de chave que o Lovable espera — passa direto.
    # Slides opcionais (null se dados não disponíveis): receivables_cerc, agenda_aberta
    _SLIDES = [
        "cover", "company_profile", "business_model", "shareholders",
        "dre", "dre_analysis", "balance", "balance_analysis", "balance_detail",
        "liquidity", "revenue_analysis", "debt", "bureaus",
        "receivables_cerc", "agenda_aberta", "products", "proposal",
        "conclusion", "recommendation", "anexos",
    ]

    slides_content: dict = {slide: p4.get(slide) for slide in _SLIDES}

    # ── Pós-processamento: injetar campos formatados que montador controla ─
    cp = slides_content.get("company_profile")
    if isinstance(cp, dict):
        # ranking: p1 tem os dados brutos; montador formata e injeta
        ranking_bruto = p1.get("ranking_setorial") or cp.get("ranking")
        if ranking_bruto:
            cp["ranking"] = _formatar_ranking(ranking_bruto)

        # company_image e store_images: sempre vêm do p1
        cp["company_image"] = p1.get("company_image")
        cp["store_images"]  = p1.get("store_images") or []

        slides_content["company_profile"] = cp

    # balance: injetar ativo_composition, passivo_composition e reading
    bal_slide = slides_content.get("balance")
    if isinstance(bal_slide, dict):
        comps = _montar_balance_compositions(balanco, p4)
        bal_slide.update(comps)
        slides_content["balance"] = bal_slide

    # products: injetar red_flags calculados automaticamente a partir de p3/p2
    prod_slide = slides_content.get("products")
    if isinstance(prod_slide, dict):
        prod_slide["red_flags"] = _calcular_red_flags(p3, p2)
        slides_content["products"] = prod_slide

    # ── Payload final ─────────────────────────────────────────────────────
    payload: dict = {
        "company_name":  company_name,
        "cnpj":          cnpj,
        "sector":        sector,
        "analysis_date": hoje,
        "status":        "complete",
        "logo_url":      logo_url,
        "input_data": {
            "input_credito_empresa": input_credito_empresa,
        },
        "output_data":    output_data,
        "slides_content": slides_content,
    }

    return payload


# ---------------------------------------------------------------------------
# Validação estrutural mínima
# ---------------------------------------------------------------------------

_CAMPOS_OBRIGATORIOS_RAIZ = [
    "company_name", "cnpj", "sector", "analysis_date",
    "status", "input_data", "output_data", "slides_content",
]

_SLIDES_OBRIGATORIOS = [
    "cover", "company_profile", "business_model", "shareholders",
    "dre", "dre_analysis", "balance", "balance_analysis", "balance_detail",
    "liquidity", "revenue_analysis", "debt", "bureaus",
    "products", "proposal", "conclusion", "recommendation", "anexos",
]


def validar_payload(payload: dict) -> list[str]:
    """
    Valida estrutura mínima do payload antes do POST.
    Retorna lista de erros (vazia = ok).
    """
    erros: list[str] = []

    # Campos raiz
    for campo in _CAMPOS_OBRIGATORIOS_RAIZ:
        if campo not in payload or payload[campo] is None:
            erros.append(f"Campo raiz ausente: {campo}")

    # CNPJ com máscara
    cnpj = payload.get("cnpj", "")
    if cnpj and "/" not in cnpj:
        erros.append(f"CNPJ sem máscara: '{cnpj}' — esperado formato XX.XXX.XXX/XXXX-XX")

    # Slides obrigatórios
    slides = payload.get("slides_content", {})
    for slide in _SLIDES_OBRIGATORIOS:
        if slides.get(slide) is None:
            erros.append(f"Slide obrigatório ausente ou null: slides_content.{slide}")

    # output_data tem indicadores
    indicadores = payload.get("output_data", {}).get("indicadores", {})
    for bloco in ["liquidez", "margens", "receita", "endividamento", "scr_bacen"]:
        if not indicadores.get(bloco):
            erros.append(f"Bloco de indicadores ausente: output_data.indicadores.{bloco}")

    # input_data tem raw_items
    fi = (payload.get("input_data", {})
                 .get("input_credito_empresa", {})
                 .get("financial_inputs", {}))
    for fonte in ["balance_sheet", "income_statement", "monthly_revenue"]:
        if not fi.get(fonte, {}).get("raw_items"):
            erros.append(f"raw_items vazio: financial_inputs.{fonte}")

    return erros


# ---------------------------------------------------------------------------
# Validador de qualidade do p2 (pré-E3)
# ---------------------------------------------------------------------------

def validar_p2(p2: dict) -> list[dict]:
    """
    Inspeciona o dict_p2 e retorna alertas sobre campos críticos ausentes
    ou incompletos que impactam os cálculos da calculadora (E3).

    Não bloqueia o pipeline — apenas informa o analista para que decida
    se resubmete documentos ou segue com os dados disponíveis.

    Retorna lista de dicts:
      {
        "nivel":    "critico" | "atencao",
        "campo":    "<caminho do campo>",
        "impacto":  "<indicador(es) que não serão calculados>",
        "sugestao": "<o que solicitar ou verificar>"
      }

    "critico"  → indicador importante ficará None no p3 (ex: Dívida/EBITDA)
    "atencao"  → indicador ficará menos preciso ou com proxy inferior
    """
    alertas: list[dict] = []

    def _add(nivel: str, campo: str, impacto: str, sugestao: str) -> None:
        alertas.append({
            "nivel":    nivel,
            "campo":    campo,
            "impacto":  impacto,
            "sugestao": sugestao,
        })

    dre     = p2.get("dre", [])
    balanco = p2.get("balanco", [])
    fat_men = p2.get("faturamento_mensal", [])
    bureaux = p2.get("bureaux", {}) or {}
    fontes  = p2.get("fontes_input", {}) or {}

    # ── DRE ──────────────────────────────────────────────────────────────

    if not dre:
        _add("critico", "dre",
             "Nenhum indicador de margem, EBITDA, receita ou crescimento será calculado",
             "Solicitar DRE dos últimos 2 exercícios + período parcial mais recente")
    else:
        # EBITDA — crítico para Dívida/EBITDA e margem EBITDA
        tem_ebitda = any(
            d.get("itens", {}).get("ebitda") is not None for d in dre
        )
        if not tem_ebitda:
            _add("critico", "dre[].itens.ebitda",
                 "Dívida/EBITDA e Margem EBITDA não serão calculados — indicadores centrais do rating",
                 "Solicitar DRE consolidada do contador (Painel de Gestão / DRE Gerencial) "
                 "que destaque EBITDA. DREs de ERP (CISSPoder, TOTVS) geralmente não incluem EBITDA.")

        # DRE anual — necessária para margens e CAGR
        tem_anual = any(
            _periodo_dias(d) >= 300 for d in dre
        )
        if not tem_anual:
            _add("critico", "dre — período anual",
                 "CAGR de receita e margens anuais não serão calculados com precisão",
                 "Solicitar DRE anual (jan–dez) de pelo menos 1 exercício fechado")

        # Resultado financeiro — relevante para análise de custo da dívida
        tem_res_fin = any(
            d.get("itens", {}).get("resultado_financeiro") is not None for d in dre
        )
        if not tem_res_fin:
            _add("atencao", "dre[].itens.resultado_financeiro",
                 "Custo da dívida e impacto financeiro não serão visíveis na análise",
                 "Presente em DREs gerenciais do contador — geralmente ausente em exports de ERP")

    # ── Balanço ───────────────────────────────────────────────────────────

    if not balanco:
        _add("critico", "balanco",
             "Liquidez, endividamento relativo (Dívida/PL), ciclo de capital de giro e "
             "composições de balanço não serão calculados",
             "Solicitar Balanço Patrimonial dos últimos 2 exercícios fechados")
    else:
        ultimo = balanco[-1].get("itens", {})

        if ultimo.get("fornecedores") is None:
            _add("atencao", "balanco[].itens.fornecedores",
                 "PMP (Prazo Médio de Pagamento) e NCG usarão proxy menos preciso",
                 "Verificar se balanço detalha Fornecedores separado de outras obrigações do PC")

        if ultimo.get("disponivel") is None:
            _add("atencao", "balanco[].itens.disponivel",
                 "Liquidez Imediata e Dívida Líquida não serão calculadas",
                 "Verificar linha Caixa / Bancos / Disponível no balanço")

        # Verificar se tem balanço recente (menos de 18 meses)
        from datetime import date
        try:
            data_ult = date.fromisoformat(balanco[-1].get("data_base", ""))
            meses_defasagem = (date.today() - data_ult).days / 30
            if meses_defasagem > 18:
                _add("atencao", "balanco — defasagem",
                     f"Balanço mais recente tem {int(meses_defasagem)} meses — "
                     "indicadores de liquidez e PL podem não refletir situação atual",
                     "Solicitar balancete ou balanço mais recente (últimos 12 meses)")
        except (ValueError, TypeError):
            pass

    # ── Faturamento mensal ────────────────────────────────────────────────

    if not fat_men:
        _add("atencao", "faturamento_mensal",
             "Receita média mensal recente e sazonalidade serão estimados pela DRE anual",
             "Solicitar relação de faturamento mensal dos últimos 24–36 meses")
    else:
        anos = {item.get("year") for item in fat_men if item.get("year")}
        if len(anos) < 2:
            _add("atencao", "faturamento_mensal — cobertura",
                 "CAGR e comparação anual de faturamento limitados com menos de 2 anos",
                 "Solicitar faturamento mensal de pelo menos 2 exercícios completos")

    # ── Bureaux ───────────────────────────────────────────────────────────

    if not bureaux.get("scr_bacen"):
        _add("critico", "bureaux.scr_bacen",
             "Dívida bruta, modalidades, prazo CP/LP e histórico bancário não serão calculados",
             "Solicitar relatório SCR Bacen — data-base mais recente")

    if not bureaux.get("serasa"):
        _add("atencao", "bureaux.serasa",
             "Score Serasa, PEFIN, REFIN e restritivos não estarão disponíveis",
             "Solicitar Serasa Recomenda Avançado ou Serasa Experian PJ")

    if not bureaux.get("nuclea"):
        _add("atencao", "bureaux.nuclea",
             "Faturamento transacional, score Nuclea e liquidez de pagamentos não disponíveis — "
             "cobertura de recebíveis para garantia não pode ser estimada",
             "Solicitar Nuclea / CIP — relatório de comportamento transacional")

    # ── CERC ─────────────────────────────────────────────────────────────

    if not fontes.get("cerc"):
        _add("atencao", "fontes_input.cerc",
             "Agenda de recebíveis não disponível — cessão fiduciária completa não estruturável",
             "Solicitar agenda CERC ou Nuclea com detalhamento de recebíveis de cartão")

    return alertas


def _periodo_dias(dre_item: dict) -> int:
    """Retorna a duração em dias de um período DRE."""
    from datetime import date
    try:
        ini = date.fromisoformat(dre_item.get("data_inicio", ""))
        fim = date.fromisoformat(dre_item.get("data_fim", ""))
        return (fim - ini).days
    except (ValueError, TypeError):
        return 0
