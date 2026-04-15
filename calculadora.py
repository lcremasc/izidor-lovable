"""
calculadora.py
==============
Etapa 3 do pipeline de análise de crédito.
Recebe dict_p2 (extração de documentos) e produz dict_p3 (indicadores calculados).

Princípios:
  - Zero LLM — Python puro, resultados determinísticos
  - Cada variável tem fórmula explícita no docstring e no campo _formula
  - None propagado com segurança — divisão por zero retorna None, não exceção
  - Validação final contra schema P3

Uso:
    from calculadora import calcular
    dict_p3 = calcular(dict_p2)
"""

from __future__ import annotations

import statistics
from datetime import date
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MISSING = object()  # sentinel para _get — distingue "chave ausente" de valor falsy


def _get(d, *keys, default=None):
    """
    Navega dict (e opcionalmente lista com índice inteiro) com segurança.
    Usa sentinel para distinguir chave ausente de valores falsy legítimos (0, False).
    """
    cur = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k, _MISSING)
        elif isinstance(cur, list) and isinstance(k, int) and 0 <= k < len(cur):
            cur = cur[k]
        else:
            return default
        if cur is _MISSING:
            return default
    return cur


def _div(num, den, ndigits: int = 6):
    """
    Divisão segura.
    Retorna None se den == 0 ou qualquer operando for None.
    """
    if num is None or den is None or den == 0:
        return None
    return round(num / den, ndigits)


def _round(v, ndigits: int = 6):
    if v is None:
        return None
    return round(v, ndigits)


def _balanco_mais_recente(balanco: list[dict]) -> tuple[str | None, dict]:
    """Retorna (data_base, itens) do período mais recente disponível."""
    if not balanco:
        return None, {}
    mais_recente = max(balanco, key=lambda x: x.get("data_base", ""))
    return mais_recente.get("data_base"), mais_recente.get("itens", {})


def _dre_ano_fechado(dre: list[dict]) -> dict:
    """
    Retorna itens do último ano fechado (período ≥ 350 dias).
    Usado para rácios que exigem ano completo.
    """
    anuais = [p for p in dre if _periodo_dias(p) >= 350]
    if not anuais:
        return {}
    return max(anuais, key=lambda x: x.get("data_fim", "")).get("itens", {})


def _dre_mais_recente(dre: list[dict]) -> dict | None:
    """Retorna o período mais recente da DRE (maior data_fim), ou None."""
    if not dre:
        return None
    return max(dre, key=lambda x: x.get("data_fim", ""))


def _anualizar_campo(dre: list[dict], campo: str) -> float | None:
    """
    Retorna o valor de `campo` do período mais recente da DRE, anualizado.
    Fórmula: valor × (365 / dias_do_período)
    """
    periodo = _dre_mais_recente(dre)
    if not periodo:
        return None
    val = periodo.get("itens", {}).get(campo)
    if val is None:
        return None
    ini = periodo.get("data_inicio", "")
    fim = periodo.get("data_fim", "")
    return val * _fator_anualização(ini, fim)


# Constante de módulo — não recria dict a cada chamada
_MESES_NOME: dict[int, str] = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março",    4: "Abril",
    5: "Maio",    6: "Junho",     7: "Julho",     8: "Agosto",
    9: "Setembro",10: "Outubro",  11: "Novembro", 12: "Dezembro",
}


# ---------------------------------------------------------------------------
# 1. LIQUIDEZ
# ---------------------------------------------------------------------------

def calcular_liquidez(balanco: list[dict]) -> dict:
    """
    Indicadores de liquidez com base no balanço mais recente.

    Fórmulas:
      liquidez_corrente  = Ativo Circulante / Passivo Circulante
      liquidez_seca      = (Ativo Circulante - Estoque) / Passivo Circulante
      liquidez_geral     = Ativo Circulante / (Passivo Circulante + Passivo Não Circulante)
      liquidez_imediata  = Disponível / Passivo Circulante
    """
    periodo, itens = _balanco_mais_recente(balanco)
    if not itens:
        return {}

    ac  = itens.get("ativo_circulante")
    est = itens.get("estoques")
    dis = itens.get("disponivel")
    pc  = itens.get("passivo_circulante")
    pnc = itens.get("passivo_nao_circulante")

    pc_pnc = (pc or 0) + (pnc or 0) if pc is not None and pnc is not None else None

    return {
        "periodo_balanco":        periodo,
        "liquidez_corrente":      _div(ac, pc),
        "liquidez_seca":          _div((ac - est) if ac is not None and est is not None else None, pc),
        "liquidez_geral":         _div(ac, pc_pnc),
        "liquidez_imediata":      _div(dis, pc),
        "liquidez_corrente_formula":  "Ativo Circulante / Passivo Circulante",
        "liquidez_seca_formula":      "(Ativo Circulante - Estoque) / Passivo Circulante",
        "liquidez_geral_formula":     "Ativo Circulante / (Passivo Circulante + Passivo Não Circulante)",
        "liquidez_imediata_formula":  "Disponível / Passivo Circulante",
    }


# ---------------------------------------------------------------------------
# 2. MARGENS
# ---------------------------------------------------------------------------

def calcular_margens(dre: list[dict]) -> list[dict]:
    """
    Margens por período disponível na DRE.
    Denominador: Receita Líquida (após deduções e abatimentos).

    Fórmulas:
      margem_bruta    = Lucro Bruto / Receita Líquida
      margem_ebitda   = EBITDA / Receita Líquida
      margem_ebit     = EBIT / Receita Líquida
      margem_liquida  = Lucro Líquido / Receita Líquida
    """
    resultado = []
    for periodo in dre:
        itens    = periodo.get("itens", {})
        inicio   = periodo.get("data_inicio", "")
        fim      = periodo.get("data_fim", "")
        label    = f"{inicio} a {fim}"

        rl  = itens.get("receita_liquida")        # denominador = RL
        lb  = itens.get("lucro_bruto")
        ebt = itens.get("ebitda")
        ebit_val = itens.get("ebit")
        ebt_calc = ebit_val if ebit_val is not None else itens.get("lucro_operacional")
        ll  = itens.get("lucro_liquido")

        resultado.append({
            "periodo":        label,
            "margem_bruta":   _div(lb,       rl),
            "margem_ebitda":  _div(ebt,      rl),
            "margem_ebit":    _div(ebt_calc, rl),
            "margem_liquida": _div(ll,       rl),
            # fórmulas
            "margem_bruta_formula":   "Lucro Bruto / Receita Líquida",
            "margem_ebitda_formula":  "EBITDA / Receita Líquida",
            "margem_ebit_formula":    "EBIT / Receita Líquida",
            "margem_liquida_formula": "Lucro Líquido / Receita Líquida",
        })
    return resultado


# ---------------------------------------------------------------------------
# 3. RECEITA
# ---------------------------------------------------------------------------

def calcular_receita(
    dre: list[dict],
    faturamento_mensal: list[dict],
    quod: dict | None = None,
) -> dict:
    """
    Indicadores de receita.
    Usa o último ANO FECHADO (≥350 dias) como período de referência.

    Fórmulas:
      receita_media_mensal         = Receita Bruta do ano fechado / 12
      receita_media_mensal_recente:
        → Se faturamento_mensal tem ano > ano_fim da DRE:
            soma dos meses do ano mais recente / qtd de meses (independente da qtd)
        → Caso contrário (DRE mais recente ou sem faturamento_mensal):
            Receita Bruta DRE mais recente / 12
      cagr_receita                 = (Receita_fim / Receita_inicio) ^ (1 / n_anos) - 1
      crescimento_receita_mensal   = (Media_recente - Media_ano_fechado) / Media_ano_fechado
    """
    faturamento_presumido_quod = quod.get("faturamento_presumido") if quod else None

    # Guard: sem DRE retorna estrutura vazia tipada (não ValueError)
    if not dre:
        return {
            "periodo": None, "receita_bruta": None, "receita_liquida": None,
            "receita_media_mensal": None, "receita_media_mensal_recente": None,
            "periodo_receita_media_mensal_recente": None, "cagr_receita": None,
            "crescimento_receita_yoy": None, "crescimento_receita_mensal": None,
            "faturamento_presumido_quod": faturamento_presumido_quod,
        }

    # Último ano fechado
    anuais = [p for p in dre if _periodo_dias(p) >= 350]
    if not anuais:
        anuais = dre
    periodo_ref = max(anuais, key=lambda x: x.get("data_fim", ""))
    itens_ref   = periodo_ref.get("itens", {})
    ini_ref     = periodo_ref.get("data_inicio", "")
    fim_ref     = periodo_ref.get("data_fim", "")

    rb_ref = itens_ref.get("receita_bruta")
    rl_ref = itens_ref.get("receita_liquida")
    media_mensal = _div(rb_ref, 12)

    # CAGR — ano inicial E final precisam ter 12 meses completos
    cagr = None
    if faturamento_mensal:
        contagem = {}
        for r in faturamento_mensal:
            if r.get("year") and r.get("value") is not None:
                contagem[r["year"]] = contagem.get(r["year"], 0) + 1
        anos_completos = sorted([a for a, c in contagem.items() if c == 12])
        if len(anos_completos) >= 2:
            ano_ini_f = anos_completos[0]
            ano_fim_f = anos_completos[-1]
            n = ano_fim_f - ano_ini_f
            rb_ini_f = sum(r["value"] for r in faturamento_mensal
                           if r.get("year") == ano_ini_f and r.get("value") is not None)
            rb_fim_f = sum(r["value"] for r in faturamento_mensal
                           if r.get("year") == ano_fim_f and r.get("value") is not None)
            if rb_ini_f is not None and rb_fim_f is not None and n > 0 and rb_ini_f != 0:
                cagr = _round((rb_fim_f / rb_ini_f) ** (1 / n) - 1)

    # ── Média mensal recente ────────────────────────────────────────────────
    media_recente   = None
    periodo_recente = None

    fat_validos = sorted(
        [r for r in faturamento_mensal
         if r.get("year") and r.get("month") and r.get("value") is not None],
        key=lambda r: (r["year"], r["month"]),
        reverse=True,
    )

    ano_fim_dre = int(fim_ref[:4]) if fim_ref else 0
    ano_max_fat = fat_validos[0]["year"] if fat_validos else 0

    if fat_validos and ano_max_fat > ano_fim_dre:
        meses_recentes = [r for r in fat_validos if r["year"] == ano_max_fat]
        vals  = [r["value"] for r in meses_recentes]
        n_m   = len(vals)
        media_recente = _round(sum(vals) / n_m)
        meses_ord = sorted(r["month"] for r in meses_recentes)
        m_ini = f"{ano_max_fat}-{meses_ord[0]:02d}-01"
        m_fim = f"{ano_max_fat}-{meses_ord[-1]:02d}-{_ultimo_dia(ano_max_fat, meses_ord[-1])}"
        sufixo = "" if n_m == 12 else f" ({n_m}m)"
        periodo_recente = f"{m_ini} a {m_fim}{sufixo}"

    elif anuais:
        dre_rec = max(anuais, key=lambda x: x.get("data_fim", ""))
        rb_base = dre_rec.get("itens", {}).get("receita_bruta")
        media_recente   = _round(rb_base / 12) if rb_base is not None else None
        periodo_recente = (
            f"{dre_rec.get('data_inicio','')} a {dre_rec.get('data_fim','')} (via DRE)"
        )

    faturamento_presumido_quod = quod.get("faturamento_presumido") if quod else None

    # Crescimento: média recente vs média do ano fechado
    cresc_mensal = None
    if media_recente is not None and media_mensal is not None:
        cresc_mensal = _div(media_recente - media_mensal, media_mensal)

    return {
        "periodo":                              f"{ini_ref} a {fim_ref}",
        "receita_bruta":                        rb_ref,
        "receita_liquida":                      rl_ref,
        "receita_media_mensal":                 _round(media_mensal),
        "receita_media_mensal_recente":         media_recente,
        "periodo_receita_media_mensal_recente": periodo_recente,
        "cagr_receita":                         cagr,
        "crescimento_receita_yoy":              None,
        "crescimento_receita_mensal":           cresc_mensal,
        "faturamento_presumido_quod":           faturamento_presumido_quod,
        # fórmulas
        "receita_media_mensal_formula":             "Receita Bruta (ano fechado) / 12",
        "receita_media_mensal_recente_formula":     "Faturamento mensal ano recente / n_meses  (ou DRE/12 se DRE mais recente)",
        "cagr_receita_formula":                     "(Receita_fim / Receita_inicio) ^ (1 / n_anos) - 1",
        "crescimento_mensal_formula":               "(Média mensal recente - Média mensal ano fechado) / Média mensal ano fechado",
        "faturamento_presumido_quod_formula":       "Faixa textual do Quod — referência qualitativa, não calculada",
    }


def _ultimo_dia(ano: int, mes: int) -> str:
    """Retorna o último dia do mês como string 'DD'."""
    return str((date(ano + mes // 12, mes % 12 + 1, 1) - date(ano, mes, 1)).days)


# ---------------------------------------------------------------------------
# 4. FATURAMENTO — série mensal
# ---------------------------------------------------------------------------

def calcular_faturamento(faturamento_mensal: list[dict]) -> dict:
    """
    Série mensal de faturamento com variações homólogas.

    Fórmulas:
      variacao_ano_a_ano = (Fat_ano_n - Fat_ano_n-1) / Fat_ano_n-1
      cagr               = (Total_ano_fim / Total_ano_ini) ^ (1 / n_anos) - 1
      media_mensal       = Total_ano / meses_disponíveis
    """
    if not faturamento_mensal:
        return {}

    anos = sorted({r["year"] for r in faturamento_mensal if r.get("year")})

    # Organizar por (ano, mês)
    tabela: dict[tuple, float] = {}
    for r in faturamento_mensal:
        if r.get("year") and r.get("month") and r.get("value") is not None:
            tabela[(r["year"], r["month"])] = r["value"]

    todos_meses = sorted({m for (_, m) in tabela})

    # Série mensal
    serie = []
    for mes in todos_meses:
        item: dict = {"mes": mes, "mes_nome": _MESES_NOME.get(mes, str(mes))}
        for ano in anos:
            key = f"faturamento_{ano}"
            val = tabela.get((ano, mes))
            item[key] = int(val) if val is not None else None

        # Variações
        for i, ano in enumerate(anos[1:], 1):
            ano_ant = anos[i - 1]
            v_ant = tabela.get((ano_ant, mes))
            v_atu = tabela.get((ano, mes))
            chave = f"variacao_{ano_ant}_{ano}"
            item[chave] = _div(
                (v_atu - v_ant) if v_ant is not None and v_atu is not None else None,
                v_ant
            )
        serie.append(item)

    # Totais anuais
    totais: dict[str, Any] = {}
    medias: dict[str, Any] = {}
    for ano in anos:
        vals = [v for (a, _), v in tabela.items() if a == ano and v is not None]
        meses_disp = len(vals)
        total = int(sum(vals)) if vals else None
        media = _round(sum(vals) / meses_disp) if vals else None
        sufixo = "_parcial" if meses_disp < 12 else ""
        totais[f"total_{ano}{sufixo}"] = total
        if meses_disp < 12:
            totais[f"meses_{ano}_disponiveis"] = meses_disp
        medias[f"media_mensal_{ano}"] = media

    # Variação anual e CAGR
    variacao_anual: dict[str, Any] = {}
    for i, ano in enumerate(anos[1:], 1):
        ano_ant = anos[i - 1]
        t_ant = totais.get(f"total_{ano_ant}") if totais.get(f"total_{ano_ant}") is not None \
                else totais.get(f"total_{ano_ant}_parcial")
        t_atu = totais.get(f"total_{ano}") if totais.get(f"total_{ano}") is not None \
                else totais.get(f"total_{ano}_parcial")
        variacao_anual[f"variacao_{ano_ant}_{ano}"] = _div(
            (t_atu - t_ant) if t_ant is not None and t_atu is not None else None,
            t_ant
        )

    if len(anos) >= 2:
        ano_ini, ano_fim = anos[0], anos[-1]
        n = ano_fim - ano_ini
        t_ini = totais.get(f"total_{ano_ini}") if totais.get(f"total_{ano_ini}") is not None \
                else totais.get(f"total_{ano_ini}_parcial")
        t_fim = totais.get(f"total_{ano_fim}") if totais.get(f"total_{ano_fim}") is not None \
                else totais.get(f"total_{ano_fim}_parcial")
        if t_ini is not None and t_fim is not None and n > 0 and t_ini != 0:
            variacao_anual[f"cagr_{ano_ini}_{ano_fim}"] = _round(
                (t_fim / t_ini) ** (1 / n) - 1
            )

    return {
        "periodo":        f"{anos[0]} a {anos[-1]}",
        "unidade":        "BRL",
        "serie_mensal":   serie,
        "totais_anuais":  totais,
        "medias_mensais": medias,
        "variacao_anual": variacao_anual,
        # fórmulas
        "variacao_formula": "(Fat_ano_n - Fat_ano_n-1) / Fat_ano_n-1",
        "cagr_formula":     "(Total_ano_fim / Total_ano_ini) ^ (1 / n_anos) - 1",
        "media_formula":    "Total_ano / meses_disponíveis",
    }


# ---------------------------------------------------------------------------
# 5. ENDIVIDAMENTO
# ---------------------------------------------------------------------------

def calcular_endividamento(
    balanco: list[dict],
    scr: dict | None,
    dre: list[dict],
) -> dict:
    """
    Indicadores de endividamento e alavancagem.

    Fórmulas:
      divida_bruta          = carteira_credito_ativa do SCR Bacen
      divida_liquida        = Dívida Bruta - Disponível
      divida_pl             = Dívida Bruta / Patrimônio Líquido
      divida_ativo          = Dívida Bruta / Ativo Total
      divida_ebitda         = Dívida Bruta / EBITDA (ano referência)
      divida_liquida_ebitda = Dívida Líquida / EBITDA
      divida_liquida_pl     = Dívida Líquida / Patrimônio Líquido
      divida_curto_prazo    = soma modalidades com prazo='curto'
      divida_longo_prazo    = soma modalidades com prazo='longo'
    """
    _, itens = _balanco_mais_recente(balanco)

    pl  = itens.get("patrimonio_liquido")
    at  = itens.get("ativo_total")
    dis = itens.get("disponivel")

    # Dívida bruta — prioriza SCR (carteira ativa)
    divida_bruta = None
    modalidades  = []
    divida_cp    = None
    divida_lp    = None

    if scr:
        divida_bruta = scr.get("carteira_ativa")
        modalidades  = scr.get("modalidades_divida", [])

        # CP/LP pelo critério Bacen: portfolio_up_to_360_days
        # (dias corridos até vencimento, não por tipo de modalidade)
        p360 = scr.get("portfolio_up_to_360_days")
        if p360 is not None and divida_bruta is not None:
            divida_cp = p360
            divida_lp = round(divida_bruta - p360, 2)
        else:
            # Fallback: soma das modalidades por campo prazo
            divida_cp = sum(
                m["valor"] for m in modalidades
                if m.get("prazo") == "curto" and m.get("valor") is not None
            ) if modalidades else None
            divida_lp = sum(
                m["valor"] for m in modalidades
                if m.get("prazo") == "longo" and m.get("valor") is not None
            ) if modalidades else None

    # EBITDA do período mais recente anualizado
    ebitda = _anualizar_campo(dre, "ebitda")

    divida_liquida = (
        round(divida_bruta - dis, 2)
        if divida_bruta is not None and dis is not None
        else None
    )

    return {
        "divida_bruta":           _round(divida_bruta, 2),
        "divida_liquida":         _round(divida_liquida, 2),
        "divida_pl":              _div(divida_bruta, pl),
        "divida_ativo":           _div(divida_bruta, at),
        "divida_ebitda":          _div(divida_bruta, ebitda),
        "divida_liquida_ebitda":  _div(divida_liquida, ebitda),
        "divida_liquida_pl":      _div(divida_liquida, pl),
        "divida_curto_prazo":     _round(divida_cp, 2),
        "divida_longo_prazo":     _round(divida_lp, 2),
        # composição: ver scr_bacen.modalidades_divida
        # fórmulas
        "divida_bruta_formula":           "Carteira ativa SCR Bacen",
        "divida_liquida_formula":         "Dívida Bruta - Disponível (Caixa + Equivalentes)",
        "divida_pl_formula":              "Dívida Bruta / Patrimônio Líquido",
        "divida_ativo_formula":           "Dívida Bruta / Ativo Total",
        "divida_ebitda_formula":          "Dívida Bruta / EBITDA",
        "divida_liquida_ebitda_formula":  "Dívida Líquida / EBITDA",
        "divida_liquida_pl_formula":      "Dívida Líquida / Patrimônio Líquido",
        "divida_curto_prazo_formula":     "portfolio_up_to_360_days do SCR Bacen (critério Bacen de vencimento)",
        "divida_longo_prazo_formula":     "Carteira Ativa SCR - portfolio_up_to_360_days",
    }


# ---------------------------------------------------------------------------
# 6. RENTABILIDADE
# ---------------------------------------------------------------------------

def calcular_rentabilidade(
    balanco: list[dict],
    dre: list[dict],
) -> dict:
    """
    ROA, ROE e ROCE — anualizados quando período parcial.

    Fórmulas:
      ROA  = Lucro Líquido anualizado / Ativo Total
      ROE  = Lucro Líquido anualizado / Patrimônio Líquido
      ROCE = EBIT anualizado / (Ativo Total - Passivo Circulante)
    """
    _, itens_bal = _balanco_mais_recente(balanco)
    pl  = itens_bal.get("patrimonio_liquido")
    at  = itens_bal.get("ativo_total")
    pc  = itens_bal.get("passivo_circulante")

    dre_rec = _dre_mais_recente(dre)
    if not dre_rec:
        return {}

    itens    = dre_rec.get("itens", {})
    inicio   = dre_rec.get("data_inicio", "")
    fim      = dre_rec.get("data_fim", "")
    periodo  = f"{inicio} a {fim}"

    ll       = itens.get("lucro_liquido")
    ebit_val = itens.get("ebit")
    ebit     = ebit_val if ebit_val is not None else itens.get("lucro_operacional")

    # Fator de anualização
    fator = _fator_anualização(inicio, fim)

    ll_anual   = (ll   * fator) if ll   is not None else None
    ebit_anual = (ebit * fator) if ebit is not None else None

    ce = (at - pc) if at is not None and pc is not None else None  # Capital Empregado

    sufixo = f" (anualizado x{fator:.1f})" if fator != 1.0 else ""

    return {
        "periodo":  periodo + sufixo,
        "roa":      _div(ll_anual, at),
        "roe":      _div(ll_anual, pl),
        "roce":     _div(ebit_anual, ce),
        # fórmulas
        "roa_formula":  "Lucro Líquido anualizado / Ativo Total",
        "roe_formula":  "Lucro Líquido anualizado / Patrimônio Líquido",
        "roce_formula": "EBIT anualizado / (Ativo Total - Passivo Circulante)",
    }


def _fator_anualização(inicio: str, fim: str) -> float:
    """
    Calcula fator para anualizar resultado de período parcial.
    Fórmula: 365 / dias_do_período
    Exemplos: 6m (181 dias) → 2.017, 3m (92 dias) → 3.967, 12m (365 dias) → 1.0
    """
    try:
        d_ini = date.fromisoformat(inicio)
        d_fim = date.fromisoformat(fim)
        dias  = (d_fim - d_ini).days + 1
        return round(365 / dias, 6) if dias > 0 else 1.0
    except (ValueError, TypeError):
        return 1.0


# ---------------------------------------------------------------------------
# 7. CICLOS OPERACIONAIS
# ---------------------------------------------------------------------------

def calcular_ciclos(balanco: list[dict], dre: list[dict]) -> dict:
    """
    Prazos médios e ciclos operacional e financeiro.
    Usa o período mais recente da DRE com anualização, e balanço mais recente.

    Fórmulas:
      PME  (Prazo Médio Estoque)      = (Estoque / CMV anualizado) × 365
      PMR  (Prazo Médio Recebimento)  = (Contas a Receber / Receita Bruta anualizada) × 365
      PMP  (Prazo Médio Pagamento)    = (Fornecedores / CMV anualizado) × 365
      CO   (Ciclo Operacional)        = PME + PMR
      CF   (Ciclo Financeiro)         = CO - PMP

    Nota: usa CMV e RB anualizados do período mais recente disponível.
    Fornecedores mapeados como "Obrigações com Terceiros" (conta sintética).
    Para balanços granulares com conta "Fornecedores" separada, PMR/PMP serão mais precisos.
    """
    _, itens_bal = _balanco_mais_recente(balanco)

    rec_periodo = _dre_mais_recente(dre)
    if not rec_periodo:
        return {}

    itens_dre = rec_periodo.get("itens", {})
    ini = rec_periodo.get("data_inicio", "")
    fim = rec_periodo.get("data_fim", "")
    fator = _fator_anualização(ini, fim)

    cmv_raw = itens_dre.get("cmv")
    rb_raw  = itens_dre.get("receita_bruta")

    cmv_pos = abs(cmv_raw) * fator if cmv_raw is not None else None
    rb_anual = rb_raw * fator       if rb_raw  is not None else None

    est  = itens_bal.get("estoques")
    cr   = itens_bal.get("creditos")
    forn = itens_bal.get("fornecedores")

    pme = _round(_div((est  * 365) if est  is not None else None, cmv_pos), 2)
    pmr = _round(_div((cr   * 365) if cr   is not None else None, rb_anual), 2)
    pmp = _round(_div((forn * 365) if forn is not None else None, cmv_pos), 2)

    co = _round((pme + pmr) if pme is not None and pmr is not None else None, 2)
    cf = _round((co - pmp)  if co  is not None and pmp is not None else None, 2)

    return {
        "prazo_medio_estoque":      pme,
        "prazo_medio_recebimento":  pmr,
        "prazo_medio_pagamento":    pmp,
        "ciclo_operacional":        co,
        "ciclo_financeiro":         cf,
        # fórmulas
        "pme_formula":  "(Estoque / CMV anualizado) × 365",
        "pmr_formula":  "(Contas a Receber / Receita Bruta anualizada) × 365",
        "pmp_formula":  "(Fornecedores / CMV anualizado) × 365",
        "co_formula":   "PME + PMR",
        "cf_formula":   "Ciclo Operacional - PMP",
        "nota":         "PMR e PMP mais precisos com balanço granular separando Clientes e Fornecedores de outras obrigações",
    }


def _periodo_dias(dre_periodo: dict) -> int:
    try:
        ini = date.fromisoformat(dre_periodo.get("data_inicio", ""))
        fim = date.fromisoformat(dre_periodo.get("data_fim", ""))
        return (fim - ini).days
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# 8. CAPITAL DE GIRO
# ---------------------------------------------------------------------------

def calcular_capital_giro(balanco: list[dict], dre: list[dict]) -> dict:
    """
    Necessidade de capital de giro e seus componentes em relação à receita.
    Denominador: Receita Líquida anualizada do período mais recente disponível.

    Fórmulas:
      ncg_receita              = (Contas a Receber + Estoque - Fornecedores) / RL anualizada
      estoque_receita          = Estoque / RL anualizada
      contas_receber_receita   = Contas a Receber / RL anualizada
      fornecedores_receita     = Fornecedores / RL anualizada

    Nota: "Fornecedores" mapeado como "Obrigações com Terceiros" no balanço sintético.
    Para balanços com conta "Fornecedores" separada, o resultado será mais preciso.
    """
    _, itens_bal = _balanco_mais_recente(balanco)

    # Receita Líquida do período mais recente anualizada
    rl_anual = _anualizar_campo(dre, "receita_liquida")

    est  = itens_bal.get("estoques")
    cr   = itens_bal.get("creditos")
    forn = itens_bal.get("fornecedores")
    ncg  = None
    if est is not None and cr is not None and forn is not None:
        ncg = cr + est - forn

    return {
        "ncg_receita":             _div(ncg,  rl_anual),
        "estoque_receita":         _div(est,  rl_anual),
        "contas_receber_receita":  _div(cr,   rl_anual),
        "fornecedores_receita":    _div(forn, rl_anual),
        # fórmulas
        "ncg_formula":                    "(Contas a Receber + Estoque - Fornecedores) / Receita Líquida anualizada",
        "estoque_receita_formula":        "Estoque / Receita Líquida anualizada",
        "contas_receber_receita_formula": "Contas a Receber / Receita Líquida anualizada",
        "fornecedores_receita_formula":   "Fornecedores / Receita Líquida anualizada",
    }


# ---------------------------------------------------------------------------
# 9. ESTRUTURA DE CAPITAL
# ---------------------------------------------------------------------------

def calcular_estrutura_capital(
    balanco: list[dict],
    dre: list[dict],
    scr_raw: dict | None = None,
) -> dict:
    """
    Indicadores de estrutura de capital e capitalização.

    Fórmulas:
      patrimonio_liquido_ativo  = PL / Ativo Total
      capitalizacao             = Ativo Permanente / PL
                                  (grau de imobilização do capital próprio)
      endividamento_bancario    = Dívida Bruta SCR / PL
      ll_pl                     = Lucro Líquido anualizado / PL  (= ROE)
      capital_social_ativo      = Capital Social / Ativo Total
      lucros_acumulados_pl      = Resultados Acumulados / PL
    """
    _, itens = _balanco_mais_recente(balanco)

    pl  = itens.get("patrimonio_liquido")
    at  = itens.get("ativo_total")
    ap  = itens.get("ativo_permanente")
    cs  = itens.get("capital_social")
    ra  = itens.get("resultados_acumulados")

    # Dívida bruta: carteira ativa do SCR (mesma base do bloco endividamento)
    divida_bruta = scr_raw.get("carteira_ativa") if scr_raw else None

    # ll_pl: lucro líquido do período mais recente anualizado
    ll_anual = _anualizar_campo(dre, "lucro_liquido")

    return {
        "patrimonio_liquido":         _round(pl, 2),
        "patrimonio_liquido_ativo":   _div(pl,          at),
        "capitalizacao":              _div(ap,          pl),
        "endividamento_bancario":     _div(divida_bruta, pl),
        "ll_pl":                      _div(ll_anual,    pl),
        "capital_social_ativo":       _div(cs,          at),
        "lucros_acumulados_pl":       _div(ra,          pl),
        # fórmulas
        "patrimonio_liquido_ativo_formula":  "Patrimônio Líquido / Ativo Total",
        "capitalizacao_formula":             "Ativo Permanente / Patrimônio Líquido (grau de imobilização do PL)",
        "endividamento_bancario_formula":    "Dívida Bruta SCR / Patrimônio Líquido",
        "ll_pl_formula":                     "Lucro Líquido anualizado / Patrimônio Líquido (= ROE)",
        "capital_social_ativo_formula":      "Capital Social / Ativo Total",
        "lucros_acumulados_pl_formula":      "Resultados Acumulados / Patrimônio Líquido",
    }


# ---------------------------------------------------------------------------
# 10. SCR BACEN — breakdown
# ---------------------------------------------------------------------------

def calcular_scr(scr_raw: dict | None) -> dict:
    """
    Organiza e enriquece os dados do SCR Bacen.

    Fórmulas:
      overdue_carteira         = Crédito Vencido / Carteira Ativa
      credito_utilizado_limite = Carteira Ativa / Limite de Crédito
      tempo_relacionamento     = (Hoje - Data Início Relacionamento) / 365
      pct por modalidade       = Valor Modalidade / Carteira Ativa
    """
    if not scr_raw:
        return {}

    carteira   = scr_raw.get("carteira_ativa")
    vencido    = scr_raw.get("overdue", 0.0)
    perda      = scr_raw.get("perda", 0.0)
    limite     = scr_raw.get("limite_credito")
    coob       = scr_raw.get("coobrigacoes", 0.0)
    num_ops    = scr_raw.get("num_operacoes")
    num_ifs    = scr_raw.get("num_instituicoes")
    dt_inicio  = scr_raw.get("data_inicio_relacionamento")
    modalidades = scr_raw.get("modalidades_divida", [])

    # Tempo de relacionamento em anos
    tempo_rel = None
    if dt_inicio:
        try:
            d_ini  = date.fromisoformat(dt_inicio)
            tempo_rel = _round((date.today() - d_ini).days / 365, 2)
        except ValueError:
            pass

    # CP/LP usando portfolio_up_to_360_days do SCR (critério Bacen)
    # CP = carteira com vencimento até 360 dias
    # LP = carteira total - carteira até 360 dias
    p360 = scr_raw.get("portfolio_up_to_360_days")
    if p360 is not None and carteira is not None:
        divida_cp = p360
        divida_lp = round(carteira - p360, 2)
        total_modal = carteira  # usa carteira total como referência
    else:
        # fallback: soma das modalidades
        divida_cp = sum(
            m["valor"] for m in modalidades if m.get("prazo") == "curto" and m.get("valor") is not None
        ) if modalidades else 0.0
        divida_lp = sum(
            m["valor"] for m in modalidades if m.get("prazo") == "longo" and m.get("valor") is not None
        ) if modalidades else 0.0
        total_modal = divida_cp + divida_lp
    # Enriquecer modalidades com pct sobre carteira total
    total_modal_pct = carteira if carteira else (total_modal or 1)
    modalidades_enriquecidas = []
    for m in modalidades:
        val = m.get("valor")
        modalidades_enriquecidas.append({
            **m,
            "pct": _div(val, total_modal_pct) if val is not None else None,
        })

    return {
        "carteira_credito_ativa":        _round(carteira, 2),
        "credito_vencido":               _round(vencido, 2),
        "overdue_carteira":              _div(vencido, carteira),
        "perda_carteira":                _round(perda, 2),
        "credito_perda":                 _round(perda, 2),
        "limite_credito_total":          _round(limite, 2),
        "credito_utilizado_limite":      _div(carteira, limite),
        "numero_operacoes_credito":      num_ops,
        "numero_instituicoes_financeiras": num_ifs,
        "tempo_relacionamento_bancario": tempo_rel,
        "coobrigacoes":                  _round(coob, 2),
        "modalidades_divida":            modalidades_enriquecidas,
        "divida_curto_prazo":            _round(divida_cp, 2),
        "divida_longo_prazo":            _round(divida_lp, 2),
        "divida_total":                  _round(total_modal, 2),
        # fórmulas
        "overdue_carteira_formula":         "Crédito Vencido / Carteira Ativa",
        "credito_utilizado_limite_formula": "Carteira Ativa / Limite de Crédito Total",
        "tempo_relacionamento_formula":     "(Data Hoje - Data Início Relacionamento) / 365",
        "pct_modalidade_formula":           "Valor Modalidade / Total Carteira Ativa",
    }


# ---------------------------------------------------------------------------
# 11. RECEBÍVEIS DE CARTÃO / CERC
# ---------------------------------------------------------------------------

def calcular_cartao_recebiveis(
    cerc_raw: dict | None,
    nuclea: dict | None,
    dre: list[dict],
    scr_raw: dict | None = None,
) -> dict:
    """
    Indicadores de recebíveis de cartão.

    Fórmulas:
      faturamento_cartao_total   = Soma do histórico de agenda CERC (12 meses)
      faturamento_cartao_mensal  = faturamento_cartao_total / 12
      cartao_receita             = faturamento_cartao_total / Receita Bruta anual
      dependencia_adquirente     = market_share do maior adquirente
      recebiveis_cartao_divida   = faturamento_cartao_total / Dívida Bruta
      volatilidade               = Desvio Padrão / Média (coef. de variação da agenda mensal)
    """
    # Histórico de agenda (12 meses)
    historico = []
    if cerc_raw:
        raw_items = cerc_raw.get("raw_items", [])
        for item in raw_items:
            historico.extend(item.get("historico_agenda", []))

    vals_agenda = [
        h["valor_liquidado"] for h in historico
        if h.get("valor_liquidado") is not None
    ]

    fat_total  = _round(sum(vals_agenda), 2) if vals_agenda else None
    fat_mensal = _div(fat_total, len(vals_agenda)) if vals_agenda else None

    # Receita Líquida anualizada do período mais recente
    rl_anual_cart = _anualizar_campo(dre, "receita_liquida")

    # Dívida bruta (para recebiveis_cartao_divida)
    divida_bruta = scr_raw.get("carteira_ativa") if scr_raw else None

    # Maior adquirente (CERC market_share_adquirente)
    dep_adquirente = None
    if cerc_raw:
        for item in cerc_raw.get("raw_items", []):
            shares = item.get("market_share_adquirente", [])
            if shares:
                dep_adquirente = max(s.get("market_share", 0) for s in shares)
                break

    # Volatilidade (coeficiente de variação)
    volatilidade = None
    if len(vals_agenda) >= 2:
        media_ag = sum(vals_agenda) / len(vals_agenda)
        dp_ag    = statistics.stdev(vals_agenda)
        volatilidade = _div(dp_ag, media_ag)

    return {
        "faturamento_cartao_total":         fat_total,
        "faturamento_cartao_mensal":        _round(fat_mensal, 2),
        "cartao_receita":                   _div(fat_total, rl_anual_cart),
        "dependencia_adquirente":           _round(dep_adquirente, 4),
        "dependencia_bandeira":             None,  # não disponível nas fontes atuais
        "ticket_medio_cartao":              None,  # não disponível
        "prazo_medio_parcelamento":         None,  # não disponível
        "recebiveis_cartao_divida":         _div(fat_total, divida_bruta),
        "volatilidade_faturamento_cartao":  _round(volatilidade, 6),
        "crescimento_faturamento_cartao":   None,
        # fórmulas
        "faturamento_cartao_total_formula":        "Soma do histórico de agenda CERC (últimos 12 meses)",
        "faturamento_cartao_mensal_formula":       "faturamento_cartao_total / n_meses",
        "cartao_receita_formula":                  "faturamento_cartao_total / Receita Líquida anualizada",
        "dependencia_adquirente_formula":          "market_share do maior adquirente (CERC)",
        "recebiveis_cartao_divida_formula":        "faturamento_cartao_total / Dívida Bruta (SCR)",
        "volatilidade_formula":                    "Desvio Padrão / Média (coef. de variação da agenda mensal)",
    }


# ---------------------------------------------------------------------------
# 12. EFICIÊNCIA OPERACIONAL
# ---------------------------------------------------------------------------

def calcular_eficiencia(balanco: list[dict], dre: list[dict]) -> dict:
    """
    Giros operacionais — todos com período mais recente anualizado.

    Fórmulas:
      giro_ativos         = Receita Líquida anualizada / Ativo Total
      giro_estoque        = CMV anualizado / Estoque
      giro_contas_pagar   = CMV anualizado / Fornecedores
      giro_contas_receber = Receita Bruta anualizada / Contas a Receber
    """
    _, itens_bal = _balanco_mais_recente(balanco)

    rec = _dre_mais_recente(dre)
    if not rec:
        return {}

    itens_d  = rec.get("itens", {})
    ini, fim = rec.get("data_inicio", ""), rec.get("data_fim", "")
    fator    = _fator_anualização(ini, fim)

    at   = itens_bal.get("ativo_total")
    est  = itens_bal.get("estoques")
    forn = itens_bal.get("fornecedores")
    cr   = itens_bal.get("creditos")

    rl_raw  = itens_d.get("receita_liquida")
    rb_raw  = itens_d.get("receita_bruta")
    cmv_raw = itens_d.get("cmv")

    rl_anual  = rl_raw  * fator          if rl_raw  is not None else None
    rb_anual  = rb_raw  * fator          if rb_raw  is not None else None
    cmv_anual = abs(cmv_raw) * fator     if cmv_raw is not None else None

    return {
        "giro_ativos":          _div(rl_anual,  at),
        "giro_estoque":         _div(cmv_anual, est),
        "giro_contas_pagar":    _div(cmv_anual, forn),
        "giro_contas_receber":  _div(rb_anual,  cr),
        # fórmulas
        "giro_ativos_formula":         "Receita Líquida anualizada / Ativo Total",
        "giro_estoque_formula":        "CMV anualizado / Estoque",
        "giro_contas_pagar_formula":   "CMV anualizado / Fornecedores",
        "giro_contas_receber_formula": "Receita Bruta anualizada / Contas a Receber",
    }


# ---------------------------------------------------------------------------
# 13. TRANSACIONAL NUCLEA
# ---------------------------------------------------------------------------

def calcular_transacional_nuclea(nuclea: dict | None, dre: list[dict]) -> dict | None:
    """
    Indicadores derivados dos dados transacionais da Nuclea.

    Fórmulas:
      pagamentos_receita              = Valores Pagos / Receita Líquida anualizada
      faturamento_transacional_receita = Faturamento Transacional / Receita Líquida anualizada
    """
    if not nuclea:
        return None

    # Receita Líquida anualizada do período mais recente
    rl_anual = _anualizar_campo(dre, "receita_liquida")

    fat_trans   = nuclea.get("faturamento_transacional")
    pag_total   = nuclea.get("valores_pagos")
    liq_pag     = nuclea.get("liquidez_pagamentos")
    liq_rec     = nuclea.get("liquidez_recebimento")
    conc_cli    = nuclea.get("concentracao_clientes")
    conc_forn   = nuclea.get("concentracao_fornecedores")

    return {
        "faturamento_transacional":           fat_trans,
        "pagamentos_total":                   pag_total,
        "liquidez_pagamento":                 liq_pag,
        "liquidez_recebimento":               liq_rec,
        "concentracao_clientes":              conc_cli,
        "concentracao_fornecedores":          conc_forn,
        "pagamentos_receita":                 _div(pag_total, rl_anual),
        "faturamento_transacional_receita":   _div(fat_trans, rl_anual),
        # fórmulas
        "pagamentos_receita_formula":               "Valores Pagos (Nuclea) / Receita Líquida anualizada",
        "faturamento_transacional_receita_formula": "Faturamento Transacional (Nuclea) / Receita Líquida anualizada",
    }


# ---------------------------------------------------------------------------
# 15. RESTRITIVOS BUREAU
# ---------------------------------------------------------------------------

def calcular_restritivos_bureau(serasa: dict | None, quod: dict | None = None) -> dict:
    """
    Consolida restritivos dos bureaux. Passagem direta com campos calculados do Quod.

    Campos Quod incluídos:
      quod_score            — score numérico (passagem direta)
      quod_pontualidade     — classificação qualitativa de pontualidade de pagamento
      quod_inadimplencia    — flag de inadimplência registrada
      quod_faturamento_presumido — faixa de faturamento estimada pelo Quod (texto)
    """
    resultado = {
        # ── Serasa ────────────────────────────────────────────────────────
        "serasa_score":               serasa.get("score")               if serasa else None,
        "pefin_valor":                serasa.get("pefin")               if serasa else None,
        "refin_valor":                serasa.get("refin")               if serasa else None,
        "divida_vencida_valor":       serasa.get("divida_vencida")      if serasa else None,
        "protestos_valor":            serasa.get("protestos")           if serasa else None,
        "acoes_judiciais_valor":      serasa.get("acoes_judiciais")     if serasa else None,
        "falencia_valor":             serasa.get("falencia")            if serasa else None,
        "recuperacao_judicial_valor": serasa.get("recuperacao_judicial") if serasa else None,
        "cheques_sem_fundo":          serasa.get("cheque_sem_fundo")    if serasa else None,
        "serasa_consultas":           serasa.get("consultas_12m")       if serasa else None,
        # ── Quod ──────────────────────────────────────────────────────────
        "quod_score":                 quod.get("score")                 if quod else None,
        "quod_pontualidade":          quod.get("pontualidade_pagamento") if quod else None,
        "quod_inadimplencia":         quod.get("inadimplencia")         if quod else None,
        "quod_faturamento_presumido": quod.get("faturamento_presumido") if quod else None,
    }
    return resultado


# ---------------------------------------------------------------------------
# 16. RESTRITIVOS RELATIVOS
# ---------------------------------------------------------------------------

def calcular_restritivos_relativos(
    restritivos: dict,
    dre: list[dict],
    balanco: list[dict],
) -> dict:
    """
    Normaliza restritivos em relação a receita líquida anualizada, EBITDA anualizado, PL e ativo.
    Total = PEFIN + REFIN + Dívida Vencida + Protestos (ações judiciais não entram no total)

    Fórmulas:
      X_receita = Valor_Restritivo / Receita Líquida anualizada (período mais recente)
      X_ebitda  = Valor_Restritivo / EBITDA anualizado (período mais recente)
      X_pl      = Total_Restritivos / PL
      X_ativo   = Total_Restritivos / Ativo Total
    """
    # RL e EBITDA do período mais recente, ambos anualizados
    rl_anual    = _anualizar_campo(dre, "receita_liquida")
    ebtda_anual = _anualizar_campo(dre, "ebitda")

    _, itens_bal = _balanco_mais_recente(balanco)
    pl = itens_bal.get("patrimonio_liquido")
    at = itens_bal.get("ativo_total")

    pefin  = restritivos.get("pefin_valor")         or 0
    refin  = restritivos.get("refin_valor")         or 0
    div_v  = restritivos.get("divida_vencida_valor") or 0
    prot   = restritivos.get("protestos_valor")     or 0
    ajud   = restritivos.get("acoes_judiciais_valor") or 0
    total  = pefin + refin + div_v + prot  # ações judiciais não entram no total

    return {
        "pefin_receita":              _div(pefin,  rl_anual),
        "pefin_ebitda":               _div(pefin,  ebtda_anual),
        "refin_receita":              _div(refin,  rl_anual),
        "refin_ebitda":               _div(refin,  ebtda_anual),
        "divida_vencida_receita":     _div(div_v,  rl_anual),
        "divida_vencida_ebitda":      _div(div_v,  ebtda_anual),
        "protestos_receita":          _div(prot,   rl_anual),
        "protestos_ebitda":           _div(prot,   ebtda_anual),
        "acoes_judiciais_receita":    _div(ajud,   rl_anual),
        "restritivos_totais_receita": _div(total,  rl_anual),
        "restritivos_totais_ebitda":  _div(total,  ebtda_anual),
        "restritivos_pl":             _div(total,  pl),
        "restritivos_ativo":          _div(total,  at),
        # fórmulas
        "formula_geral": "Valor Restritivo / Base (RL anualizada, EBITDA anualizado, PL ou Ativo)",
        "total_formula": "PEFIN + REFIN + Dívida Vencida + Protestos (ações judiciais não entram no total)",
    }


# ---------------------------------------------------------------------------
# 16. GRUPO ECONÔMICO
# ---------------------------------------------------------------------------

def calcular_grupo_economico(
    p2: dict,
    dre: list[dict],
    scr_raw: dict | None,
) -> dict:
    """
    Consolidação do grupo econômico.
    Quando não há balanço consolidado, usa dados da matriz com observação.

    Fórmulas:
      receita_consolidada  = Receita Bruta do período mais recente (anualizada se parcial)
      ebitda_consolidado   = EBITDA do período mais recente (anualizado se parcial)
      divida_consolidada   = Carteira SCR ativa
      idade_media_empresas = Média de anos desde a abertura de cada CNPJ do grupo
    """
    cnpjs         = p2.get("cnpjs_consultados", [])
    raizes        = p2.get("cnpjs_raiz_identificados", [])
    dados_cad     = p2.get("dados_cadastrais_raiz", [])
    scr           = scr_raw or {}

    n_cnpjs = len(cnpjs)
    n_raizes = len(raizes)

    # Idade média das empresas
    idade_media = None
    idades = []
    hoje = date.today()
    for item in dados_cad:
        # Tenta extrair data de abertura do QSA ou dados cadastrais CERC
        abertura = item.get("data_abertura") or item.get("data_opcao_pelo_simples")
        # Fallback: pega do commercial_inputs.cerc.dados_cadastrais
        if not abertura:
            cerc_items = _get(p2, "commercial_inputs", "cerc", "raw_items") or []
            for ci in cerc_items:
                ab = _get(ci, "dados_cadastrais", "data_abertura")
                if ab:
                    abertura = ab
                    break
        if abertura:
            try:
                d_ab   = date.fromisoformat(str(abertura)[:10])
                idades.append((hoje - d_ab).days / 365)
            except ValueError:
                pass
    if idades:
        idade_media = _round(sum(idades) / len(idades), 2)

    # Receita e EBITDA do período mais recente, anualizados
    rec_periodo = _dre_mais_recente(dre)
    receita_obs = None
    ebitda_obs  = None
    receita_val = None
    ebitda_val  = None

    if rec_periodo:
        itens = rec_periodo.get("itens", {})
        ini   = rec_periodo.get("data_inicio", "")
        fim   = rec_periodo.get("data_fim", "")
        fator = _fator_anualização(ini, fim)
        rb    = itens.get("receita_bruta")
        ebt   = itens.get("ebitda")

        if rb is not None:
            receita_val = _round(rb * fator, 0)
        if ebt is not None:
            ebitda_val = _round(ebt * fator, 0)

        if fator != 1.0:
            sufixo = f"anualizado (×{fator:.1f}) — apenas matriz"
            receita_obs = f"Receita Bruta {ini[:7]} a {fim[:7]} {sufixo}."
            ebitda_obs  = f"EBITDA {ini[:7]} a {fim[:7]} {sufixo}."

    # Restritivos consolidados (Serasa PEFIN como proxy)
    serasa = p2.get("bureaux", {}).get("serasa", {}) or {}
    restrit_total = (serasa.get("pefin") or 0) + (serasa.get("protestos") or 0)

    return {
        "numero_cnpjs":                    n_cnpjs or 1,
        "numero_raizes_cnpj":              n_raizes or 1,
        "idade_media_empresas":            idade_media,
        "capital_social_consolidado":      _get(p2, "dados_cadastrais_raiz", 0, "capital_social"),
        "receita_consolidada":             receita_val,
        "ebitda_consolidado":              ebitda_val,
        "divida_consolidada":              _round(scr.get("carteira_ativa"), 2),
        "restritivos_consolidados":        restrit_total,
        "receita_consolidada_observacao":  receita_obs,
        "ebitda_consolidado_observacao":   ebitda_obs,
        # fórmulas
        "receita_consolidada_formula":  "Receita Bruta do período mais recente × fator anualização",
        "ebitda_consolidado_formula":   "EBITDA do período mais recente × fator anualização",
        "divida_consolidada_formula":   "Carteira de crédito ativa (SCR Bacen)",
        "idade_media_formula":          "Média de (Hoje - Data Abertura) / 365 para cada CNPJ",
    }


# ---------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ---------------------------------------------------------------------------

def calcular(p2: dict) -> dict:
    """
    Recebe dict_p2 e retorna dict_p3 completo.

    Ordem de execução:
      1.  liquidez
      2.  margens
      3.  receita
      4.  faturamento
      5.  endividamento
      6.  rentabilidade
      7.  ciclos_operacionais
      8.  capital_giro
      9.  estrutura_capital
      10. scr_bacen
      11. cartao_recebiveis
      12. eficiencia_operacional
      13. transacional_nuclea
      14. restritivos_bureau
      15. restritivos_relativos
      16. grupo_economico
    """
    balanco    = p2.get("balanco", [])
    dre        = p2.get("dre", [])
    fat_mensal = p2.get("faturamento_mensal", [])
    bureaux    = p2.get("bureaux", {})
    serasa     = bureaux.get("serasa") or {}
    quod       = bureaux.get("quod")       # pode ser dict ou None
    nuclea     = bureaux.get("nuclea")
    scr_raw    = bureaux.get("scr_bacen")
    cerc_raw   = _get(p2, "commercial_inputs", "cerc")

    # 1. Liquidez
    liquidez = calcular_liquidez(balanco)

    # 2. Margens
    margens = calcular_margens(dre)

    # 3. Receita (passa quod para faturamento_presumido_vs_declarado)
    receita = calcular_receita(dre, fat_mensal, quod=quod)

    # 4. Faturamento série
    faturamento = calcular_faturamento(fat_mensal)

    # 5. SCR (precisa antes de endividamento)
    scr = calcular_scr(scr_raw)

    # 6. Endividamento
    endividamento = calcular_endividamento(balanco, scr_raw, dre)

    # 7. Rentabilidade
    rentabilidade = calcular_rentabilidade(balanco, dre)

    # 8. Ciclos
    ciclos = calcular_ciclos(balanco, dre)

    # 9. Capital de giro
    capital_giro = calcular_capital_giro(balanco, dre)

    # 10. Estrutura de capital
    estrutura = calcular_estrutura_capital(balanco, dre, scr_raw=scr_raw)

    # 11. Cartão / CERC
    cartao = calcular_cartao_recebiveis(cerc_raw, nuclea, dre, scr_raw)

    # 12. Eficiência
    eficiencia = calcular_eficiencia(balanco, dre)

    # 14. Transacional Nuclea
    transacional = calcular_transacional_nuclea(nuclea, dre)

    # 15. Restritivos bureau (Serasa + Quod)
    restritivos = calcular_restritivos_bureau(serasa, quod=quod)

    # 16. Restritivos relativos
    rest_rel = calcular_restritivos_relativos(restritivos, dre, balanco)

    # 17. Grupo econômico
    grupo = calcular_grupo_economico(p2, dre, scr_raw)

    return {
        "liquidez":               liquidez,
        "margens":                margens,
        "receita":                receita,
        "faturamento":            faturamento,
        "endividamento":          endividamento,
        "rentabilidade":          rentabilidade,
        "ciclos_operacionais":    ciclos,
        "capital_giro":           capital_giro,
        "estrutura_capital":      estrutura,
        "scr_bacen":              scr,
        "cartao_recebiveis":      cartao,
        "eficiencia_operacional": eficiencia,
        "transacional_nuclea":    transacional,
        "restritivos_bureau":     restritivos,
        "restritivos_relativos":  rest_rel,
        "grupo_economico":        grupo,
    }
