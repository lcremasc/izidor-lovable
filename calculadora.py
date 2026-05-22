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

    Para os campos derivados 'ebit', 'ebitda', 'lucro_bruto', aplica a hierarquia
    de cálculo de _calcular_lucro_bruto_ebit_ebitda quando vêm null no p2.
    """
    periodo = _dre_mais_recente(dre)
    if not periodo:
        return None

    itens = periodo.get("itens", {})

    # Para campos derivados, aplica hierarquia de fallback
    if campo in ("ebit", "ebitda", "lucro_bruto"):
        calc = _calcular_lucro_bruto_ebit_ebitda(itens)
        val = calc.get(campo)
    else:
        val = itens.get(campo)

    if val is None:
        return None

    ini = periodo.get("data_inicio", "")
    fim = periodo.get("data_fim", "")
    return val * _fator_anualização(ini, fim)


# Constante de módulo — não recria dict a cada chamada
_MESES_NOME: dict[int, str] = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
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

def _calcular_lucro_bruto_ebit_ebitda(itens: dict) -> dict:
    """
    Aplica a hierarquia de fallback para Lucro Bruto, EBIT e EBITDA quando vêm null no p2.

    Hierarquia:
      Lucro Bruto:
        1. Usa valor explícito do p2 se existir
        2. Senão, calcula: Receita Líquida − |CMV|

      EBIT:
        1. Usa valor explícito do p2 se existir
        2. Senão, calcula: Lucro Bruto − |Despesas Operacionais| + Outras Receitas Operacionais
           (Outras Receitas Op tratadas como parte do resultado operacional)

      EBITDA:
        1. Usa valor explícito do p2 se existir
        2. Se Depreciação existe: EBITDA = EBIT + |Depreciação|
        3. Se Depreciação null: EBITDA = EBIT (subestimado, com aviso)

    Retorna dict com:
      lucro_bruto, ebit, ebitda          → valores finais (calculados ou do p2)
      lucro_bruto_origem, ebit_origem, ebitda_origem  → "p2" ou "calculado"
      alertas → lista de avisos
    """
    rl       = itens.get("receita_liquida")
    cmv      = itens.get("cmv")
    lb_p2    = itens.get("lucro_bruto")
    desp_op  = itens.get("despesas_operacionais")
    out_rec  = itens.get("outras_receitas_operacionais") or 0
    ebit_p2  = itens.get("ebit")
    deprec   = itens.get("depreciacao")
    ebitda_p2 = itens.get("ebitda")

    alertas = []

    # --- Lucro Bruto ---
    if lb_p2 is not None:
        lb         = lb_p2
        lb_origem  = "p2"
        lb_formula = "valor do p2"
    elif rl is not None and cmv is not None:
        lb         = _round(rl - abs(cmv), 2)
        lb_origem  = "calculado"
        lb_formula = "Receita Líquida − |CMV|"
    else:
        lb         = None
        lb_origem  = "indisponivel"
        lb_formula = None
        alertas.append("Lucro Bruto não disponível: faltam Receita Líquida e/ou CMV no p2")

    # --- EBIT ---
    if ebit_p2 is not None:
        ebit         = ebit_p2
        ebit_origem  = "p2"
        ebit_formula = "valor do p2"
    elif lb is not None and desp_op is not None:
        ebit         = _round(lb - abs(desp_op) + out_rec, 2)
        ebit_origem  = "calculado"
        ebit_formula = "Lucro Bruto − |Despesas Operacionais| + Outras Receitas Operacionais"
    else:
        # Fallback: derivar EBIT pelo resultado, quando despesas_operacionais vem null.
        # EBIT = LAIR − Resultado Financeiro;  LAIR = Lucro Líquido + |IR/CSLL| (se LAIR null)
        # Validado contra DREs auditadas: bate exatamente com o EBIT explícito.
        ll       = itens.get("lucro_liquido")
        ir_csll  = itens.get("ir_csll")
        lair_p2  = itens.get("lair")
        res_fin  = itens.get("resultado_financeiro")

        lair_calc = None
        if lair_p2 is not None:
            lair_calc = lair_p2
        elif ll is not None:
            lair_calc = ll + (abs(ir_csll) if ir_csll is not None else 0)

        if lair_calc is not None and res_fin is not None:
            ebit         = _round(lair_calc - res_fin, 2)
            ebit_origem  = "calculado_via_resultado"
            ebit_formula = "LAIR − Resultado Financeiro (LAIR = Lucro Líquido + |IR/CSLL|) — fallback p/ despesas_operacionais null"
            alertas.append("EBIT derivado via LAIR − Resultado Financeiro (despesas_operacionais ausente no p2)")
        else:
            ebit         = None
            ebit_origem  = "indisponivel"
            ebit_formula = None
            alertas.append("EBIT não disponível: faltam Despesas Operacionais e também Lucro Líquido/Resultado Financeiro para fallback")

    # --- EBITDA ---
    if ebitda_p2 is not None:
        ebitda         = ebitda_p2
        ebitda_origem  = "p2"
        ebitda_formula = "valor do p2"
    elif ebit is not None and deprec is not None:
        ebitda         = _round(ebit + abs(deprec), 2)
        ebitda_origem  = "calculado"
        ebitda_formula = "EBIT + |Depreciação|"
    elif ebit is not None and deprec is None:
        ebitda         = ebit
        ebitda_origem  = "calculado_sem_depreciacao"
        ebitda_formula = "EBIT (depreciação indisponível — EBITDA subestimado)"
        alertas.append("EBITDA ≈ EBIT: depreciação não disponível no p2, EBITDA pode estar subestimado")
    else:
        ebitda         = None
        ebitda_origem  = "indisponivel"
        ebitda_formula = None

    return {
        "lucro_bruto":        lb,
        "lucro_bruto_origem": lb_origem,
        "lucro_bruto_formula": lb_formula,
        "ebit":               ebit,
        "ebit_origem":        ebit_origem,
        "ebit_formula":       ebit_formula,
        "ebitda":             ebitda,
        "ebitda_origem":      ebitda_origem,
        "ebitda_formula":     ebitda_formula,
        "alertas":            alertas,
    }


def calcular_margens(dre: list[dict]) -> list[dict]:
    """
    Margens por período disponível na DRE.
    Denominador: Receita Líquida (após deduções e abatimentos).

    Fórmulas:
      margem_bruta    = Lucro Bruto / Receita Líquida
      margem_ebitda   = EBITDA / Receita Líquida
      margem_ebit     = EBIT / Receita Líquida
      margem_liquida  = Lucro Líquido / Receita Líquida

    Para Lucro Bruto, EBIT e EBITDA, aplica hierarquia de fallback via
    _calcular_lucro_bruto_ebit_ebitda quando vêm null no p2.
    """
    resultado = []
    for periodo in dre:
        itens    = periodo.get("itens", {})
        inicio   = periodo.get("data_inicio", "")
        fim      = periodo.get("data_fim", "")
        label    = f"{inicio} a {fim}"

        rl  = itens.get("receita_liquida")
        ll  = itens.get("lucro_liquido")

        # Aplica hierarquia de cálculo para Lucro Bruto, EBIT e EBITDA
        calc = _calcular_lucro_bruto_ebit_ebitda(itens)

        resultado.append({
            "periodo":        label,
            "margem_bruta":   _div(calc["lucro_bruto"], rl),
            "margem_ebitda":  _div(calc["ebitda"],      rl),
            "margem_ebit":    _div(calc["ebit"],        rl),
            "margem_liquida": _div(ll,                  rl),
            # Valores absolutos calculados (úteis para downstream)
            "lucro_bruto":          calc["lucro_bruto"],
            "lucro_bruto_origem":   calc["lucro_bruto_origem"],
            "ebit":                 calc["ebit"],
            "ebit_origem":          calc["ebit_origem"],
            "ebitda":               calc["ebitda"],
            "ebitda_origem":        calc["ebitda_origem"],
            "alertas_calculo":      calc["alertas"],
            # fórmulas
            "margem_bruta_formula":   "Lucro Bruto / Receita Líquida",
            "margem_ebitda_formula":  "EBITDA / Receita Líquida",
            "margem_ebit_formula":    "EBIT / Receita Líquida",
            "margem_liquida_formula": "Lucro Líquido / Receita Líquida",
            "lucro_bruto_formula":    calc["lucro_bruto_formula"],
            "ebit_formula":           calc["ebit_formula"],
            "ebitda_formula":         calc["ebitda_formula"],
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

    # Inferir year e month do campo `period` (formato "YYYY-MM") quando ausentes
    # ou null. O ano extraído é dinâmico — vem do próprio valor do period de cada item.
    # E2 às vezes preenche apenas o period como string e deixa year/month null.
    for r in faturamento_mensal:
        if not r.get("year") and r.get("period"):
            try:
                r["year"] = int(str(r["period"]).split("-")[0])
            except (ValueError, IndexError):
                pass  # period não é parseable — segue sem year
        if not r.get("month") and r.get("period"):
            try:
                r["month"] = int(str(r["period"]).split("-")[1])
            except (ValueError, IndexError):
                pass

    anos = sorted({r["year"] for r in faturamento_mensal if r.get("year")})

    # Proteção: se nem o year direto nem o period eram extraíveis, anos fica vazio.
    # Não há como construir período da série — retornar dict vazio.
    # Itens com year=null devem idealmente ser filtrados pelo PROMPT_01 antes de chegar aqui.
    if not anos:
        return {}

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

    # EBIT — aplica hierarquia de cálculo (igual margens) quando vier null no p2
    calc = _calcular_lucro_bruto_ebit_ebitda(itens)
    ebit        = calc["ebit"]
    ebit_origem = calc["ebit_origem"]
    ebit_formula_calc = calc["ebit_formula"]

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
        # rastreabilidade
        "ebit_anual_usado":     ebit_anual,
        "ebit_origem":          ebit_origem,
        "ebit_formula_calculo": ebit_formula_calc,
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
        "credito_utilizado_limite":      _div(carteira, (carteira + limite) if carteira is not None and limite is not None else None),
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
        "credito_utilizado_limite_formula": "Carteira Ativa / (Carteira Ativa + Limite de Crédito Total)",
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

        # EBITDA — aplica hierarquia de cálculo (igual margens/rentabilidade)
        calc = _calcular_lucro_bruto_ebit_ebitda(itens)
        ebt = calc["ebitda"]
        ebt_origem = calc["ebitda_origem"]

        if rb is not None:
            receita_val = _round(rb * fator, 0)
        if ebt is not None:
            ebitda_val = _round(ebt * fator, 0)

        if fator != 1.0:
            sufixo = f"anualizado (×{fator:.1f}) — apenas matriz"
            receita_obs = f"Receita Bruta {ini[:7]} a {fim[:7]} {sufixo}."
            ebitda_obs  = f"EBITDA {ini[:7]} a {fim[:7]} {sufixo} (origem: {ebt_origem})."
        elif ebt is not None:
            ebitda_obs = f"EBITDA {ini[:7]} a {fim[:7]} (origem: {ebt_origem})."

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
# 17. PRODUTOS DE CRÉDITO — Cenários de limite por rating
# ---------------------------------------------------------------------------

# Tabela n_rating Política CG Cartão v03
_N_RATING = {
    "AA": 2.0,
    "A":  1.5,
    "B":  1.5,
    "C":  1.0,
    "D":  0.5,
    "E":  0.5,
    # F, G, H = recusa
}

# Percentuais CG Clean (% Receita Bruta anualizada por rating) — Produto 1
_PCT_CG_CLEAN = {"AA": 0.10, "A": 0.10, "B": 0.10, "C": 0.07, "D": 0.05, "E": 0.05}

# Percentuais Antecipação Risco Sacado / Cedente (% EBITDA anual) — Produtos 3 e 4
_PCT_ANTECIPACAO = {"AA": 0.70, "A": 0.70, "B": 0.70, "C": 0.50, "D": 0.30, "E": 0.30}

# Percentuais Não Performado (% EBITDA anual) — Produto 5 (mais conservador)
_PCT_NAO_PERFORMADO = {"AA": 0.40, "A": 0.40, "B": 0.40, "C": 0.30, "D": 0.20, "E": 0.20}

# Percentuais Convênio Risco Sacado (% Capacidade Pgto Núclea MIN/12) — Produto 7
_PCT_CONVENIO = {"AA": 0.40, "A": 0.40, "B": 0.40, "C": 0.30, "D": 0.20, "E": 0.20}

# Percentuais CG Cessão Fid Duplicatas (% giro mensal duplicatas)
_PCT_DUPLICATAS = {"AA": 0.50, "A": 0.50, "B": 0.50, "C": 0.30, "D": 0.20, "E": 0.20}

# Teto Dívida/EBITDA por rating — usado em L2 do Produto 1 (CG Clean)
# Padrão de mercado (Bacen-aligned)
_TETO_DIVIDA_EBITDA = {"AA": 4.5, "A": 4.0, "B": 3.5, "C": 3.0, "D": 2.5, "E": 2.0}

# % DSCR (cobertura mensal) — usado em L3 do Produto 1 (CG Clean)
# Limita a parcela mensal a esse % do EBITDA mensal
_PCT_DSCR = {"AA": 0.30, "A": 0.30, "B": 0.30, "C": 0.25, "D": 0.20, "E": 0.20}

# Cap operacional Produto 2 (CG Cessão Fid. Cartão)
_CAP_CARTAO = 15_000_000.0

# Teto operacional n_efetivo
_TETO_N_EFETIVO = 3.0


def _ajuste_idade(anos: float | int | None) -> float:
    """Curva Sebrae para comércio. Política v03."""
    if anos is None:
        return 1.00  # default conservador
    if anos < 2:
        return 0.70  # política não cobre <2 anos; aplica menor multiplicador
    if anos <= 3:
        return 0.70
    if anos <= 5:
        return 0.90
    return 1.00


def _ajuste_tendencia(yoy: float | None) -> tuple[float, str]:
    """
    Política v03. yoy é decimal (ex: 0.12 = +12%).
    Retorna (multiplicador, faixa_textual).
    """
    if yoy is None:
        return 1.00, "Tendência não calculável — assumido estável"
    if yoy < -0.25:
        return 0.0, f"Queda forte ({yoy*100:.1f}%) — RECUSA pela política"
    if yoy < -0.15:
        return 0.60, f"Queda moderada ({yoy*100:.1f}%)"
    if yoy < -0.05:
        return 0.85, f"Queda leve ({yoy*100:.1f}%)"
    return 1.00, f"Tendência estável/positiva ({yoy*100:.1f}%)"


def _cenarios_n_efetivo(ajuste_idade: float, ajuste_tendencia: float) -> dict[str, float]:
    """Calcula n_efetivo para cada rating, respeitando teto operacional."""
    return {
        rating: min(n * ajuste_idade * ajuste_tendencia, _TETO_N_EFETIVO)
        for rating, n in _N_RATING.items()
    }


def calcular_produtos_credito(
    p1: dict | None,
    p2: dict,
    p3_parcial: dict,
) -> dict:
    """
    Gera cenários de limite por rating para cada um dos 7 produtos da política Izi Cash.

    Inputs:
      p1: dict_p1 (pra anos_operacao). Pode ser None — fallback usa dados_cadastrais ou default.
      p2: dict_p2 completo.
      p3_parcial: dict_p3 sendo construído (precisa de receita, faturamento, capital_giro, transacional_nuclea).

    Output: dict com fmm_cartao, capacidade_pgto_mensal, giro_duplicatas_mensal, ajustes_comuns
            e cenarios_por_produto[produto][rating] = {n_rating, limite_bruto, limite_final, parcela_mensal}.
    """

    # ---- 1. Inputs de fontes ----
    bureaux  = p2.get("bureaux", {}) or {}
    nuclea   = bureaux.get("nuclea") or {}
    cerc_raw = _get(p2, "commercial_inputs", "cerc")
    balanco  = p2.get("balanco", [])

    # Anos de operação (p1 preferencial)
    anos_op = None
    if p1:
        anos_op = p1.get("anos_operacao")
    if anos_op is None:
        # fallback simples: ignora
        anos_op = None

    # Tendência YoY entre os 2 últimos anos completos do faturamento
    yoy = None
    var_anual = _get(p3_parcial, "faturamento", "variacao_anual", default={}) or {}
    # pega a última variação entre dois anos completos (chave do tipo "variacao_YYYY_YYYY")
    chaves_var = sorted([k for k in var_anual if k.startswith("variacao_")])
    if chaves_var:
        # ignora a última se envolver ano parcial (heurística simples)
        # Por segurança, pega penúltima quando existir
        if len(chaves_var) >= 2:
            yoy = var_anual.get(chaves_var[-2])
        else:
            yoy = var_anual.get(chaves_var[-1])

    ajuste_id  = _ajuste_idade(anos_op)
    ajuste_ten, faixa_ten = _ajuste_tendencia(yoy)
    n_efetivos = _cenarios_n_efetivo(ajuste_id, ajuste_ten)

    # ---- 2. FMM Cartão (Produto 2) ----
    # Hierarquia: CERC > Núclea total transacional > null
    fmm_cartao  = None
    fmm_fonte   = None
    fmm_alertas = []

    if cerc_raw:
        # Soma agenda dos últimos 12 meses
        historico = []
        for item in cerc_raw.get("raw_items", []):
            historico.extend(item.get("historico_agenda", []))
        vals = [h["valor_liquidado"] for h in historico if h.get("valor_liquidado") is not None]
        if vals:
            fmm_cartao = _round(sum(vals) / len(vals), 2)
            fmm_fonte  = "CERC (agenda 12 meses, fonte primária)"

    if fmm_cartao is None:
        fat_trans = nuclea.get("faturamento_transacional")
        if fat_trans is not None:
            fmm_cartao = _round(fat_trans / 12, 2)
            fmm_fonte  = "Núclea faturamento_transacional ÷ 12 (fonte secundária oficial)"
            fmm_alertas.append("FMM via Núclea — agenda CERC requerida para confirmação definitiva")

    if fmm_cartao is None:
        fmm_alertas.append("Nem CERC nem Núclea disponíveis — Produto 2 deve ser APROVADO COM CONDIÇÕES exigindo CERC antes de calcular")

    # ---- 3. EBITDA anualizado e mensal ----
    # Base primária dos produtos 1 (L2, L3), 3, 4, 5
    ebitda_anual = _anualizar_campo(p2.get("dre", []), "ebitda")
    ebitda_mensal = _round(ebitda_anual / 12, 2) if ebitda_anual else None
    ebitda_alertas = []

    if ebitda_anual is None:
        ebitda_alertas.append("EBITDA não disponível mesmo após hierarquia de cálculo — produtos baseados em EBITDA serão null")
    elif ebitda_anual <= 0:
        ebitda_alertas.append(f"EBITDA negativo ou zero (R$ {ebitda_anual/1e6:.2f}M anual) — produtos baseados em EBITDA serão null")
        ebitda_anual = None
        ebitda_mensal = None

    # ---- 4. Capacidade Núclea Opção D — MIN(faturamento_transacional, valores_pagos) ÷ 12 ----
    # Usada como base primária do Produto 7 (Convênio Risco Sacado)
    nuclea_cap_min = None
    nuclea_cap_fonte = None
    nuclea_alertas = []

    fat_trans = nuclea.get("faturamento_transacional")
    val_pagos = nuclea.get("valores_pagos")

    if fat_trans is not None and val_pagos is not None:
        nuclea_cap_min = _round(min(fat_trans, val_pagos) / 12, 2)
        nuclea_cap_fonte = (
            f"Núclea MIN(faturamento_transacional R$ {fat_trans/1e6:.1f}M, "
            f"valores_pagos R$ {val_pagos/1e6:.1f}M) ÷ 12 = "
            f"R$ {nuclea_cap_min/1e6:.1f}M/mês"
        )
        if val_pagos > fat_trans * 1.3:
            nuclea_alertas.append(
                f"Pagamentos Núclea ({val_pagos/1e6:.1f}M) excedem recebimentos ({fat_trans/1e6:.1f}M) "
                "— empresa pode operar fora do circuito Núclea ou ter compromissos elevados"
            )
    elif fat_trans is not None:
        nuclea_cap_min = _round(fat_trans / 12, 2)
        nuclea_cap_fonte = "Núclea faturamento_transacional ÷ 12 (valores_pagos null)"
    elif val_pagos is not None:
        nuclea_cap_min = _round(val_pagos / 12, 2)
        nuclea_cap_fonte = "Núclea valores_pagos ÷ 12 (faturamento_transacional null)"

    # ---- 5. Capacidade de pagamento mensal (hierarquia geral) ----
    # 1º EBITDA mensal | 2º Núclea MIN/12 | 3º Receita Bruta × margem média setorial ÷ 12
    capacidade = None
    cap_fonte  = None
    cap_alertas = []

    if ebitda_mensal is not None:
        capacidade = ebitda_mensal
        cap_fonte = "EBITDA anualizado ÷ 12 (geração operacional)"
    elif nuclea_cap_min is not None:
        capacidade = nuclea_cap_min
        cap_fonte = f"Fallback Núclea: {nuclea_cap_fonte}"
        cap_alertas.append("Capacidade pgto via Núclea — EBITDA não disponível")
    else:
        # Fallback final: Receita Bruta × 5% (proxy margem EBITDA setorial conservadora) ÷ 12
        rb_anual_tmp = _anualizar_campo(p2.get("dre", []), "receita_bruta")
        if rb_anual_tmp is not None:
            capacidade = _round(rb_anual_tmp * 0.05 / 12, 2)
            cap_fonte = "Receita Bruta anual × 5% ÷ 12 (fallback — sem EBITDA nem Núclea)"
            cap_alertas.append("Capacidade pgto estimada via RB × 5% — preferível ter EBITDA ou Núclea")

    if capacidade is None:
        cap_alertas.append("Sem dados para estimar capacidade pgto mensal — produtos baseados em capacidade serão null")

    # ---- 6. FMM Cartão (Produto 2) ----
    # Hierarquia: CERC > Núclea faturamento_transacional ÷ 12 > null
    fmm_cartao  = None
    fmm_fonte   = None
    fmm_alertas = []

    if cerc_raw:
        historico = []
        for item in cerc_raw.get("raw_items", []):
            historico.extend(item.get("historico_agenda", []))
        vals = [h["valor_liquidado"] for h in historico if h.get("valor_liquidado") is not None]
        if vals:
            fmm_cartao = _round(sum(vals) / len(vals), 2)
            fmm_fonte  = "CERC (agenda 12 meses, fonte primária)"

    if fmm_cartao is None and fat_trans is not None:
        fmm_cartao = _round(fat_trans / 12, 2)
        fmm_fonte  = "Núclea faturamento_transacional ÷ 12 (fonte secundária oficial)"
        fmm_alertas.append("FMM via Núclea — agenda CERC requerida para confirmação definitiva")

    if fmm_cartao is None:
        fmm_alertas.append("Nem CERC nem Núclea disponíveis — Produto 2 deve ser APROVADO COM CONDIÇÕES exigindo CERC antes de calcular")

    # ---- 7. Giro mensal de duplicatas (Produto 6) ----
    giro_dup = None
    giro_fonte = None
    giro_alertas = []

    _, bal_atual = _balanco_mais_recente(balanco)
    if bal_atual:
        cr = bal_atual.get("creditos")
        if cr is not None:
            giro_dup = _round(cr, 2)
            giro_fonte = "Contas a Receber do balanço (proxy de giro mensal — PMR ~30 dias)"
        else:
            giro_alertas.append("Contas a Receber não disponível no balanço")

    if giro_dup is None:
        giro_alertas.append("Sem dados de duplicatas — Produto 6 depende de relatório específico de carteira")

    # ---- 8. Dívida bruta (do SCR) — usada em L2 do Produto 1 ----
    divida_bruta = None
    scr_bacen = bureaux.get("scr_bacen") or {}
    if scr_bacen:
        divida_bruta = scr_bacen.get("carteira_ativa")

    # ---- 9. Helpers de cenários ----

    def cenarios_simples(
        base: float | None,
        pct_por_rating: dict[str, float],
        cap: float | None = None,
        prazo_meses: int = 12,
    ) -> dict:
        """Gera cenários para produtos cuja fórmula é (% × base × n_efetivo)."""
        if base is None:
            return {r: None for r in _N_RATING}

        result = {}
        for rating in _N_RATING:
            n_ef = n_efetivos[rating]
            pct  = pct_por_rating[rating]
            limite_bruto = _round(pct * base * n_ef, 2)
            limite_final = _round(min(limite_bruto, cap), 2) if cap else limite_bruto
            parcela = _round(limite_final / prazo_meses, 2) if prazo_meses else None
            pct_base = _div(parcela, base, 4) if parcela and base else None
            result[rating] = {
                "n_rating":          _N_RATING[rating],
                "n_efetivo":         _round(n_ef, 4),
                "pct_base":          pct,
                "limite_bruto":      limite_bruto,
                "limite_final":      limite_final,
                "parcela_mensal":    parcela,
                "pct_parcela_base":  pct_base,
                "cap_aplicado":      bool(cap and limite_bruto > cap),
            }
        return result

    def cenarios_cg_cartao(fmm: float | None) -> dict:
        """Produto 2: limite = n_efetivo × FMM. Cap R$ 15M. Restrição: parcela ≤ 30% FMM."""
        if fmm is None:
            return {r: None for r in _N_RATING}

        result = {}
        for rating in _N_RATING:
            n_ef = n_efetivos[rating]
            limite_bruto = _round(n_ef * fmm, 2)
            limite_final = _round(min(limite_bruto, _CAP_CARTAO), 2)
            parcela = _round(limite_final / 12, 2)
            pct_fmm = _div(parcela, fmm, 4)
            result[rating] = {
                "n_rating":          _N_RATING[rating],
                "n_efetivo":         _round(n_ef, 4),
                "limite_bruto":      limite_bruto,
                "limite_final":      limite_final,
                "parcela_mensal":    parcela,
                "pct_parcela_fmm":   pct_fmm,
                "dscr":              _round(1 / pct_fmm, 4) if pct_fmm and pct_fmm > 0 else None,
                "cap_aplicado":      limite_bruto > _CAP_CARTAO,
                "restricao_30pct":   "OK" if (pct_fmm is None or pct_fmm <= 0.30) else "VIOLA",
            }
        return result

    def cenarios_cg_clean(rb_anual: float | None, ebitda_anual_val: float | None,
                         ebitda_mensal_val: float | None, divida: float | None) -> dict:
        """
        Produto 1 — CG Clean com 3 restrições (Abordagem C combinada):
          L1 = % RB × n_efetivo                      → restrição comercial
          L2 = MAX(0, Teto × EBITDA - Dívida)        → cap de alavancagem
          L3 = % DSCR × EBITDA_mensal × 24m          → restrição de cobertura

          Limite_Final = MIN(L1, L2, L3)

        Se EBITDA é null/negativo → todos cenários null (CG Clean não pode ser oferecido).
        """
        if rb_anual is None or ebitda_anual_val is None or ebitda_mensal_val is None:
            return {r: None for r in _N_RATING}

        result = {}
        for rating in _N_RATING:
            n_ef = n_efetivos[rating]

            # L1 — restrição comercial
            l1 = _round(_PCT_CG_CLEAN[rating] * rb_anual * n_ef, 2)

            # L2 — cap de alavancagem (capacidade adicional além da dívida atual)
            teto = _TETO_DIVIDA_EBITDA[rating]
            divida_max_permitida = teto * ebitda_anual_val
            l2_raw = divida_max_permitida - (divida or 0)
            l2 = _round(max(0, l2_raw), 2)

            # L3 — cobertura DSCR (parcela mensal sobre EBITDA mensal)
            pct_dscr = _PCT_DSCR[rating]
            parcela_max = pct_dscr * ebitda_mensal_val
            l3 = _round(parcela_max * 24, 2)  # 24 meses de prazo

            # Limite final = MIN dos 3
            limite_final = _round(min(l1, l2, l3), 2)
            restricao_ativa = (
                "L1" if l1 <= l2 and l1 <= l3 else
                "L2" if l2 <= l1 and l2 <= l3 else
                "L3"
            )

            parcela = _round(limite_final / 24, 2) if limite_final > 0 else 0
            pct_parcela_ebitda_mensal = _div(parcela, ebitda_mensal_val, 4)

            result[rating] = {
                "n_rating":          _N_RATING[rating],
                "n_efetivo":         _round(n_ef, 4),
                "L1_comercial":      l1,
                "L1_formula":        f"{_PCT_CG_CLEAN[rating]*100:.0f}% × RB × n_efetivo",
                "L2_alavancagem":    l2,
                "L2_formula":        f"MAX(0, {teto}x × EBITDA - Dívida_Bruta)",
                "L2_teto_divida":    teto,
                "L3_dscr":           l3,
                "L3_formula":        f"{pct_dscr*100:.0f}% × EBITDA_mensal × 24m",
                "limite_bruto":      l1,            # mantém L1 como "bruto" pra comparação
                "limite_final":      limite_final,
                "restricao_ativa":   restricao_ativa,
                "parcela_mensal":    parcela,
                "pct_parcela_ebitda_mensal": pct_parcela_ebitda_mensal,
                "cap_aplicado":      restricao_ativa in ("L2", "L3"),
            }
        return result

    def cenarios_com_dscr(
        base: float | None,
        ebitda_mensal_val: float | None,
        pct_por_rating: dict[str, float],
        prazo_meses: int,
    ) -> dict:
        """
        Produtos 3, 4, 5, 7 — fórmula L1 ∩ L2:
          L1 = % × base × n_efetivo                      → restrição comercial
          L2 = % DSCR × EBITDA_mensal × prazo            → cobertura

        Para Produto 7 (Convênio), base = Núclea MIN; EBITDA é usado apenas em L2.
        Se EBITDA mensal é null, L2 não aplica e Limite_Final = L1.
        """
        if base is None:
            return {r: None for r in _N_RATING}

        result = {}
        for rating in _N_RATING:
            n_ef = n_efetivos[rating]
            pct  = pct_por_rating[rating]

            # L1 — restrição comercial
            l1 = _round(pct * base * n_ef, 2)

            # L2 — cobertura DSCR (só se EBITDA disponível)
            if ebitda_mensal_val is not None:
                pct_dscr = _PCT_DSCR[rating]
                l2 = _round(pct_dscr * ebitda_mensal_val * prazo_meses, 2)
                limite_final = _round(min(l1, l2), 2)
                restricao_ativa = "L1" if l1 <= l2 else "L2"
            else:
                l2 = None
                limite_final = l1
                restricao_ativa = "L1 (L2 não calculável — sem EBITDA)"

            parcela = _round(limite_final / prazo_meses, 2) if limite_final > 0 else 0

            result[rating] = {
                "n_rating":          _N_RATING[rating],
                "n_efetivo":         _round(n_ef, 4),
                "pct_base":          pct,
                "L1_comercial":      l1,
                "L1_formula":        f"{pct*100:.0f}% × base × n_efetivo",
                "L2_dscr":           l2,
                "L2_formula":        f"{_PCT_DSCR[rating]*100:.0f}% × EBITDA_mensal × {prazo_meses}m" if l2 is not None else None,
                "limite_bruto":      l1,
                "limite_final":      limite_final,
                "restricao_ativa":   restricao_ativa,
                "parcela_mensal":    parcela,
                "cap_aplicado":      restricao_ativa.startswith("L2"),
            }
        return result

    # Receita Bruta anualizada (base do CG Clean L1)
    rb_anual = _anualizar_campo(p2.get("dre", []), "receita_bruta")

    cenarios = {
        # Produto 1 — CG Clean com L1+L2+L3
        "cg_clean":                   cenarios_cg_clean(rb_anual, ebitda_anual, ebitda_mensal, divida_bruta),

        # Produto 2 — CG Cartão (não muda)
        "cg_cessao_fid_cartao":       cenarios_cg_cartao(fmm_cartao),

        # Produtos 3, 4 — Antecipação Sacado/Cedente: base EBITDA anual, com cap DSCR
        "antecipacao_risco_sacado":   cenarios_com_dscr(ebitda_anual, ebitda_mensal, _PCT_ANTECIPACAO, prazo_meses=8),
        "antecipacao_risco_cedente":  cenarios_com_dscr(ebitda_anual, ebitda_mensal, _PCT_ANTECIPACAO, prazo_meses=8),

        # Produto 5 — Não Performado: base EBITDA anual, % menor, com cap DSCR (prazo 12m)
        "antecipacao_nao_performado": cenarios_com_dscr(ebitda_anual, ebitda_mensal, _PCT_NAO_PERFORMADO, prazo_meses=12),

        # Produto 6 — Duplicatas: base Contas a Receber (sem DSCR — recebível é a garantia)
        "cg_cessao_fid_duplicatas":   cenarios_simples(giro_dup,   _PCT_DUPLICATAS,  cap=None, prazo_meses=12),

        # Produto 7 — Convênio Sacado: base Núclea MIN, com cap DSCR
        "convenio_risco_sacado":      cenarios_com_dscr(nuclea_cap_min, ebitda_mensal, _PCT_CONVENIO, prazo_meses=8),
    }

    return {
        "fmm_cartao": {
            "valor":   fmm_cartao,
            "fonte":   fmm_fonte,
            "alertas": fmm_alertas,
        },
        "ebitda_anual": {
            "valor":   ebitda_anual,
            "fonte":   "DRE: período mais recente anualizado (com hierarquia EBIT + Depreciação)",
            "alertas": ebitda_alertas,
        },
        "ebitda_mensal": {
            "valor":   ebitda_mensal,
            "fonte":   "EBITDA anual ÷ 12",
        },
        "capacidade_pgto_mensal": {
            "valor":   capacidade,
            "fonte":   cap_fonte,
            "alertas": cap_alertas,
            "hierarquia_usada": [
                "1º EBITDA mensal (geração operacional)",
                "2º Núclea MIN(recebimentos, pagamentos)/12 (fallback)",
                "3º Receita Bruta × 5% / 12 (último fallback)",
            ],
        },
        "nuclea_capacidade_min": {
            "valor":   nuclea_cap_min,
            "fonte":   nuclea_cap_fonte,
            "alertas": nuclea_alertas,
        },
        "giro_duplicatas_mensal": {
            "valor":   giro_dup,
            "fonte":   giro_fonte,
            "alertas": giro_alertas,
        },
        "divida_bruta_scr": divida_bruta,
        "receita_bruta_anual": rb_anual,
        "ajustes_comuns": {
            "anos_operacao":         anos_op,
            "ajuste_idade":          ajuste_id,
            "tendencia_yoy_recente": yoy,
            "ajuste_tendencia":      ajuste_ten,
            "faixa_tendencia":       faixa_ten,
            "teto_n_efetivo":        _TETO_N_EFETIVO,
        },
        "cap_operacional": {
            "cg_cessao_fid_cartao": _CAP_CARTAO,
            "demais_produtos":      None,
        },
        "tabela_n_rating":            _N_RATING,
        "tabela_teto_divida_ebitda":  _TETO_DIVIDA_EBITDA,
        "tabela_pct_dscr":            _PCT_DSCR,
        "cenarios_por_produto":       cenarios,
        "nota_uso": (
            "O E4 escolhe o rating Izi (AA-E) baseado em análise qualitativa "
            "(liquidez, alavancagem, scores, prejuízo). Após escolher rating, "
            "lê limite_final do cenário correspondente. Sem haircuts adicionais. "
            "Produto 1 (CG Clean) aplica MIN(L1=%RB, L2=teto alavancagem, L3=DSCR). "
            "Produtos 3, 4, 5, 7 aplicam MIN(L1, L2 DSCR)."
        ),
    }


# ---------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ---------------------------------------------------------------------------

def calcular(p2: dict, p1: dict | None = None) -> dict:
    """
    Recebe dict_p2 (e opcionalmente dict_p1) e retorna dict_p3 completo.

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
      11. cartao_recebiveis (indicadores brutos de CERC/Núclea)
      12. eficiencia_operacional
      13. transacional_nuclea
      14. restritivos_bureau
      15. restritivos_relativos
      16. grupo_economico
      17. produtos_credito (cenários de limite por rating, com cap R$ 15M no Produto 2)
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

    # 18. Produtos de crédito (cenários de limite por rating)
    # Construído por último porque depende de outros indicadores já calculados
    p3_parcial = {
        "receita":              receita,
        "faturamento":          faturamento,
        "capital_giro":         capital_giro,
        "transacional_nuclea":  transacional,
    }
    produtos_credito = calcular_produtos_credito(p1, p2, p3_parcial)

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
        "produtos_credito":       produtos_credito,
    }
