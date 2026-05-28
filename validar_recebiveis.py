"""validar_recebiveis.py — validação das fontes de recebíveis (padrão validar_p2()).
Cada função retorna lista de alertas dict {nivel,campo,impacto,sugestao}. Vazia = ok.
"""
from __future__ import annotations
from parsers_recebiveis import (parse_radar, parse_agenda_ap005, parse_raiox_html,
                                RADAR_CATEGORIAS, RADAR_FAIXAS, ARRANJOS_CONHECIDOS)

def _a(L, nivel, campo, impacto, sugestao):
    L.append({"nivel": nivel, "campo": campo, "impacto": impacto, "sugestao": sugestao})

def validar_radar(radar: dict) -> list[dict]:
    L = []
    if not radar.get("raw_items"):
        _a(L, "critico", "radar_recebiveis", "Sem aging de recebíveis — sizing por faixa indisponível",
           "Verificar se o CSV do Radar foi carregado")
        return L
    for i, it in enumerate(radar["raw_items"]):
        if not (it.get("estabelecimento_comercial") or "").isdigit():
            _a(L, "atencao", f"radar[{i}].estabelecimento_comercial", "EC inválido", "Conferir coluna documento_estabelecimento_comercial")
        faltam = [c for c in RADAR_CATEGORIAS if set(it["aging"][c]) != {f"d{f}" for f in RADAR_FAIXAS}]
        if faltam:
            _a(L, "critico", f"radar[{i}].aging", f"Faixas de aging incompletas em {faltam}", "Conferir as 20 colunas valor_*")
    t = radar["totais"]
    if t["constituido_total"] and t["comprometido_total"] > t["constituido_total"] * 1.0001:
        _a(L, "atencao", "radar.totais", "comprometido > constituído (índice >100%)",
           "Plausível se agenda super-comprometida; confirmar definição das categorias")
    return L

def validar_agenda_ap005(ag: dict) -> list[dict]:
    L = []
    if not ag.get("n_urs"):
        _a(L, "critico", "agenda_ap005", "Agenda não parseada", "Verificar CSVs CERC-*_ret_agenda_nova.csv")
        return L
    import datetime as dt
    per = ag.get("periodo_liquidacao", {})
    for chave in ("min", "max"):
        try: dt.date.fromisoformat(per.get(chave) or "")
        except (ValueError, TypeError):
            _a(L, "critico", f"agenda.periodo_liquidacao.{chave}", "Data fora de ISO — quebra ordenação de vencimentos",
               f"Valor: {per.get(chave)!r}")
    desconhecidos = [a for a in ag.get("distribuicao_por_arranjo", {}) if a not in ARRANJOS_CONHECIDOS]
    if desconhecidos:
        _a(L, "atencao", "agenda.distribuicao_por_arranjo", f"Arranjos desconhecidos: {desconhecidos}",
           "Conferir contra a tabela de dicionário de domínio de arranjos do manual CERC")
    if ag.get("n_pagamentos", 0) == 0:
        _a(L, "critico", "agenda.pagamentos", "Nenhum pagamento extraído do campo 12 (aninhado)",
           "Conferir split por '|' e ';' do campo 12")
    if ag.get("schema_status") != "CONFIRMADO_AP005":
        _a(L, "atencao", "agenda.schema_status", f"Status inesperado: {ag.get('schema_status')!r}",
           "Esperado CONFIRMADO_AP005 após mapeamento oficial")
    if "total" not in (ag.get("comprometido", {}) or {}):
        _a(L, "critico", "agenda.comprometido", "Comprometido não computado",
           "Somar pg_valor_constituido_efeito_ur (12.15) por tipo de efeito (12.13)")
    return L


def validar_agenda_completa(df_ur, df_pg) -> list[dict]:
    """Valida a EXTRAÇÃO COMPLETA (df_ur + df_pg) — contagens e integridade UR↔pagamentos."""
    L = []
    if df_ur is None or len(df_ur) == 0:
        _a(L, "critico", "agenda_completa.df_ur", "Extração completa vazia", "Verificar arquivos AP005")
        return L
    # toda UR deve ter as 16 colunas + ur_id; qtd_pagamentos deve bater com o explodido
    soma_qtd = int(df_ur["qtd_pagamentos"].sum()) if "qtd_pagamentos" in df_ur else -1
    if soma_qtd != len(df_pg):
        _a(L, "critico", "agenda_completa.integridade",
           f"Soma de qtd_pagamentos ({soma_qtd}) != linhas explodidas em df_pg ({len(df_pg)})",
           "Conferir parse_lista_pagamentos / explosão")
    # datas ISO no nível UR
    import datetime as dt
    ruins = 0
    for d in df_ur["data_liquidacao"].tolist():
        if d is None:
            continue
        try: dt.date.fromisoformat(d)
        except (ValueError, TypeError):
            ruins += 1
    if ruins:
        _a(L, "atencao", "agenda_completa.data_liquidacao", f"{ruins} datas não-ISO no nível UR",
           "Conferir parse_data (formato AAAA-MM-DD)")
    return L

def validar_raiox(x: dict) -> list[dict]:
    L = []
    obrig = ["faturamento_estimado", "agenda_mensal_media", "volume_antecipacao"]
    for k in obrig:
        if x.get(k) is None:
            _a(L, "critico", f"raio_x.{k}", "KPI central do Raio-X ausente", "Conferir DOM do HTML / re-extrair")
    if not x.get("market_share_adquirente"):
        _a(L, "atencao", "raio_x.market_share_adquirente", "Sem instituições de pagamento", "Conferir bloco do HTML")
    hist = x.get("historico_agenda_mensal")
    if not hist:
        _a(L, "atencao", "raio_x.historico_agenda_mensal",
           "Gráfico de histórico de agenda não reconstruído",
           "Conferir barras .recharts-bar no HTML; se ausente, extrair por VISÃO no raio_x_pt_2.png")
    else:
        if len(hist) != 12:
            _a(L, "atencao", "raio_x.historico_agenda_mensal",
               f"Esperados 12 meses, reconstruídos {len(hist)}", "Conferir geometria das barras")
        # CROSS-CHECK determinístico: soma das séries mensais deve bater com os KPIs (aria-label)
        soma_ag = round(sum(r.get("agenda", 0) for r in hist), 2)
        soma_va = round(sum(r.get("volume_antecipado", 0) for r in hist), 2)
        tot = x.get("historico_agenda_total")
        vol = x.get("volume_antecipacao")
        if tot and abs(soma_ag - tot) > max(tot * 0.01, 1.0):
            _a(L, "critico", "raio_x.historico_agenda_mensal[agenda]",
               f"Soma mensal ({soma_ag}) diverge de historico_agenda_total ({tot}) >1%",
               "Conferir escala do eixo Y / mapeamento de séries no histograma")
        if vol and abs(soma_va - vol) > max(vol * 0.01, 1.0):
            _a(L, "critico", "raio_x.historico_agenda_mensal[volume_antecipado]",
               f"Soma mensal ({soma_va}) diverge de volume_antecipacao ({vol}) >1%",
               "Conferir escala do eixo Y / mapeamento de séries no histograma")
    return L
