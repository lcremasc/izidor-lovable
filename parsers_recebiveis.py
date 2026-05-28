"""parsers_recebiveis.py — parsers determinísticos das fontes de recebíveis de cartão.
Fonte A: Radar (CSV)  | Fonte B: Agenda CERC AP005 (CSV retorno) | Fonte C: Raio-X (HTML).
Todos os valores numéricos passam por normalizacao.normalizar_numero.
"""
from __future__ import annotations
import csv, re, glob, os
from normalizacao import normalizar_numero

# ─────────────────────────── FONTE A — RADAR ────────────────────────────────
RADAR_CATEGORIAS = ("livre", "pre", "comprometido", "constituido")
RADAR_FAIXAS     = ("0_30", "31_60", "61_90", "91_120", "120_mais")

def parse_radar(path: str) -> dict:
    """CSV do Radar → schema dedicado commercial_inputs.radar_recebiveis."""
    itens, tot = [], {c: 0.0 for c in RADAR_CATEGORIAS}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            aging = {}
            for cat in RADAR_CATEGORIAS:
                aging[cat] = {}
                for fx in RADAR_FAIXAS:
                    v = normalizar_numero(row.get(f"valor_{cat}_{fx}"), "us") or 0.0
                    aging[cat][f"d{fx}"] = v
                    tot[cat] += v
            itens.append({
                "estabelecimento_comercial": (row.get("documento_estabelecimento_comercial") or "").strip(),
                "credenciadora": {
                    "documento":   (row.get("documento_credenciadora_sub") or "").strip(),
                    "razao_social": (row.get("razao_social_credenciadora") or "").strip(),
                },
                "arranjo": (row.get("arranjo") or "").strip(),
                "aging": aging,
            })
    idx = (tot["comprometido"] / tot["constituido"]) if tot["constituido"] else None
    return {
        "source_format": "radar_cerc_aging_por_arranjo",
        "raw_items": itens,
        "totais": {**{f"{c}_total": round(tot[c], 2) for c in RADAR_CATEGORIAS},
                   "indice_comprometimento": (round(idx, 4) if idx is not None else None)},
    }

# ──────────────────────── FONTE B — AGENDA AP005 (delega ao leitor canônico) ─
# Parsing/semântica completos vivem em ler_agenda_completa.py (layout oficial CERC AP005,
# 16 campos da UR + 16 subcampos de pagamento). Aqui só expomos as funções de integração.
# Arranjos observados; dicionário oficial de domínio do manual não foi fornecido (D/H a confirmar).
ARRANJOS_CONHECIDOS = {"ACC","CBC","DCC","ECC","ECD","HCC","HCD","MCC","MCD","VCC","VCD"}

def extrair_agenda_completa(paths: list[str]):
    """Extração COMPLETA da agenda AP005: (df_ur 1 linha/UR, df_pg 1 linha/pagamento)."""
    from ler_agenda_completa import ler_agenda_multi
    return ler_agenda_multi(paths)

def parse_agenda_ap005(paths: list[str]) -> dict:
    """Digest agregado (compacto) da agenda AP005, computado a partir da extração COMPLETA.
    Para a leitura granular completa use extrair_agenda_completa()."""
    from ler_agenda_completa import processar_agenda
    digest, _df_ur, _df_pg = processar_agenda(paths)
    return digest

# ──────────────────────── FONTE C — RAIO-X (HTML) ────────────────────────────
# Ticks do eixo Y do histograma (CERC 2.0 / Recharts): y=360 → R$0 ; y=50 → R$600k.
_RAIOX_Y0, _RAIOX_V0 = 360.0, 0.0
_RAIOX_Y1, _RAIOX_V1 = 50.0, 600_000.0
_RAIOX_MESES = ["2025-05","2025-06","2025-07","2025-08","2025-09","2025-10",
                "2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"]
_RAIOX_FILL = {"#40A9F4": "agenda", "#56B5A4": "volume_antecipado"}

def _raiox_historico_mensal(soup) -> list[dict] | None:
    """Reconstrói o gráfico 'Histórico de agenda' (séries Agenda + Volume Antecipado)
    a partir da geometria SVG das barras Recharts. Determinístico (sem visão/OCR)."""
    series = soup.select(".recharts-bar")
    if not series:
        return None
    escala = (_RAIOX_V1 - _RAIOX_V0) / (_RAIOX_Y0 - _RAIOX_Y1)
    pormes: dict[str, dict] = {}
    for s in series:
        inner = s.find(["path", "rect"])
        nome = _RAIOX_FILL.get(inner.get("fill") if inner else None, (inner.get("fill") if inner else "serie"))
        barras = s.select(".recharts-bar-rectangle")
        def _x(b):
            p = b.find("path"); m = re.match(r"M\s*([\d.]+),", p.get("d") or "")
            return float(m.group(1)) if m else 0.0
        for i, b in enumerate(sorted(barras, key=_x)):
            if i >= len(_RAIOX_MESES):
                break
            d = b.find("path").get("d") or ""
            m = re.search(r"v\s*([\d.]+)", d)
            altura = float(m.group(1)) if m else 0.0
            pormes.setdefault(_RAIOX_MESES[i], {"mes": _RAIOX_MESES[i]})[nome] = round(altura * escala, 2)
    return [pormes[k] for k in _RAIOX_MESES if k in pormes] or None

def parse_raiox_html(path: str) -> dict:
    """Extrai KPIs, listas e o gráfico de histórico de agenda do dashboard CERC 2.0,
    direto do DOM (determinístico, sem OCR). Valores exatos vêm dos aria-labels quando
    disponíveis; o histograma é reconstruído da geometria SVG das barras."""
    from bs4 import BeautifulSoup
    html = open(path, encoding="utf-8", errors="replace").read()
    soup = BeautifulSoup(html, "html.parser")
    txt = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

    # 1) Valores EXATOS via aria-label (preferencial). Mapeia descrição → próximo valor R$.
    aria = [el.get("aria-label", "").replace("\xa0", " ").strip()
            for el in soup.find_all(attrs={"aria-label": True})]
    exatos: dict[str, float] = {}
    ultima_desc = ""
    CHAVE = {"estimativa de faturamento":            "faturamento_estimado",
             "média mensal das agendas":             "agenda_mensal_media",
             "soma dos últimos 12 meses de agendas": "historico_agenda_total",
             "soma dos últimos 12 meses de antecipa": "volume_antecipacao"}
    for a in aria:
        if re.fullmatch(r"R\$\s*[\d.,]+", a):
            for frag, campo in CHAVE.items():
                if frag in ultima_desc.lower():
                    exatos[campo] = normalizar_numero(a, "br")
                    ultima_desc = ""   # consome — só o 1º valor após a descrição mapeia
                    break
        else:
            ultima_desc = a

    # 2) KPIs por texto do DOM (fallback / campos sem aria de valor)
    def kpi(label):
        m = re.search(re.escape(label) + r"\s+(R\$\s*[\d.,]+\s*[KMB]{0,2}|[\d.,]+%)", txt)
        if not m:
            return None
        bruto = m.group(1)
        return normalizar_numero(bruto.rstrip("%"), "br") if bruto.endswith("%") else normalizar_numero(bruto, "br")

    def lista_share(secao, fim):
        bloco = txt[txt.find(secao):txt.find(fim)] if secao in txt else ""
        return [{"razao_social": n.strip(), "market_share": round(float(s) / 100, 4)}
                for n, s in re.findall(r"([A-ZÀ-Ú][A-ZÀ-Ú0-9 .,&()/-]{4,}?)\s+(\d+(?:\.\d+)?)%", bloco)]

    # NOTA: Raio-X (dashboard CERC 2.0) é fonte SEPARADA da Agenda AP005.
    return {
        "source_format": "raio_x_cerc_2_0_html",
        "faturamento_estimado":       exatos.get("faturamento_estimado",  kpi("Faturamento Estimado")),
        "faturamento_medio_diario":   kpi("Faturamento Médio Diário"),
        "agenda_mensal_media":        exatos.get("agenda_mensal_media",   kpi("Agenda Mensal Média")),
        "historico_agenda_total":     exatos.get("historico_agenda_total", kpi("Histórico de Agenda")),
        "volume_antecipacao":         exatos.get("volume_antecipacao",   kpi("Volume de Antecipação")),
        "nivel_comprometimento":      kpi("Nível de Comprometimento"),
        "potencial_chargeback":       kpi("Potencial de ChargeBack"),
        "market_share_adquirente":    lista_share("Instituições de Pagamento", "Financiadores"),
        "market_share_financiador":   lista_share("Financiadores", "Constatações") or lista_share("Financiadores", "28/05"),
        "historico_agenda_mensal":    _raiox_historico_mensal(soup),
    }
