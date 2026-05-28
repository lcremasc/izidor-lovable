"""
Leitor da AGENDA CERC completa (layout AP005).

Estrutura do arquivo:
  - Separador de campos: ';'
  - Aspas: '"'  (o campo 12 vem inteiro entre aspas)
  - Sem linha de cabeçalho (os nomes vêm do layout, campos 1 a 16)
  - Cada LINHA = uma UR (unidade de recebível), com os campos 1..16
  - O campo 12 ("Lista de informações de pagamento") é ANINHADO: contém
    vários sub-registros separados por '|', cada um com os campos 12.1..12.16
    separados por ';'.

Saídas:
  - df_ur          : 1 linha por UR (16 colunas). A coluna 12 vira a contagem
                     de pagamentos + a lista estruturada.
  - df_pagamentos  : formato longo, 1 linha por sub-registro de pagamento,
                     com 'ur_id' apontando para a UR de origem.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers de tipagem
# ---------------------------------------------------------------------------
def parse_documento(v: str) -> str | None:
    if not v:
        return None
    d = re.sub(r"\D", "", v)
    if not d:
        return None
    return d.zfill(11) if len(d) <= 11 else d.zfill(14)


def parse_decimal(v: str) -> Decimal | None:
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def parse_data(v: str) -> str | None:
    if not v or str(v).strip() == "":
        return None
    try:
        return datetime.strptime(v.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def s(v: str) -> str | None:
    v = (v or "").strip()
    return v or None


# ---------------------------------------------------------------------------
# Campos 12.1 .. 12.16 (sub-registros de pagamento dentro do campo 12)
# ---------------------------------------------------------------------------
SUBCAMPOS: list[tuple[str, Callable[[str], Any]]] = [
    ("pg_numero_documento_titular_domicilio", parse_documento),  # 12.1
    ("pg_tipo_conta", s),                                         # 12.2
    ("pg_compe", s),                                             # 12.3
    ("pg_ispb", lambda v: (s(v) or "").zfill(8) or None),        # 12.4
    ("pg_agencia", s),                                          # 12.5
    ("pg_numero_conta", s),                                     # 12.6
    ("pg_valor_a_pagar", parse_decimal),                        # 12.7
    ("pg_beneficiario", parse_documento),                       # 12.8
    ("pg_data_liquidacao_efetiva", parse_data),                 # 12.9
    ("pg_valor_liquidacao_efetiva", parse_decimal),             # 12.10
    ("pg_regra_divisao", s),                                    # 12.11
    ("pg_valor_onerado_ur", parse_decimal),                     # 12.12
    ("pg_tipo_informacao_pagamento", s),                        # 12.13
    ("pg_indicador_ordem_efeito", s),                           # 12.14
    ("pg_valor_constituido_efeito_ur", parse_decimal),          # 12.15
    ("pg_identificador_cerc_contrato", s),                      # 12.16
]
SUB_NOMES = [nome for nome, _ in SUBCAMPOS]


def parse_lista_pagamentos(blob: str) -> list[dict[str, Any]]:
    """Recebe o conteúdo do campo 12 e devolve a lista de pagamentos."""
    if not blob or not blob.strip():
        return []
    registros = [r for r in (x.strip() for x in blob.split("|")) if r]
    out = []
    for reg in registros:
        brutos = reg.split(";")
        item = {}
        for i, (nome, parser) in enumerate(SUBCAMPOS):
            item[nome] = parser(brutos[i] if i < len(brutos) else "")
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# Campos 1 .. 16 (nível da UR)
# ---------------------------------------------------------------------------
@dataclass
class CampoUR:
    nome: str
    parser: Callable[[str], Any] | None  # None = tratamento especial (campo 12)


CAMPOS_UR: list[CampoUR] = [
    CampoUR("referencia_externa", s),                                  # 1
    CampoUR("entidade_registradora", parse_documento),                 # 2
    CampoUR("instituicao_credenciadora", parse_documento),             # 3
    CampoUR("usuario_final_recebedor", parse_documento),               # 4
    CampoUR("arranjo_pagamento", s),                                   # 5
    CampoUR("data_liquidacao", parse_data),                            # 6
    CampoUR("titular_ur", parse_documento),                            # 7
    CampoUR("constituicao_ur", s),                                     # 8
    CampoUR("valor_constituido_total", parse_decimal),                 # 9
    CampoUR("valor_constituido_antecipacao_precontratado", parse_decimal),  # 10
    CampoUR("valor_bloqueado", parse_decimal),                         # 11
    CampoUR("lista_informacoes_pagamento", None),                      # 12 (aninhado)
    CampoUR("carteira", s),                                            # 13
    CampoUR("valor_livre", parse_decimal),                             # 14
    CampoUR("valor_total_ur", parse_decimal),                          # 15
    CampoUR("data_hora_ultima_atualizacao", s),                        # 16
]
N_UR = len(CAMPOS_UR)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def ler_agenda(caminho: str):
    linhas_ur: list[dict[str, Any]] = []
    linhas_pg: list[dict[str, Any]] = []
    avisos: list[str] = []

    with open(caminho, "r", encoding="utf-8", newline="") as f:
        leitor = csv.reader(f, delimiter=";", quotechar='"')
        for n, campos in enumerate(leitor, start=1):
            if not campos or all(c == "" for c in campos):
                continue
            if len(campos) != N_UR:
                avisos.append(f"linha {n}: esperados {N_UR} campos, "
                              f"encontrados {len(campos)}")

            registro: dict[str, Any] = {"ur_id": n}
            pagamentos: list[dict[str, Any]] = []

            for i, campo in enumerate(CAMPOS_UR):
                bruto = campos[i] if i < len(campos) else ""
                if campo.parser is None:        # campo 12: lista aninhada
                    pagamentos = parse_lista_pagamentos(bruto)
                    registro["qtd_pagamentos"] = len(pagamentos)
                    registro["lista_informacoes_pagamento"] = pagamentos
                else:
                    registro[campo.nome] = campo.parser(bruto)

            linhas_ur.append(registro)
            for ordem, pg in enumerate(pagamentos, start=1):
                linhas_pg.append({"ur_id": n, "ordem_no_array": ordem, **pg})

    # DataFrame nível UR
    colunas_ur = (["ur_id"]
                  + [c.nome if c.parser else "qtd_pagamentos" for c in CAMPOS_UR]
                  + ["lista_informacoes_pagamento"])
    # remove duplicata de nome e reordena de forma limpa
    df_ur = pd.DataFrame(linhas_ur)

    # DataFrame nível pagamento (explodido)
    df_pg = pd.DataFrame(linhas_pg,
                         columns=["ur_id", "ordem_no_array"] + SUB_NOMES)

    if avisos:
        print(f"=== {len(avisos)} aviso(s) de contagem de campos ===")
        for a in avisos[:20]:
            print(" -", a)
        if len(avisos) > 20:
            print(f"   ... e mais {len(avisos) - 20}")
    else:
        print("Contagem de campos OK em todas as linhas.")

    return df_ur, df_pg


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    CAMINHO = "/mnt/user-data/uploads/CERC-AP005_44198946_20260528_ret_agenda_nova_unificado.csv"
    df_ur, df_pg = ler_agenda(CAMINHO)

    print(f"\nURs (linhas): {len(df_ur)}")
    print(f"Pagamentos (explodido): {len(df_pg)}")

    # salvar: o df_ur leva a lista como JSON pra caber no CSV
    df_ur_csv = df_ur.copy()
    df_ur_csv["lista_informacoes_pagamento"] = df_ur_csv[
        "lista_informacoes_pagamento"].apply(lambda x: len(x))
    df_ur_csv = df_ur_csv.drop(columns=["lista_informacoes_pagamento"])
    df_ur_csv.to_csv("/mnt/user-data/outputs/agenda_ur.csv", index=False)
    df_pg.to_csv("/mnt/user-data/outputs/agenda_pagamentos.csv", index=False)
    print("\nCSVs salvos: agenda_ur.csv e agenda_pagamentos.csv")


# ===========================================================================
# EXTENSÕES (integração pipeline Izidor) — multi-arquivo, resumo e processamento
# Acrescentadas sem alterar as funções originais acima.
# ===========================================================================
import glob as _glob
from pathlib import Path as _Path

# Tipo de informação de pagamento (subcampo 12.13) — dicionário oficial do manual
TIPO_INFORMACAO_PAGAMENTO = {
    "1": "troca_titularidade", "2": "onus_cessao_fiduciaria", "3": "onus_outros",
    "4": "bloqueio_judicial",  "5": "antecipacao_pos_contratada", "6": "liquidacao",
    "7": "domicilio_pagamento", "8": "promessa_cessao",
}


def ler_agenda_multi(caminhos: list[str]):
    """Lê e concatena vários arquivos AP005 (ex.: os 3 retornos), mantendo ur_id global."""
    import pandas as pd
    dfs_ur, dfs_pg = [], []
    desloc = 0
    for cam in sorted(caminhos):
        df_ur, df_pg = ler_agenda(cam)
        if desloc:
            df_ur = df_ur.copy(); df_pg = df_pg.copy()
            df_ur["ur_id"] = df_ur["ur_id"] + desloc
            if len(df_pg):
                df_pg["ur_id"] = df_pg["ur_id"] + desloc
        dfs_ur.append(df_ur); dfs_pg.append(df_pg)
        desloc += int(df_ur["ur_id"].max()) if len(df_ur) else 0
    df_ur = pd.concat(dfs_ur, ignore_index=True)
    df_pg = pd.concat(dfs_pg, ignore_index=True)
    return df_ur, df_pg


def _f(v):
    """Decimal/None → float/0.0 para agregação."""
    return float(v) if v is not None else 0.0


def resumir_agenda(df_ur, df_pg) -> dict:
    """Digest agregado da agenda completa (para o bloco autoritativo da API).
    Comprometido vem do efeito 12.15 (pg_valor_constituido_efeito_ur) por tipo (12.13),
    NUNCA do parâmetro 12.12 (pg_valor_onerado_ur)."""
    datas = [d for d in df_ur["data_liquidacao"].tolist() if d]
    totais = {
        "constituido_total": round(sum(_f(v) for v in df_ur["valor_constituido_total"]), 2),
        "constituido_antecipacao_precontratado": round(sum(_f(v) for v in df_ur["valor_constituido_antecipacao_precontratado"]), 2),
        "bloqueado": round(sum(_f(v) for v in df_ur["valor_bloqueado"]), 2),
        "livre": round(sum(_f(v) for v in df_ur["valor_livre"]), 2),
    }
    # comprometido por tipo de efeito
    comp: dict[str, float] = {}
    if len(df_pg):
        for tipo, vef in zip(df_pg["pg_tipo_informacao_pagamento"], df_pg["pg_valor_constituido_efeito_ur"]):
            v = _f(vef)
            if v:
                rotulo = TIPO_INFORMACAO_PAGAMENTO.get(str(tipo), str(tipo))
                comp[rotulo] = round(comp.get(rotulo, 0.0) + v, 2)
    # agenda por mês
    por_mes: dict[str, dict] = {}
    for d, vct, vliv in zip(df_ur["data_liquidacao"], df_ur["valor_constituido_total"], df_ur["valor_livre"]):
        if not d:
            continue
        m = por_mes.setdefault(d[:7], {"n_urs": 0, "constituido_total": 0.0, "livre": 0.0})
        m["n_urs"] += 1; m["constituido_total"] += _f(vct); m["livre"] += _f(vliv)
    for k in por_mes:
        por_mes[k]["constituido_total"] = round(por_mes[k]["constituido_total"], 2)
        por_mes[k]["livre"] = round(por_mes[k]["livre"], 2)
    # distribuição por arranjo
    por_arr: dict[str, dict] = {}
    for arr, vct in zip(df_ur["arranjo_pagamento"], df_ur["valor_constituido_total"]):
        a = por_arr.setdefault(arr or "?", {"n_urs": 0, "constituido_total": 0.0})
        a["n_urs"] += 1; a["constituido_total"] += _f(vct)
    for k in por_arr:
        por_arr[k]["constituido_total"] = round(por_arr[k]["constituido_total"], 2)
    # beneficiários distintos (12.8)
    benef = sorted({b for b in (df_pg["pg_beneficiario"].tolist() if len(df_pg) else []) if b}) if len(df_pg) else []
    return {
        "source_format": "cerc_ap005_ret_agenda_futura",
        "schema_status": "CONFIRMADO_AP005",
        "n_urs": int(len(df_ur)),
        "n_pagamentos": int(len(df_pg)),
        "periodo_liquidacao": {"min": min(datas) if datas else None, "max": max(datas) if datas else None},
        "totais": totais,
        "comprometido": {
            "total": round(sum(comp.values()), 2),
            "por_tipo_efeito": comp,
            "cessao_fiduciaria": comp.get("onus_cessao_fiduciaria", 0.0),
        },
        "agenda_por_mes": dict(sorted(por_mes.items())),
        "distribuicao_por_arranjo": dict(sorted(por_arr.items())),
        "beneficiarios_distintos": benef,
    }


def processar_agenda(caminhos: list[str]):
    """Pipeline completo: extração COMPLETA (df_ur + df_pg) + digest agregado.
    Retorna (digest, df_ur, df_pg)."""
    df_ur, df_pg = ler_agenda_multi(caminhos)
    return resumir_agenda(df_ur, df_pg), df_ur, df_pg
