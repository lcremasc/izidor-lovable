"""
orquestrador.py
===============
Encadeia as 5 etapas do pipeline de análise de crédito corporativo.

Etapas:
  E1 — Claude API + web_search  → dict_p1  (pesquisa pública)
  E2 — Claude API multimodal    → dict_p2  (extração de documentos)
  E3 — Python puro              → dict_p3  (indicadores calculados)
  E4 — Claude API               → dict_p4  (memorando / slides)
  E5 — montador + POST Lovable  → payload  (entregue ao frontend)

Uso:
    from orquestrador import executar_pipeline, PipelineInput
    resultado = await executar_pipeline(inp, callbacks)
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Awaitable

import httpx

from calculadora import calcular
from montador import montar_payload, validar_payload
from api_client import enviar_payload, ResultadoEnvio


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODELO_CLAUDE     = "claude-sonnet-4-20250514"
MAX_TOKENS        = 8192
MAX_TOKENS_P4     = 16000     # p4 gera JSON grande
TIMEOUT_SEGUNDOS  = 300       # 5 min por chamada de API

# Caminhos dos prompts (relativos ao diretório deste arquivo)
_DIR = Path(__file__).parent
PROMPT_P1_PATH = _DIR / "PROMPT_00_PESQUISA_v4.md"   # pesquisa pública (E1)
PROMPT_P2_PATH = _DIR / "PROMPT_01_EXTRACAO_v5.md"
PROMPT_P4_PATH = _DIR / "PROMPT_04_MEMORANDO_v1.md"


# ---------------------------------------------------------------------------
# Tipos de dados
# ---------------------------------------------------------------------------

@dataclass
class Documento:
    """Arquivo a ser enviado para extração na Etapa 2."""
    nome:      str
    conteudo:  bytes           # conteúdo binário do arquivo
    mime_type: str             # "application/pdf" | "image/png" | etc.


@dataclass
class PipelineInput:
    """Entrada completa do pipeline."""
    cnpj:               str                   # com máscara XX.XXX.XXX/XXXX-XX
    empresa:            str                   # nome da empresa
    produto_prioritario: str | None = None
    documentos:         list[Documento] = field(default_factory=list)
    api_key:            str | None = None     # x-api-key para o endpoint Lovable
    anthropic_api_key:  str | None = None     # x-api-key para a API do Claude (E1, E2, E4)

    # Parâmetros opcionais
    pular_e1:   bool        = False
    p1_externo: dict | None = None


@dataclass
class PipelineResultado:
    """Resultado de cada etapa."""
    sucesso:   bool
    payload:   dict | None      = None
    p1:        dict | None      = None
    p2:        dict | None      = None
    p3:        dict | None      = None
    p4:        dict | None      = None
    erros:     list[str]        = field(default_factory=list)
    duracoes:  dict[str, float] = field(default_factory=dict)  # etapa → segundos


Callback = Callable[[str, Any], Awaitable[None]] | None


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _ler_prompt(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Prompt não encontrado: {path}")
    return path.read_text(encoding="utf-8")


def _extrair_json(texto: str) -> dict:
    """
    Extrai o primeiro objeto JSON válido de uma string de resposta do Claude.
    Lida com markdown (```json ... ```) e texto extra antes/depois.
    """
    # Tentativa 1: parse direto (caso o Claude retorne JSON puro)
    try:
        return json.loads(texto.strip())
    except json.JSONDecodeError:
        pass

    # Tentativa 2: remover blocos de markdown
    limpo = re.sub(r"```(?:json)?\s*", "", texto)
    limpo = re.sub(r"```\s*$", "", limpo, flags=re.MULTILINE)
    try:
        return json.loads(limpo.strip())
    except json.JSONDecodeError:
        pass

    # Tentativa 3: encontrar o primeiro { ... } balanceado
    inicio = texto.find("{")
    if inicio == -1:
        raise ValueError("Nenhum objeto JSON encontrado na resposta")
    profundidade = 0
    for i, c in enumerate(texto[inicio:], inicio):
        if c == "{":
            profundidade += 1
        elif c == "}":
            profundidade -= 1
            if profundidade == 0:
                return json.loads(texto[inicio : i + 1])
    raise ValueError("JSON incompleto na resposta do Claude")


def _documentos_para_content(documentos: list[Documento]) -> list[dict]:
    """
    Converte lista de Documento para o formato de content blocks da API Anthropic.
    Suporta PDF e imagens.
    """
    import base64
    blocks = []
    for doc in documentos:
        data_b64 = base64.standard_b64encode(doc.conteudo).decode()
        if doc.mime_type == "application/pdf":
            blocks.append({
                "type": "document",
                "source": {
                    "type":       "base64",
                    "media_type": "application/pdf",
                    "data":       data_b64,
                },
                "title": doc.nome,
            })
        elif doc.mime_type.startswith("image/"):
            blocks.append({
                "type": "image",
                "source": {
                    "type":       "base64",
                    "media_type": doc.mime_type,
                    "data":       data_b64,
                },
            })
        else:
            # Excel e outros: enviar como documento genérico
            blocks.append({
                "type": "document",
                "source": {
                    "type":       "base64",
                    "media_type": "application/octet-stream",
                    "data":       data_b64,
                },
                "title": doc.nome,
            })
    return blocks


async def _chamar_claude(
    mensagens:  list[dict],
    anthropic_api_key: str = "",
    max_tokens: int = MAX_TOKENS,
    tools:      list[dict] | None = None,
) -> str:
    """
    Chama a API do Claude e retorna o texto da resposta.
    Requer anthropic_api_key (x-api-key da Anthropic).
    Lida com tool_use (web_search) percorrendo todos os blocos de content.
    """
    chave = anthropic_api_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not chave:
        raise ValueError(
            "Chave da API Anthropic não fornecida. "
            "Defina ANTHROPIC_API_KEY ou passe anthropic_api_key."
        )

    body: dict[str, Any] = {
        "model":      MODELO_CLAUDE,
        "max_tokens": max_tokens,
        "messages":   mensagens,
    }
    if tools:
        body["tools"] = tools

    headers = {
        "Content-Type":  "application/json",
        "x-api-key":     chave,
        "anthropic-version": "2023-06-01",
    }

    async with httpx.AsyncClient(timeout=TIMEOUT_SEGUNDOS) as client:
        resp = await client.post(
            ANTHROPIC_API_URL,
            headers=headers,
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

    # Juntar todos os blocos de texto (ignorar tool_use)
    partes = [
        bloco["text"]
        for bloco in data.get("content", [])
        if bloco.get("type") == "text"
    ]
    return "\n".join(partes)


# ---------------------------------------------------------------------------
# Etapa 1 — Pesquisa pública (E1)
# ---------------------------------------------------------------------------

async def _executar_e1(inp: PipelineInput, cb: Callback) -> dict:
    """
    Pesquisa pública sobre a empresa via Claude + web_search.
    Retorna dict_p1.

    Se PROMPT_00_PESQUISA.md não existir, retorna um p1 mínimo com os dados
    informados pelo usuário (empresa + CNPJ).
    """
    if inp.pular_e1 and inp.p1_externo:
        return inp.p1_externo

    try:
        prompt_p1 = _ler_prompt(PROMPT_P1_PATH)
    except FileNotFoundError:
        # Fallback: p1 mínimo com dados cadastrais básicos
        return {
            "company_name": inp.empresa,
            "cnpj":         inp.cnpj,
            "sector":       "",
            "logo_url":     None,
            "company_image": None,
            "store_images":  [],
            "alertas_p1":    ["Pesquisa pública (E1) não executada — PROMPT_00 não encontrado."],
        }

    mensagem_usuario = (
        f"Empresa: {inp.empresa}\n"
        f"CNPJ: {inp.cnpj}\n\n"
        f"{prompt_p1}"
    )

    tools = [{"type": "web_search_20250305", "name": "web_search"}]
    texto = await _chamar_claude(
        mensagens=[{"role": "user", "content": mensagem_usuario}],
        anthropic_api_key=inp.anthropic_api_key or "",
        tools=tools,
    )
    return _extrair_json(texto)


# ---------------------------------------------------------------------------
# Etapa 2 — Extração de documentos (E2)
# ---------------------------------------------------------------------------

async def _executar_e2(
    inp:    PipelineInput,
    p1:     dict,
    cb:     Callback,
) -> dict:
    """
    Extração de documentos via Claude multimodal.
    Envia os arquivos como base64 junto com o PROMPT_01 e retorna dict_p2.
    """
    prompt_p2 = _ler_prompt(PROMPT_P2_PATH)

    # Montar content: documentos + instrução
    content_blocks = _documentos_para_content(inp.documentos)

    instrucao = (
        f"Empresa: {inp.empresa}\n"
        f"CNPJ: {inp.cnpj}\n"
    )
    if inp.produto_prioritario:
        instrucao += f"Produto prioritário: {inp.produto_prioritario}\n"
    instrucao += f"\n{prompt_p2}"

    content_blocks.append({"type": "text", "text": instrucao})

    texto = await _chamar_claude(
        mensagens=[{"role": "user", "content": content_blocks}],
        anthropic_api_key=inp.anthropic_api_key or "",
        max_tokens=MAX_TOKENS,
    )
    p2 = _extrair_json(texto)

    # Garantir que produto_prioritario está no p2
    if inp.produto_prioritario and not p2.get("produto_prioritario"):
        p2["produto_prioritario"] = inp.produto_prioritario

    return p2


# ---------------------------------------------------------------------------
# Etapa 3 — Cálculo de indicadores (E3 — Python puro)
# ---------------------------------------------------------------------------

def _executar_e3(p2: dict) -> dict:
    """
    Chama calculadora.calcular(p2) e retorna dict_p3.
    Zero chamadas de API — determinístico.
    """
    return calcular(p2)


# ---------------------------------------------------------------------------
# Etapa 4 — Memorando / slides (E4)
# ---------------------------------------------------------------------------

async def _executar_e4(
    p1: dict,
    p2: dict,
    p3: dict,
    cb: Callback,
    anthropic_api_key: str = "",
) -> dict:
    """
    Gera o memorando via Claude a partir de p1 + p2 + p3.
    Retorna dict_p4 (slides para o Lovable).
    """
    prompt_p4 = _ler_prompt(PROMPT_P4_PATH)

    # Serializar p1, p2, p3 para o contexto
    # p2 é omitido de campos volumosos (raw_items do balanço/DRE/faturamento)
    # para não desperdiçar contexto — o Claude precisa dos indicadores (p3), não dos raw
    p2_resumido = {k: v for k, v in p2.items() if k not in ("balanco", "dre", "faturamento_mensal")}
    # Mas mantém bureaux, CERC, dados cadastrais e produto_prioritario

    mensagem = (
        f"## dict_p1 (pesquisa pública)\n```json\n{json.dumps(p1, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## dict_p2 (documentos extraídos — sem raw_items financeiros)\n```json\n{json.dumps(p2_resumido, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## dict_p3 (indicadores calculados)\n```json\n{json.dumps(p3, ensure_ascii=False, indent=2)}\n```\n\n"
        f"---\n\n{prompt_p4}"
    )

    texto = await _chamar_claude(
        mensagens=[{"role": "user", "content": mensagem}],
        anthropic_api_key=anthropic_api_key,
        max_tokens=MAX_TOKENS_P4,
    )
    return _extrair_json(texto)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

async def executar_pipeline(
    inp:       PipelineInput,
    callbacks: dict[str, Callback] | None = None,
) -> PipelineResultado:
    """
    Executa as 5 etapas do pipeline em sequência.

    callbacks: dict opcional com funções assíncronas chamadas em cada etapa.
      Chaves possíveis: "e1_inicio", "e1_fim", "e2_inicio", "e2_fim",
                        "e3_inicio", "e3_fim", "e4_inicio", "e4_fim",
                        "e5_inicio", "e5_fim", "erro"
      Cada callback recebe (nome_etapa: str, dados: Any).

    Retorna PipelineResultado com p1/p2/p3/p4/payload e erros se houver.
    """
    cb_map   = callbacks or {}
    resultado = PipelineResultado(sucesso=False)

    async def _cb(nome: str, dados: Any = None) -> None:
        fn = cb_map.get(nome)
        if fn:
            await fn(nome, dados)

    def _tempo(t0: float) -> float:
        return round(time.monotonic() - t0, 2)

    # ── E1 ────────────────────────────────────────────────────────────────
    await _cb("e1_inicio")
    t0 = time.monotonic()
    try:
        p1 = await _executar_e1(inp, None)
        resultado.p1 = p1
        resultado.duracoes["e1"] = _tempo(t0)
        await _cb("e1_fim", p1)
    except Exception as exc:
        resultado.erros.append(f"E1 falhou: {exc}\n{traceback.format_exc()}")
        await _cb("erro", {"etapa": "e1", "erro": str(exc)})
        return resultado

    # ── E2 ────────────────────────────────────────────────────────────────
    await _cb("e2_inicio")
    t0 = time.monotonic()
    try:
        p2 = await _executar_e2(inp, p1, None)
        resultado.p2 = p2
        resultado.duracoes["e2"] = _tempo(t0)
        await _cb("e2_fim", p2)
    except Exception as exc:
        resultado.erros.append(f"E2 falhou: {exc}\n{traceback.format_exc()}")
        await _cb("erro", {"etapa": "e2", "erro": str(exc)})
        return resultado

    # ── E3 ────────────────────────────────────────────────────────────────
    await _cb("e3_inicio")
    t0 = time.monotonic()
    try:
        p3 = _executar_e3(p2)
        resultado.p3 = p3
        resultado.duracoes["e3"] = _tempo(t0)
        await _cb("e3_fim", p3)
    except Exception as exc:
        resultado.erros.append(f"E3 falhou: {exc}\n{traceback.format_exc()}")
        await _cb("erro", {"etapa": "e3", "erro": str(exc)})
        return resultado

    # ── E4 ────────────────────────────────────────────────────────────────
    await _cb("e4_inicio")
    t0 = time.monotonic()
    try:
        p4 = await _executar_e4(p1, p2, p3, None, anthropic_api_key=inp.anthropic_api_key or "")
        resultado.p4 = p4
        resultado.duracoes["e4"] = _tempo(t0)
        await _cb("e4_fim", p4)
    except Exception as exc:
        resultado.erros.append(f"E4 falhou: {exc}\n{traceback.format_exc()}")
        await _cb("erro", {"etapa": "e4", "erro": str(exc)})
        return resultado

    # ── E5 — montagem + validação + POST ─────────────────────────────────
    await _cb("e5_inicio")
    t0 = time.monotonic()
    try:
        payload = montar_payload(p1, p2, p3, p4)
        erros_payload = validar_payload(payload)
        if erros_payload:
            resultado.erros.extend([f"Payload inválido: {e}" for e in erros_payload])
            await _cb("erro", {"etapa": "e5", "erros": erros_payload})
            return resultado

        resultado.payload = payload

        # POST ao endpoint Lovable
        envio = await enviar_payload(payload, api_key=inp.api_key)
        if not envio.sucesso:
            resultado.erros.append(
                f"POST falhou (HTTP {envio.status_code}): {envio.erro}"
            )
            await _cb("erro", {"etapa": "e5_post", "erro": envio.erro})
            return resultado

        resultado.sucesso  = True
        resultado.duracoes["e5"] = _tempo(t0)
        await _cb("e5_fim", {
            "payload_kb":  round(len(__import__('json').dumps(payload)) / 1024, 1),
            "status_code": envio.status_code,
            "tentativas":  envio.tentativas,
            "duracao_post_s": envio.duracao_s,
        })
    except Exception as exc:
        resultado.erros.append(f"E5 falhou: {exc}\n{traceback.format_exc()}")
        await _cb("erro", {"etapa": "e5", "erro": str(exc)})
        return resultado

    return resultado


# ---------------------------------------------------------------------------
# Execução direta (teste de smoke)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    async def _smoke():
        inp = PipelineInput(
            cnpj="03.073.056/0001-64",
            empresa="Locatelli Supermercados",
            produto_prioritario="capital_de_giro",
            documentos=[],    # sem documentos — E2 retornará p2 vazio
            pular_e1=True,
            p1_externo={
                "company_name": "Locatelli Supermercados",
                "cnpj":         "03.073.056/0001-64",
                "sector":       "Varejo Alimentar",
                "logo_url":     None,
                "company_image": None,
                "store_images":  [],
            },
        )

        async def log(nome, dados=None):
            print(f"  [{nome}]", end="")
            if isinstance(dados, dict):
                print(f" {list(dados.keys())[:3]}...")
            else:
                print()

        print("Iniciando smoke test...")
        res = await executar_pipeline(inp, callbacks={
            "e1_inicio": log, "e1_fim": log,
            "e2_inicio": log, "e2_fim": log,
            "e3_inicio": log, "e3_fim": log,
            "e4_inicio": log, "e4_fim": log,
            "e5_inicio": log, "e5_fim": log,
            "erro": log,
        })

        print(f"\nSucesso: {res.sucesso}")
        print(f"Erros:   {res.erros}")
        print(f"Tempos:  {res.duracoes}")

    asyncio.run(_smoke())
