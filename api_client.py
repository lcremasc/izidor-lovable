"""
api_client.py
=============
Cliente HTTP para envio do payload final ao endpoint Supabase/Lovable.

Endpoint:  https://dttaxfnfflgqqetqdyir.supabase.co/functions/v1/ingest
Auth:      header x-api-key
Payload:   JSON completo montado pelo montador.py

Uso:
    from api_client import enviar_payload, ResultadoEnvio
    resultado = await enviar_payload(payload, api_key)
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

import httpx


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

ENDPOINT_URL     = "https://dttaxfnfflgqqetqdyir.supabase.co/functions/v1/ingest"
TIMEOUT_SEGUNDOS = 60
MAX_TENTATIVAS   = 3
BACKOFF_BASE     = 2.0   # segundos — dobra a cada retry


# ---------------------------------------------------------------------------
# Tipos
# ---------------------------------------------------------------------------

@dataclass
class ResultadoEnvio:
    sucesso:      bool
    status_code:  int | None         = None
    response_body: dict | str | None = None
    tentativas:   int                = 1
    duracao_s:    float              = 0.0
    erro:         str | None         = None


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

async def enviar_payload(
    payload: dict,
    api_key: str | None = None,
) -> ResultadoEnvio:
    """
    Envia o payload final ao endpoint Lovable com retry automático.

    A chave de API é lida na seguinte ordem de prioridade:
      1. Parâmetro `api_key` (passado diretamente)
      2. Variável de ambiente LOVABLE_API_KEY

    Retenta até MAX_TENTATIVAS vezes com backoff exponencial
    em caso de erro de rede ou status 5xx.
    Status 4xx não são retentados (erro de payload — não adianta tentar de novo).
    """
    chave = api_key or os.getenv("LOVABLE_API_KEY", "")
    if not chave:
        return ResultadoEnvio(
            sucesso=False,
            erro="API key não fornecida. Defina LOVABLE_API_KEY ou passe api_key.",
        )

    headers = {
        "x-api-key":    chave,
    }

    t0          = time.monotonic()
    ultima_erro = ""
    tentativa   = 0

    async with httpx.AsyncClient(timeout=TIMEOUT_SEGUNDOS) as client:
        while tentativa < MAX_TENTATIVAS:
            tentativa += 1
            try:
                resp = await client.post(
                    ENDPOINT_URL,
                    headers=headers,
                    json=payload,  # httpx serializa e seta Content-Type automaticamente
                )
                duracao = round(time.monotonic() - t0, 2)

                # Tentar parsear body como JSON
                try:
                    body = resp.json()
                except Exception:
                    body = resp.text

                # Sucesso
                if resp.status_code in (200, 201):
                    return ResultadoEnvio(
                        sucesso=True,
                        status_code=resp.status_code,
                        response_body=body,
                        tentativas=tentativa,
                        duracao_s=duracao,
                    )

                # Erro 4xx — não retentar
                if 400 <= resp.status_code < 500:
                    return ResultadoEnvio(
                        sucesso=False,
                        status_code=resp.status_code,
                        response_body=body,
                        tentativas=tentativa,
                        duracao_s=duracao,
                        erro=f"HTTP {resp.status_code} — erro no payload (não retentável)",
                    )

                # Erro 5xx — retentar
                ultima_erro = f"HTTP {resp.status_code}"

            except httpx.TimeoutException as exc:
                ultima_erro = f"Timeout após {TIMEOUT_SEGUNDOS}s: {exc}"
            except httpx.ConnectError as exc:
                ultima_erro = f"Erro de conexão: {exc}"
            except Exception as exc:
                ultima_erro = f"Erro inesperado: {exc}"

            # Backoff antes de retentar
            if tentativa < MAX_TENTATIVAS:
                await _sleep(BACKOFF_BASE ** tentativa)

    duracao = round(time.monotonic() - t0, 2)
    return ResultadoEnvio(
        sucesso=False,
        status_code=None,
        erro=f"Falhou após {tentativa} tentativas. Último erro: {ultima_erro}",
        tentativas=tentativa,
        duracao_s=duracao,
    )


# ---------------------------------------------------------------------------
# Helper de sleep assíncrono
# ---------------------------------------------------------------------------

async def _sleep(segundos: float) -> None:
    import asyncio
    await asyncio.sleep(segundos)


# ---------------------------------------------------------------------------
# Utilitário de diagnóstico
# ---------------------------------------------------------------------------

def inspecionar_payload(payload: dict) -> dict:
    """
    Retorna um resumo do payload para log/debug sem expor dados sensíveis.
    """
    slides = payload.get("slides_content", {})
    indicadores = payload.get("output_data", {}).get("indicadores", {})
    fi = (
        payload.get("input_data", {})
               .get("input_credito_empresa", {})
               .get("financial_inputs", {})
    )

    return {
        "company_name":    payload.get("company_name"),
        "cnpj":            payload.get("cnpj"),
        "analysis_date":   payload.get("analysis_date"),
        "status":          payload.get("status"),
        "slides_presentes": sorted(k for k, v in slides.items() if v is not None),
        "slides_nulos":     sorted(k for k, v in slides.items() if v is None),
        "indicadores_presentes": sorted(indicadores.keys()),
        "raw_items": {
            "balance_sheet":    len(fi.get("balance_sheet",    {}).get("raw_items", [])),
            "income_statement": len(fi.get("income_statement", {}).get("raw_items", [])),
            "monthly_revenue":  len(fi.get("monthly_revenue",  {}).get("raw_items", [])),
        },
        "tamanho_payload_kb": round(
            len(json.dumps(payload, ensure_ascii=False).encode()) / 1024, 1
        ),
    }
