// supabase/functions/run-pipeline/index.ts
// ─────────────────────────────────────────────────────────────────────────────
// Edge Function simples — recebe { analysis_id } do formulário e repassa
// ao servidor Python no Railway. Retorna 202 imediatamente.
//
// Variáveis de ambiente necessárias (Supabase → Settings → Edge Functions → Secrets):
//   PYTHON_SERVER_URL  → URL do Railway, ex: https://meu-pipeline.up.railway.app
//   PIPELINE_SECRET    → mesmo valor definido no servidor Python (opcional mas recomendado)
// ─────────────────────────────────────────────────────────────────────────────

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const PYTHON_SERVER_URL = Deno.env.get("PYTHON_SERVER_URL") ?? "";
const PIPELINE_SECRET   = Deno.env.get("PIPELINE_SECRET")   ?? "";

serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body: { analysis_id?: string };
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { analysis_id } = body;
  if (!analysis_id) {
    return new Response(JSON.stringify({ error: "analysis_id is required" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (!PYTHON_SERVER_URL) {
    return new Response(
      JSON.stringify({ error: "PYTHON_SERVER_URL not configured" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }

  // Repassar ao servidor Python (fire-and-forget — não awaita a resposta completa)
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (PIPELINE_SECRET) {
    headers["X-Pipeline-Secret"] = PIPELINE_SECRET;
  }

  // Disparo sem await — Edge Function retorna 202 imediatamente
  fetch(`${PYTHON_SERVER_URL}/run-pipeline`, {
    method: "POST",
    headers,
    body: JSON.stringify({ analysis_id }),
  }).catch((err) => {
    console.error("Erro ao chamar servidor Python:", err);
  });

  return new Response(
    JSON.stringify({ status: "accepted", analysis_id }),
    {
      status: 202,
      headers: {
        "Content-Type":                "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    }
  );
});
