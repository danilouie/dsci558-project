const NS = "kg-trace";

/**
 * Step-by-step tracing for search and recommend flows.
 * Set `TRACE_STEPS=false` in env to disable (all other values leave tracing on).
 */
export function isTraceEnabled() {
  return process.env.TRACE_STEPS !== "false";
}

/**
 * Verbose Ollama NL logs (full prompts + raw/ parsed outputs). Does not require TRACE_STEPS.
 * - `OLLAMA_LOG_STEPS=true`  → always log
 * - `OLLAMA_LOG_STEPS=false` → never log
 * - unset                 → follow TRACE_STEPS (on unless TRACE_STEPS=false)
 */
export function isOllamaLogEnabled() {
  const v = process.env.OLLAMA_LOG_STEPS;
  if (v === "false") return false;
  if (v === "true") return true;
  return isTraceEnabled();
}

/**
 * @param {string} step
 * @param {string} phase - e.g. "prompt", "response_raw", "parsed"
 * @param {unknown} [data]
 */
export function ollamaLog(step, phase, data) {
  if (!isOllamaLogEnabled()) return;
  const t = new Date().toISOString();
  const line = `[${t}] [${NS}:ollama] [${step}] ${phase}`;
  if (data === undefined) {
    console.log(line);
    return;
  }
  if (typeof data === "string") {
    console.log(line);
    console.log(data);
    return;
  }
  try {
    console.log(line, JSON.stringify(data, null, 2));
  } catch {
    console.log(line, data);
  }
}

/**
 * @param {string} scope
 * @param {string} step
 * @param {Record<string, unknown> | string | number | null | undefined} [detail]
 */
export function traceStep(scope, step, detail) {
  if (!isTraceEnabled()) return;
  const t = new Date().toISOString();
  if (detail !== undefined) {
    console.log(`[${t}] [${NS}:${scope}] ${step}`, typeof detail === "object" && detail !== null ? detail : { value: detail });
  } else {
    console.log(`[${t}] [${NS}:${scope}] ${step}`);
  }
}
