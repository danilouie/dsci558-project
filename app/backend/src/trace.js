const NS = "kg-trace";

/**
 * Step-by-step tracing for search and recommend flows.
 * Set `TRACE_STEPS=false` in env to disable (all other values leave tracing on).
 */
export function isTraceEnabled() {
  return process.env.TRACE_STEPS !== "false";
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
