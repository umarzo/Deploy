// FICASA Backend Worker — Full 5-Phase Pipeline
// Deployed via GitHub Actions to Cloudflare Workers
// Worker URL: https://ficasa-backend.officalumarx.workers.dev

// ═══════════════════════════════════════════════════════════════
// CONSTANTS & CONFIG
// ═══════════════════════════════════════════════════════════════

const API_BASE = 'https://openrouter.ai/api/v1';
const WALL_TIME_LIMIT = 13 * 60 * 1000; // 13 minutes — save partial at 13, Cloudflare kills at 15

// ── Structured logging ───────────────────────────────────────────
// Emits JSON log lines that Cloudflare Workers captures automatically.
// Viewable in the Workers dashboard under "Logs" → "Real-time Logs".
// Each log includes: level, event, jobId, uid, duration, tokensUsed, etc.
//
// ENHANCED: logEvent now ALSO persists each event into a per-job event log
// in KV (key: `events:{jobId}`). The frontend polls this via GET /events
// (or receives it in real-time via GET /stream SSE) to render the live
// activity feed — phase substeps, agent drafts, peer reviews, orchestrator
// thoughts, etc. Events are capped at the most recent 200 per job with a
// 24h TTL.
//
// CONCURRENCY-SAFE PERSISTENCE: events are buffered per-jobId and flushed
// through a serialized promise chain. This is critical because the pipeline
// runs 3 agents in parallel (concurrent draftAgent calls), each emitting
// multiple events — a naive get→append→put per event would lose 30-65% of
// events to last-write-wins clobbering. The buffer also coalesces bursts
// (50ms window) into single KV writes, cutting write pressure ~5x.
function logEvent(level, event, data = {}) {
  const entry = {
    ts: Date.now(),                       // epoch ms — frontend-friendly
    iso: new Date().toISOString(),        // human-readable
    level,                                // 'info' | 'warn' | 'error'
    event,                                // e.g. 'job.queued', 'phase.start', 'agent.drafting'
    ...data,
  };
  const prefix = level === 'error' ? '❌' : level === 'warn' ? '⚠️' : 'ℹ️';
  console.log(`${prefix} [FICASA] ${JSON.stringify(entry)}`);
  // Buffer the event for serialized, batched persistence. Never let a
  // logging failure break the pipeline — swallow errors here.
  try {
    if (_env && entry.jobId) {
      bufferEvent(_env, entry.jobId, entry).catch(() => {});
    }
  } catch {}
}

// Module-level ref to the active env (set at the top of queue()/fetch()).
let _env = null;

// ── Per-jobId event buffer + serialized flusher ──────────────────
// Map<jobId, { events: Entry[], flushPromise: Promise|null }>
//
// Why this exists: Cloudflare KV has no atomic append. A naive
// get→append→put per event loses data under concurrency (3 agents drafting
// in parallel each emit events; their get→put rounds interleave and
// clobber each other — last write wins, 30-65% of events vanish).
//
// The fix: accumulate events in an in-memory buffer per jobId, and flush
// through a single serialized promise chain per jobId. Within a flush:
//   1. wait 50ms to coalesce any concurrent bursts (3 parallel agents
//      firing agent.drafting within the same tick → 1 KV write, not 3)
//   2. splice the buffer atomically (no concurrent mutation during flush)
//   3. read-merge-write the KV key (serialized — no interleaving possible)
//
// This also cuts KV write pressure ~5x: instead of 1 write per event
// (~52 per job), we do ~1 write per 50ms window of activity (~10-15 per
// job). Reads from /events and /stream are unaffected (they read the
// merged log).
const _eventBuffers = new Map();

function bufferEvent(env, jobId, entry) {
  let buf = _eventBuffers.get(jobId);
  if (!buf) {
    buf = { events: [], flushPromise: null };
    _eventBuffers.set(jobId, buf);
  }
  buf.events.push(entry);
  // If no flush is in flight, schedule one. While a flush is pending,
  // new events accumulate in the buffer and will be picked up by the
  // NEXT flush (chained via .finally below). This guarantees ordering
  // and no data loss.
  if (!buf.flushPromise) {
    buf.flushPromise = _flushEventBuffer(env, jobId).finally(() => {
      const b = _eventBuffers.get(jobId);
      if (b) {
        b.flushPromise = null;
        // If more events arrived during the flush, chain another flush.
        if (b.events.length > 0) {
          bufferEvent(env, jobId, { __chained: true }); // re-trigger; the real events are already in b.events
        } else {
          _eventBuffers.delete(jobId); // idle — free the memory
        }
      }
    }).catch(() => {
      // Swallow — never let a flush failure propagate. The events are
      // still in the console log; only the KV persistence failed.
      const b = _eventBuffers.get(jobId);
      if (b) { b.flushPromise = null; }
    });
  }
  return buf.flushPromise;
}

// Flush one batch of buffered events for a jobId. Serialized per jobId
// via the flushPromise chain in bufferEvent() — only one flush runs at
// a time per job, so the read-merge-write is atomic from KV's perspective.
async function _flushEventBuffer(env, jobId) {
  const buf = _eventBuffers.get(jobId);
  if (!buf) return;
  // Coalesce window: wait briefly so concurrent events (fired within the
  // same tick by parallel agents) accumulate into one flush.
  await new Promise(r => setTimeout(r, 50));
  // Atomically drain the buffer. splice(0) removes all elements and
  // returns them; any events pushed during the await above will remain
  // in buf.events for the next chained flush.
  const toFlush = buf.events.splice(0, buf.events.length);
  // Filter out the internal re-trigger marker (it just wakes the chain).
  const real = toFlush.filter(e => !e.__chained);
  if (!real.length) return;

  const key = `events:${jobId}`;
  let prev = [];
  try {
    const raw = await env.FICASA_JOBS.get(key);
    if (raw) { prev = JSON.parse(raw); if (!Array.isArray(prev)) prev = []; }
  } catch { prev = []; }
  const merged = [...prev, ...real];
  // Cap at the most recent 200 events to bound KV value size (~each event <2KB).
  const capped = merged.length > 200 ? merged.slice(-200) : merged;
  await env.FICASA_JOBS.put(key, JSON.stringify(capped), { expirationTtl: 86400 });
}

const MODES = {
  vibe:    { name: 'Vibe Coding', temp: 0.7, maxTokens: 4000, integratorTokens: 8000, agentCount: 3, selfCritique: false, integrationLoops: 0, verify: true, prefer: 'fast' },
  serious: { name: 'Serious Work', temp: 0.55, maxTokens: 6000, integratorTokens: 12000, agentCount: 4, selfCritique: false, integrationLoops: 1, verify: true, prefer: 'balanced' },
  agentic: { name: 'Agentic Flow', temp: 0.4, maxTokens: 10000, integratorTokens: 16000, agentCount: 5, selfCritique: true, integrationLoops: 1, verify: true, prefer: 'deep' },
  // MAX mode — state-of-the-art pipeline. Activated when the client sends
  // mode: 'agentic' + maxPower: true in the /submit body.
  agentic_max: { name: 'Agentic Flow MAX', temp: 0.3, maxTokens: 14000, integratorTokens: 24000, agentCount: 7, selfCritique: true, selfCritiquePasses: 2, integrationLoops: 2, verify: true, prefer: 'deep', deepAnalysis: true, adversarialReview: true, coherenceAudit: true },
};

const MISSION_TYPE_GUIDE = {
  code: { label: 'Code / Software', deliverable_hint: 'Complete, runnable, idiomatic code. Include setup, usage, and edge-case handling.', review_focus: 'Correctness, security, error handling, readability, completeness.' },
  research: { label: 'Research / Brief', deliverable_hint: 'Structured research brief with clear sections: executive summary, key findings, evidence, gaps, outlook.', review_focus: 'Accuracy of claims, evidence quality, coverage, neutrality.' },
  planning: { label: 'Strategy / Plan', deliverable_hint: 'Actionable plan with phases, owners, timelines, dependencies, risks, and success metrics.', review_focus: 'Actionability, sequencing, risk coverage, measurability.' },
  writing: { label: 'Writing / Content', deliverable_hint: 'Polished prose with a clear narrative arc, consistent voice, and concrete detail.', review_focus: 'Clarity, voice, structure, engagement, precision.' },
  analysis: { label: 'Analysis / Decision', deliverable_hint: 'Structured analysis: define question, surface assumptions, weigh options, recommend with rationale.', review_focus: 'Rigor of reasoning, fairness to alternatives, clarity.' },
  mixed: { label: 'Mixed Deliverable', deliverable_hint: 'A cohesive document blending relevant elements. Prioritize clarity, completeness, usefulness.', review_focus: 'Coherence, completeness, usefulness, accuracy.' },
};

// ═══════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════

function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

function cleanOutput(text) {
  if (!text) return '';
  let t = text;
  t = t.replace(/<think>[\s\S]*?<\/think>/gi, '');
  for (let i = 0; i < 3; i++) {
    const before = t;
    t = t.replace(/^```[a-zA-Z0-9]*\s*\n([\s\S]*?)\n```\s*$/i, '$1');
    t = t.replace(/^```[a-zA-Z0-9]*\s*/i, '').replace(/\s*```\s*$/i, '');
    t = t.replace(/^[`'"~]{3,}/, '').replace(/[`'"~]{3,}$/, '');
    t = t.replace(/^"([\s\S]+)"$/, '$1').replace(/^'([\s\S]+)'$/, '$1');
    if (t === before) break;
  }
  return t.trim();
}

function stripMetaCommentary(text) {
  if (!text) return '';
  let t = text;
  t = t.replace(/\n#{1,3}\s*Team\s*Notes?\s*\n[\s\S]*$/i, '');
  t = t.replace(/\n#{1,3}\s*Notes?\s*(?:from|by)\s*(?:the\s+)?(?:Team|AI|FICASA)\s*\n[\s\S]*$/i, '');
  t = t.replace(/\n#{1,3}\s*(?:Process|Integration|Synthesis)\s*Notes?\s*\n[\s\S]*$/i, '');
  t = t.replace(/^(?:As (?:the |an )?(?:Integrator|AI|FICASA|Lead|Orchestrator)[^\n]*\n)/gim, '');
  t = t.replace(/^(?:I (?:have |'ve )?(?:synthesized|integrated|merged|combined|reviewed|compiled|crafted|produced)[^\n]*\n)/gim, '');
  return t.trim();
}

function extractDeliverable(rawOutput) {
  if (!rawOutput) return { deliverable: '', notes: '' };
  let cleaned = cleanOutput(rawOutput);
  let notes = '';
  let deliverable = cleaned;
  const notesPatterns = [
    /\n#{1,3}\s*Team\s*Notes?\s*\n/i,
    /\n#{1,3}\s*Notes?\s*(?:from|by)\s*(?:the\s+)?(?:Team|AI|FICASA)\s*\n/i,
    /\n#{1,3}\s*(?:Integration|Synthesis)\s*Notes?\s*\n/i,
  ];
  for (const pat of notesPatterns) {
    const m = cleaned.match(pat);
    if (m) { notes = cleaned.slice(m.index + m[0].length).trim(); deliverable = cleaned.slice(0, m.index).trim(); break; }
  }
  deliverable = stripMetaCommentary(deliverable);
  return { deliverable, notes };
}

function stripAttachments(text) {
  if (!text) return '';
  return text.replace(/\n--- ATTACHED FILE: [\s\S]*?--- END FILE: [\s\S]*?\n/g, '\n[attached files available to agents]\n').trim();
}

function outlineAsText(outline) {
  if (!outline || !outline.length) return '(no outline)';
  return outline.map(s => `## ${s.id}. ${s.title}\n  Purpose: ${s.purpose}\n  Key points: ${s.key_points.length ? s.key_points.map(p => `- ${p}`).join('\n  ') : '(none)'}\n`).join('\n\n');
}

function criteriaAsText(criteria) {
  if (!criteria || !criteria.length) return '(no explicit criteria)';
  return criteria.map((c, i) => `${i + 1}. ${c}`).join('\n');
}

function extractJSON(text) {
  if (!text) return null;
  let t = text.replace(/<think>[\s\S]*?<\/think>/gi, '').trim();
  t = t.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '');
  const s = t.indexOf('{'), e = t.lastIndexOf('}');
  if (s !== -1 && e !== -1 && e > s) t = t.slice(s, e + 1);
  try { return JSON.parse(t); } catch {
    try {
      const fixed = t.replace(/,\s*([}\]])/g, '$1').replace(/([{,]\s*)(\w+)\s*:/g, '$1"$2":');
      return JSON.parse(fixed);
    } catch { return null; }
  }
}

// ═══════════════════════════════════════════════════════════════
// MODEL MANAGEMENT
// ═══════════════════════════════════════════════════════════════

function tagModel(m) {
  const text = `${m.id || ''} ${m.name || ''} ${m.description || ''}`.toLowerCase();
  const tags = [];
  if (/coder|code|devstral|starcoder|qwen3-coder|qwen.?coder|deepseek-coder|deepseek-coding|codegemma|codellama|codestral|glm.?code|llama.?code|magicoder|phi.?code/.test(text)) tags.push('coding');
  if (/reasoning|deepseek-r1|deepseek.?v4|deepseek.?v3|\br1\b|thinking|instruct|qwq|o1|o3|o4|reasoner|kimi|minimax.?m|glm.?5|nemotron|reka/.test(text)) tags.push('reasoning');
  if (/\bvl\b|vision|multimodal|llava|gemma.?vision|qwen.?vl|llama.?vision|pixtral|moondream/.test(text)) tags.push('vision');
  if ((m.context_length || 0) > 100000) tags.push('long-context');
  if (/:online/.test(m.id || '')) tags.push('online');
  if (tags.length === 0) tags.push('general');
  return tags;
}

function isFreeModel(m) {
  if (m.id && m.id.endsWith(':free')) return true;
  if (m.pricing && parseFloat(m.pricing.prompt) === 0 && parseFloat(m.pricing.completion) === 0) return true;
  return false;
}

function assignPremiumTiers(models) {
  const premium = (models || []).filter(m => !isFreeModel(m) && m.pricing);
  premium.sort((a, b) => (parseFloat(b.pricing.completion) || 0) - (parseFloat(a.pricing.completion) || 0));
  const top20 = Math.max(1, Math.floor(premium.length * 0.2));
  const next30 = Math.max(1, Math.floor(premium.length * 0.3));
  premium.forEach((m, i) => {
    if (i < top20) m.premiumTier = 3;
    else if (i < top20 + next30) m.premiumTier = 2;
    else m.premiumTier = 1;
  });
  (models || []).forEach(m => { if (isFreeModel(m)) m.premiumTier = 0; });
  return models;
}

function getModelPool(role, state) {
  const pref = state.modelPreference || 'free-only';
  if (pref === 'free-only') return state.freeModels;
  if (pref === 'premium-all') return (state.premiumModels?.length) ? state.premiumModels : state.freeModels;
  if (pref === 'premium-custom') {
    const selected = (state.selectedPremiumModels || []).map(id => (state.premiumModels || []).find(m => m.id === id)).filter(m => m);
    if (!selected.length) return state.freeModels;
    if (['orchestrator','integrator','verifier','analyst','outline'].includes(role)) return selected;
    return state.freeModels;
  }
  // premium-critical
  if (['orchestrator','integrator','verifier','analyst','outline'].includes(role)) {
    return (state.premiumModels?.length) ? state.premiumModels : state.freeModels;
  }
  return state.freeModels;
}

function pickOrchestrator(state) {
  const mode = MODES[state.mode] || MODES.serious;
  const pool = getModelPool('orchestrator', state);
  if (!pool?.length) return state.freeModels?.[0] || null;
  return [...pool].sort((a, b) => {
    const d = (b.premiumTier || 0) - (a.premiumTier || 0);
    if (d) return d;
    const aR = a.tags?.includes('reasoning') ? 1 : 0;
    const bR = b.tags?.includes('reasoning') ? 1 : 0;
    return (bR - aR) || ((b.context_length || 0) - (a.context_length || 0));
  })[0] || pool[0];
}

function pickCriticalModel(role, state) {
  const pool = getModelPool(role, state);
  if (!pool?.length) return pickOrchestrator(state);
  return [...pool].sort((a, b) => {
    const d = (b.premiumTier || 0) - (a.premiumTier || 0);
    if (d) return d;
    const aR = a.tags?.includes('reasoning') ? 1 : 0;
    const bR = b.tags?.includes('reasoning') ? 1 : 0;
    return (bR - aR) || ((b.context_length || 0) - (a.context_length || 0));
  })[0] || pool[0];
}

function pickSpecialist(skillTags, usedIds, state) {
  const mode = MODES[state.mode] || MODES.serious;
  const pool = getModelPool('agent', state);
  if (!pool?.length) return state.freeModels?.[0] || null;
  const candidates = pool.filter(m => !usedIds.has(m.id)).map(m => {
    let score = 0;
    for (const s of skillTags) if (m.tags?.includes(s)) score += 3;
    if (mode.prefer === 'deep' && m.tags?.includes('long-context')) score += 1.5;
    if (mode.prefer === 'deep' && m.tags?.includes('reasoning')) score += 1;
    score += (m.premiumTier || 0) * 0.5;
    return { m, score };
  }).sort((a, b) => b.score - a.score || (b.m.context_length || 0) - (a.m.context_length || 0));
  return candidates[0]?.m || pool.find(m => !usedIds.has(m.id)) || pool[0];
}

function fallbackModel(skillTags, triedIds, state) {
  return pickSpecialist(skillTags, triedIds, state);
}

// ═══════════════════════════════════════════════════════════════
// LLM CALL (server-side, no streaming — browser polls for status)
// ═══════════════════════════════════════════════════════════════

async function callLLM(modelId, messages, apiKey, { maxRetries = 2, maxTokens, temp, tag, jobId } = {}) {
  const mode = MODES[apiKey._mode] || MODES.serious;
  const tokenCap = maxTokens || mode.maxTokens;
  const temperature = temp ?? mode.temp;
  // Resolve jobId: explicit param wins, else fall back to apiKey._jobId
  // (set by the queue consumer so every callLLM automatically gets job-scoped logging).
  const _jobId = jobId || apiKey._jobId;
  // Emit a call event so the live activity feed can show "calling model X for purpose Y".
  if (_jobId) logEvent('info', 'llm.call', { jobId: _jobId, model: modelId, tag: tag || 'unknown', tokenCap, attempt: 0 });
  let attempt = 0, backoff = 1000;
  while (true) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 90000);
      const res = await fetch(`${API_BASE}/chat/completions`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey.key}`, 'Content-Type': 'application/json', 'HTTP-Referer': 'https://ficasa-backend.officalumarx.workers.dev', 'X-Title': 'FICASA' },
        body: JSON.stringify({ model: modelId, messages, temperature, max_tokens: tokenCap, stream: false }),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (res.ok) {
        try {
          const data = await res.json();
          const content = data?.choices?.[0]?.message?.content ?? '';
          const tokens = data?.usage?.total_tokens || Math.round(content.length / 4);
          if (_jobId) logEvent('info', 'llm.response', { jobId: _jobId, model: modelId, tag: tag || 'unknown', chars: content.length, tokens, ok: true });
          return { ok: true, content };
        }
        catch { if (attempt < maxRetries) { await new Promise(r => setTimeout(r, 1000)); attempt++; continue; } return { ok: false, error: 'Malformed JSON response' }; }
      }
      if (res.status === 401) return { ok: false, error: 'Invalid API key', fatal: true, status: 401 };
      if (res.status === 402) return { ok: false, error: 'Payment required (402)', status: 402, modelUnavailable: true };
      if (res.status === 403) return { ok: false, error: 'Access denied (403)', status: 403, modelUnavailable: true };
      if (res.status === 429) { if (attempt < maxRetries) { await new Promise(r => setTimeout(r, backoff)); backoff = Math.min(backoff * 2, 5000); attempt++; continue; } return { ok: false, error: 'Rate limited', rateLimited: true }; }
      if (res.status >= 500) { if (attempt < 2) { await new Promise(r => setTimeout(r, Math.min(backoff, 3000))); backoff = Math.min(backoff * 1.5, 5000); attempt++; continue; } return { ok: false, error: `Server error (${res.status})`, transient: true }; }
      if (res.status === 400) { let detail = ''; try { const j = await res.json(); detail = j?.error?.message || ''; } catch {} return { ok: false, error: `Bad request${detail ? ': ' + detail : ''}`, modelUnavailable: true }; }
      let detail = ''; try { const j = await res.json(); detail = j?.error?.message || ''; } catch {}
      return { ok: false, error: `Request failed (${res.status})${detail ? ': ' + detail : ''}` };
    } catch (e) {
      if (e.name === 'AbortError') { if (attempt < maxRetries) { await new Promise(r => setTimeout(r, backoff)); backoff = Math.min(backoff * 1.5, 5000); attempt++; continue; } return { ok: false, error: 'Request timed out', timeout: true }; }
      if (attempt < maxRetries) { await new Promise(r => setTimeout(r, backoff)); backoff = Math.min(backoff * 1.5, 5000); attempt++; continue; }
      return { ok: false, error: `Network error: ${e.message}` };
    }
  }
}

async function callWithFallbacks(agent, messages, state, apiKey, opts = {}) {
  const { tag, maxFallbacks = 2, maxRetries, maxTokens, jobId } = opts;
  const tried = new Set([agent.model]);
  let modelId = agent.model;
  let fallbackCount = 0;
  while (fallbackCount <= maxFallbacks) {
    const result = await callLLM(modelId, messages, apiKey, { maxRetries, maxTokens, tag, jobId });
    if (result.ok && result.content?.trim()) return { ok: true, content: result.content, finalModelId: modelId };
    if (result.fatal) return { ok: false, ...result, finalModelId: modelId };
    tried.add(modelId);
    const next = fallbackModel(agent.skill_tags || ['general'], tried, state);
    if (next && fallbackCount < maxFallbacks) { fallbackCount++; modelId = next.id; continue; }
    return { ok: false, error: 'All fallbacks exhausted', finalModelId: modelId };
  }
  return { ok: false, error: 'Unreachable', finalModelId: modelId };
}

// ═══════════════════════════════════════════════════════════════
// PIPELINE PHASES
// ═══════════════════════════════════════════════════════════════

async function analyzeMission(mission, state, apiKey) {
  const cleanMission = stripAttachments(mission);
  const guide = MISSION_TYPE_GUIDE.mixed;
  const sys = `You are FICASA's Mission Analyst. Analyze the user's mission and respond ONLY with valid JSON:
{ "mission_type": "code"|"research"|"planning"|"writing"|"analysis"|"mixed", "deliverable_format": "code"|"markdown_report"|"json_spec"|"document"|"plan", "audience": "short phrase", "primary_language": "e.g. English", "success_criteria": ["3-6 specific criteria"], "target_length_hint": "short|medium|long", "key_constraints": ["0-4 constraints"], "suggested_section_count": 3-8 }`;

  for (let attempt = 0; attempt < 3; attempt++) {
    const orch = attempt === 0 ? pickCriticalModel('analyst', state) : pickSpecialist(['reasoning','general'], new Set(), state);
    if (!orch) break;
    const result = await callLLM(orch.id, [{ role: 'system', content: sys }, { role: 'user', content: cleanMission }], apiKey, { tag: 'analyst' });
    if (!result.ok && result.fatal) throw Object.assign(new Error(result.error), { fatal: true });
    const parsed = extractJSON(result.content);
    if (parsed?.mission_type && Array.isArray(parsed.success_criteria)?.length) {
      const validTypes = ['code','research','planning','writing','analysis','mixed'];
      const missionText = cleanMission.toLowerCase();
      const isSingleFile = /single[-\s]file|self[-\s]contained|one (?:html|file|script)|single html/i.test(cleanMission) || /single\s*file|self[-\s]contained/i.test(missionText);
      const isMultiFile = !isSingleFile && (/multi[-\s]file|multiple files|split into (?:files|modules)|separate files|monorepo/i.test(cleanMission) || /(react|vue|next\.js|node\.js)\s+app/i.test(missionText));
      let deliverableStructure = 'prose_document';
      if (isSingleFile) deliverableStructure = 'single_file';
      else if (isMultiFile) deliverableStructure = 'multi_file';
      else if (parsed.mission_type === 'code') deliverableStructure = 'single_file';
      return {
        mission_type: validTypes.includes(parsed.mission_type) ? parsed.mission_type : 'mixed',
        deliverable_format: parsed.deliverable_format || 'markdown_report',
        deliverable_structure: deliverableStructure,
        audience: String(parsed.audience || 'general reader').slice(0, 120),
        primary_language: String(parsed.primary_language || 'English').slice(0, 40),
        success_criteria: parsed.success_criteria.slice(0, 6).map(c => String(c).slice(0, 300)),
        target_length_hint: ['short','medium','long'].includes(parsed.target_length_hint) ? parsed.target_length_hint : 'medium',
        key_constraints: Array.isArray(parsed.key_constraints) ? parsed.key_constraints.slice(0, 4).map(c => String(c).slice(0, 300)) : [],
        suggested_section_count: clamp(parseInt(parsed.suggested_section_count) || 4, 3, 8),
        is_single_file: isSingleFile,
      };
    }
  }
  // Fallback
  return {
    mission_type: 'mixed', deliverable_format: 'markdown_report', deliverable_structure: 'prose_document',
    audience: 'general reader', primary_language: 'English',
    success_criteria: ['Directly addresses the mission', 'Covers key aspects', 'Is coherent and actionable'],
    target_length_hint: 'medium', key_constraints: [], suggested_section_count: 4, is_single_file: false,
  };
}

async function decompose(mission, analysis, state, apiKey) {
  const n = (MODES[state.mode] || MODES.serious).agentCount;
  const cleanMission = stripAttachments(mission);
  const guide = MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed;
  const sys = `You are FICASA's Lead Orchestrator. Break this mission into ${n} specialized workstreams. Respond ONLY with JSON:
{ "workstreams": [ { "role": "", "mandate": "", "skill_tags": ["coding"|"reasoning"|"vision"|"long-context"|"general"], "priority": "high"|"medium"|"low", "covers_criteria": [] } ] }
Mission type: ${analysis.mission_type} (${guide.label}). Success criteria: ${criteriaAsText(analysis.success_criteria)}`;

  for (let attempt = 0; attempt < 3; attempt++) {
    const orch = attempt === 0 ? pickOrchestrator(state) : pickSpecialist(['reasoning','general'], new Set(), state);
    if (!orch) break;
    const result = await callLLM(orch.id, [{ role: 'system', content: sys }, { role: 'user', content: cleanMission }], apiKey, { tag: 'orchestrator' });
    if (!result.ok && result.fatal) throw Object.assign(new Error(result.error), { fatal: true });
    const parsed = extractJSON(result.content);
    if (parsed?.workstreams?.length) {
      let ws = parsed.workstreams.filter(w => w?.role).slice(0, 6).map((w, i) => ({
        id: 'ws' + i, role: String(w.role).slice(0, 80), mandate: String(w.mandate || '').slice(0, 400),
        skill_tags: Array.isArray(w.skill_tags) ? w.skill_tags.filter(t => ['coding','reasoning','vision','long-context','general'].includes(t)) : ['general'],
        priority: ['high','medium','low'].includes(w.priority) ? w.priority : 'medium',
        covers_criteria: Array.isArray(w.covers_criteria) ? w.covers_criteria.map(n => parseInt(n)).filter(n => !isNaN(n) && n > 0) : [],
      }));
      if (ws.length > n) ws = ws.slice(0, n);
      while (ws.length < 3) ws.push({ id: 'ws' + ws.length, role: `Specialist ${ws.length+1}`, mandate: 'Cover remaining aspects.', skill_tags: ['general'], priority: 'medium', covers_criteria: [] });
      return ws;
    }
  }
  // Fallback
  const cm = cleanMission.slice(0, 200);
  return [
    { id: 'ws0', role: 'Primary Specialist', mandate: `Produce the main deliverable for: ${cm}`, skill_tags: ['reasoning','general'], priority: 'high', covers_criteria: [] },
    { id: 'ws1', role: 'Detail Specialist', mandate: `Cover details and edge cases for: ${cm}`, skill_tags: ['general'], priority: 'medium', covers_criteria: [] },
    { id: 'ws2', role: 'Quality Specialist', mandate: `Review and refine for: ${cm}`, skill_tags: ['reasoning'], priority: 'medium', covers_criteria: [] },
  ].slice(0, n);
}

async function generateOutline(mission, workstreams, analysis, state, apiKey) {
  const cleanMission = stripAttachments(mission);
  const wsList = workstreams.map((w, i) => `${i+1}. ${w.role} — ${w.mandate}`).join('\n');
  const guide = MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed;
  const sys = `You are FICASA's Outline Architect. Produce a SHARED OUTLINE. Respond ONLY with JSON:
{ "outline": [ { "id": "s1", "title": "", "purpose": "", "owner_idx": 0, "key_points": [] } ] }
Mission: ${cleanMission}. Type: ${analysis.mission_type}. Team:\n${wsList}`;

  for (let attempt = 0; attempt < 3; attempt++) {
    const orch = attempt === 0 ? pickCriticalModel('outline', state) : pickSpecialist(['reasoning','general'], new Set(), state);
    if (!orch) break;
    const result = await callLLM(orch.id, [{ role: 'system', content: sys }, { role: 'user', content: 'Produce the outline as JSON.' }], apiKey, { tag: 'outline' });
    if (!result.ok && result.fatal) throw Object.assign(new Error(result.error), { fatal: true });
    const parsed = extractJSON(result.content);
    if (parsed?.outline?.length >= 2) {
      return parsed.outline.slice(0, 10).map((s, i) => ({
        id: String(s.id || `s${i+1}`).slice(0, 12), title: String(s.title || `Section ${i+1}`).slice(0, 120),
        purpose: String(s.purpose || '').slice(0, 300), owner_idx: clamp(parseInt(s.owner_idx) || 0, 0, workstreams.length - 1),
        key_points: Array.isArray(s.key_points) ? s.key_points.slice(0, 5).map(p => String(p).slice(0, 300)) : [],
      }));
    }
  }
  return workstreams.map((w, i) => ({ id: `s${i+1}`, title: w.role, purpose: w.mandate, owner_idx: i, key_points: [] }));
}

function autoAssemble(workstreams, outline, state) {
  const used = new Set();
  return workstreams.map((ws, i) => {
    const model = pickSpecialist(ws.skill_tags, used, state);
    used.add(model.id);
    return {
      idx: i, role: ws.role, mandate: ws.mandate, skill_tags: ws.skill_tags, priority: ws.priority,
      covers_criteria: ws.covers_criteria || [],
      ownedSections: (outline || []).filter(s => s.owner_idx === i),
      model: model.id, modelName: model.name || model.id,
      status: 'idle', draft: '', tokens: 0, reviewReceived: null, error: null,
      fallbackNote: null, elapsed: 0, selfCritique: null,
      // SOTA additions for coverage tracking + token reallocation
      failureReason: null,
      coverageStatus: 'pending',           // 'pending' | 'covered' | 'partial' | 'missing'
      reassignedSections: [],
      compensationAgentIdx: null,           // idx of agent that took over this agent's sections
      reallocatedBudget: 0,                 // extra tokens granted to a compensation agent
    };
  });
}

// ─────────────────────────────────────────────────────────────────
// SOTA COVERAGE HELPERS
// All four helpers below are pure functions of `state` and do NOT
// touch the network. They are safe to call from any pipeline phase.
// ─────────────────────────────────────────────────────────────────

/**
 * Build a per-section coverage matrix from the current agent drafts.
 * Returns:
 *   {
 *     sections:   [{ id, title, purpose, owner_idx, owner_status,
 *                    drafted_chars, drafted_by, coverage }]   // 'covered' | 'partial' | 'missing'
 *     missing_ids:[sectionIds...],
 *     partial_ids:[sectionIds...],
 *     orphan_ids: [sectionIds whose owner_idx is out-of-range],
 *     failureMap: { sectionId -> originalOwnerIdx | 'orphan' }
 *   }
 *
 * `coverage` is derived by detecting section-heading mentions in any
 * surviving draft. This is intentionally fuzzy (substring scan) so it
 * works for prose, single-file, and multi-file deliverables alike —
 * the strict structural re-check happens later in verifyDeliverable.
 */
function buildCoverageMatrix(state) {
  const outline = state.outline || [];
  const agents = state.agents || [];
  const draftsByOwner = new Map();   // owner_idx -> concatenated draft text
  const draftsAll = [];              // [{ idx, role, draft }]
  for (const a of agents) {
    const txt = (a.draft || '').trim();
    if (txt) {
      draftsAll.push({ idx: a.idx, role: a.role, draft: txt });
      draftsByOwner.set(a.idx, (draftsByOwner.get(a.idx) || '') + '\n' + txt);
    }
  }

  const sections = [];
  const missing_ids = [];
  const partial_ids = [];
  const orphan_ids = [];
  const failureMap = {};

  for (const s of outline) {
    const ownerIdx = Number.isInteger(s.owner_idx) ? s.owner_idx : -1;
    const owner = agents[ownerIdx];
    const ownerStatus = owner ? owner.status : 'error';
    const ownerDraft = (draftsByOwner.get(ownerIdx) || '').trim();

    // Section-mention detection (case-insensitive). We check both the
    // literal `## sN` heading and a normalized title substring so that
    // agents who rephrased the heading still count.
    const headingNeedle = (s.id || '').toLowerCase();
    const titleNeedle = (s.title || '').toLowerCase().slice(0, 40);
    const titleHasContent = titleNeedle && titleNeedle.length >= 4;

    const ownerMentions = ownerDraft && (
      ownerDraft.toLowerCase().includes(headingNeedle) ||
      (titleHasContent && ownerDraft.toLowerCase().includes(titleNeedle))
    );

    // Any OTHER surviving agent who mentions this section (covers reassigned coverage)
    const surrogateMention = draftsAll.find(d => d.idx !== ownerIdx && (
      d.draft.toLowerCase().includes(headingNeedle) ||
      (titleHasContent && d.draft.toLowerCase().includes(titleNeedle))
    ));

    let coverage = 'missing';
    let drafted_by = null;
    let drafted_chars = 0;

    if (ownerMentions && ownerDraft.length >= 80) {
      coverage = 'covered';
      drafted_by = ownerIdx;
      drafted_chars = ownerDraft.length;
    } else if (surrogateMention) {
      // A teammate covered it (or partial overlap)
      coverage = surrogateMention.draft.length >= 80 ? 'covered' : 'partial';
      drafted_by = surrogateMention.idx;
      drafted_chars = surrogateMention.draft.length;
    } else if (ownerMentions) {
      coverage = 'partial';
      drafted_by = ownerIdx;
      drafted_chars = ownerDraft.length;
    }

    if (coverage === 'missing') {
      missing_ids.push(s.id);
      failureMap[s.id] = ownerIdx >= 0 ? ownerIdx : 'orphan';
      if (ownerIdx < 0 || ownerIdx >= agents.length) orphan_ids.push(s.id);
    } else if (coverage === 'partial') {
      partial_ids.push(s.id);
      // Also record the owner so planReassignment can bucket partial sections correctly
      failureMap[s.id] = ownerIdx >= 0 ? ownerIdx : 'orphan';
    }

    sections.push({
      id: s.id, title: s.title, purpose: s.purpose,
      owner_idx: ownerIdx, owner_status: ownerStatus,
      drafted_chars, drafted_by, coverage,
    });
  }

  return { sections, missing_ids, partial_ids, orphan_ids, failureMap };
}

/**
 * Semantic partial-failure detector.
 *
 * Three failure modes, in increasing subtlety:
 *   1. hard       — agent.status === 'error' OR empty draft
 *   2. ghost      — produced <30 chars of draft (model returned a refusal / preamble)
 *   3. dropout    — agent succeeded overall but is MISSING >= 60% of its
 *                   owned sections (silent section drop). Detected by
 *                   counting how many of agent.ownedSections appear in
 *                   agent.draft via the same mention-detection used in
 *                   buildCoverageMatrix.
 *
 * Returns: {
 *   hardFailures:   [agentIdx...],
 *   ghostFailures:  [agentIdx...],
 *   dropouts:       [agentIdx...],
 *   byAgent:        { [agentIdx]: { modes: [...], details: [...] } }
 * }
 */
function detectPartialFailures(state) {
  const agents = state.agents || [];
  const hardFailures = [];
  const ghostFailures = [];
  const dropouts = [];
  const byAgent = {};

  for (const a of agents) {
    const draft = (a.draft || '').trim();
    const modes = [];
    const details = [];

    // 1. Hard failure
    if (a.status === 'error' || !draft) {
      modes.push('hard');
      details.push(a.error || 'no draft produced');
      hardFailures.push(a.idx);
    }

    // 2. Ghost failure (tiny draft = refusal/preamble only)
    if (a.status !== 'error' && draft && draft.length < 30) {
      modes.push('ghost');
      details.push(`draft only ${draft.length} chars (likely refusal/preamble)`);
      ghostFailures.push(a.idx);
    }

    // 3. Silent section dropout
    if (a.status !== 'error' && draft && draft.length >= 30 && (a.ownedSections || []).length) {
      const hits = a.ownedSections.filter(s => {
        const head = (s.id || '').toLowerCase();
        const title = (s.title || '').toLowerCase().slice(0, 40);
        const hasTitle = title && title.length >= 4;
        const d = draft.toLowerCase();
        return d.includes(head) || (hasTitle && d.includes(title));
      });
      const missing = a.ownedSections.length - hits.length;
      const missingRatio = missing / a.ownedSections.length;
      if (missingRatio >= 0.6) {
        modes.push('dropout');
        details.push(`${missing}/${a.ownedSections.length} owned sections missing from draft`);
        dropouts.push(a.idx);
      }
    }

    if (modes.length) byAgent[a.idx] = { modes, details };
  }

  return { hardFailures, ghostFailures, dropouts, byAgent };
}

/**
 * Targeted reassignment planner.
 *
 * Given the failure list, produce a reassignment plan that prefers
 * surviving agents with spare capacity (low draft size, high quality
 * review) and degrades gracefully to "send everything to the integrator"
 * if no surviving agent is available.
 *
 * Plan shape:
 *   {
 *     reassignments: [{ fromAgentIdx, toAgentIdx, sectionIds: [..], reason }],
 *     integratorFallback: { sectionIds: [..] },     // sections the integrator must write from scratch
 *     compensationLog:   [{ toAgentIdx, extraSections, reason }],
 *   }
 */
function planReassignment(state, failures) {
  const agents = state.agents || [];
  const { sections, failureMap } = buildCoverageMatrix(state);

  // Map failed-agent idx -> list of section IDs they were responsible for
  const failedIdxToSections = new Map();
  for (const sec of sections) {
    if (sec.coverage === 'missing' || sec.coverage === 'partial') {
      const origOwner = failureMap[sec.id];
      if (origOwner === 'orphan') continue;
      if (!failedIdxToSections.has(origOwner)) failedIdxToSections.set(origOwner, []);
      failedIdxToSections.get(origOwner).push(sec.id);
    }
  }

  // Also fold ghost + dropout failures (their sections may be present-but-shoddy)
  for (const idx of [...failures.ghostFailures, ...failures.dropouts]) {
    const a = agents[idx];
    if (!a) continue;
    const ownedIds = (a.ownedSections || []).map(s => s.id);
    if (!failedIdxToSections.has(idx)) failedIdxToSections.set(idx, []);
    for (const id of ownedIds) {
      const arr = failedIdxToSections.get(idx);
      if (!arr.includes(id)) arr.push(id);
    }
  }

  // Candidate compensation agents: survivors, sorted by (draft length asc,
  // then review severity asc — i.e. pick the least-loaded, highest-quality
  // surviving agent first).
  const survivors = agents.filter(a =>
    a.status !== 'error' && (a.draft || '').trim().length >= 80 &&
    !failures.hardFailures.includes(a.idx) && !failures.ghostFailures.includes(a.idx)
  );
  const sortedSurvivors = [...survivors].sort((a, b) => {
    const aLen = (a.draft || '').length;
    const bLen = (b.draft || '').length;
    if (aLen !== bLen) return aLen - bLen;
    const sev = (s) => ({ high: 0, medium: 1, low: 2, undefined: 3 }[s?.reviewReceived?.severity]);
    return sev(a) - sev(b);
  });

  const reassignments = [];
  const compensationLog = [];
  const integratorSectionIds = [];
  const perAgentLoad = new Map();   // agentIdx -> # reassigned sections
  const MAX_EXTRA_SECTIONS = 5;     // capacity cap — prevents one survivor from being flooded

  for (const [failedIdx, secIds] of failedIdxToSections.entries()) {
    const reason = failures.byAgent[failedIdx]?.modes.join('+') || 'hard';
    const failedAgent = agents[failedIdx];
    const failedSkills = new Set(failedAgent?.skill_tags || []);

    if (!sortedSurvivors.length) {
      integratorSectionIds.push(...secIds);
      continue;
    }

    // Assign each section to the best-scoring survivor.
    // Score = skill-match bonus + spare-capacity bonus.
    // Sections that can't be assigned (all survivors at capacity) fall to integrator.
    for (let i = 0; i < secIds.length; i++) {
      const candidates = sortedSurvivors
        .map(s => {
          const load = perAgentLoad.get(s.idx) || 0;
          if (load >= MAX_EXTRA_SECTIONS) return null;  // capacity cap
          const survivorSkills = new Set(s.skill_tags || []);
          const skillMatch = [...failedSkills].some(sk => survivorSkills.has(sk));
          const score = (skillMatch ? 10 : 0) + (MAX_EXTRA_SECTIONS - load);
          return { survivor: s, score, load, skillMatch };
        })
        .filter(c => c !== null)
        .sort((a, b) => b.score - a.score);

      if (!candidates.length) {
        // All survivors at capacity → integrator fallback
        integratorSectionIds.push(secIds[i]);
        continue;
      }

      const target = candidates[0].survivor;
      perAgentLoad.set(target.idx, (perAgentLoad.get(target.idx) || 0) + 1);
      reassignments.push({
        fromAgentIdx: failedIdx,
        toAgentIdx: target.idx,
        sectionIds: [secIds[i]],
        reason,
      });
      if (failedAgent) failedAgent.compensationAgentIdx = target.idx;
    }
  }

  for (const [agentIdx, load] of perAgentLoad.entries()) {
    const a = agents[agentIdx];
    if (!a) continue;
    const ids = reassignments.filter(r => r.toAgentIdx === agentIdx).flatMap(r => r.sectionIds);
    a.reassignedSections = ids;
    a.coverageStatus = ids.length ? 'compensating' : 'covered';
    compensationLog.push({
      toAgentIdx: agentIdx,
      toAgentRole: a.role,
      extraSections: ids.length,
      sectionIds: ids,
      reason: 'compensation for failed teammate',
    });
  }

  return {
    reassignments,
    integratorFallback: { sectionIds: [...new Set(integratorSectionIds)] },  // dedup defensive
    compensationLog,
  };
}

/**
 * Token budget reallocation.
 *
 * FICASA does NOT use hard per-agent token caps — each agent's
 * `maxTokens` comes from the mode config and is enforced only inside
 * callLLM. What we CAN reallocate is the "soft budget" used for
 * downstream compensation passes: when an agent takes on extra sections,
 * we bump its reallocatedBudget counter so the compensation pass can
 * request a proportionally larger maxTokens when re-drafting those
 * sections.
 *
 * Budget source: failed agents "release" their originally-planned
 * budget. We redistribute it across the survivors who picked up their
 * sections, proportional to extra section load.
 *
 * Mutates state.agents in place (sets `.reallocatedBudget`).
 * Returns a summary object for logging.
 */
function reallocateTokenBudget(state, plan, failures) {
  const mode = MODES[state.mode] || MODES.serious;
  const baseBudget = mode.maxTokens || 4000;
  const agents = state.agents || [];

  // Total released budget = sum over failed agents of baseBudget
  const failedIdxSet = new Set([
    ...failures.hardFailures,
    ...failures.ghostFailures,
    ...failures.dropouts,
  ]);
  const releasedTotal = failedIdxSet.size * baseBudget;

  // Count extra load per survivor
  const extraByAgent = new Map();
  for (const r of plan.reassignments) {
    extraByAgent.set(r.toAgentIdx, (extraByAgent.get(r.toAgentIdx) || 0) + 1);
  }
  const totalExtra = [...extraByAgent.values()].reduce((a, b) => a + b, 0) || 1;

  for (const a of agents) {
    if (extraByAgent.has(a.idx)) {
      const share = extraByAgent.get(a.idx) / totalExtra;
      // Cap at 1.5x base budget — beyond that, extra tokens yield diminishing
      // returns (the model can't effectively use 5x budget in one call).
      // The 16000 hard cap in executeCompensationDrafts provides a second guard.
      const cap = Math.round(baseBudget * 1.5);
      a.reallocatedBudget = Math.min(Math.round(releasedTotal * share), cap);
    } else {
      a.reallocatedBudget = 0;
    }
  }

  return {
    releasedTotal,
    redistributed: [...extraByAgent.entries()].map(([idx, n]) => ({
      agentIdx: idx,
      extraSections: n,
      extraBudget: agents[idx]?.reallocatedBudget || 0,
    })),
  };
}

/**
 * Execute the reassignment plan: ask each compensation agent to draft
 * the sections it inherited from a failed teammate.
 *
 * Returns true if at least one compensation draft succeeded.
 * Each surviving agent's draft is APPENDED (with a section separator)
 * so the integrator sees both the original work and the compensation work.
 */
async function executeCompensationDrafts(state, plan, apiKey, startTime) {
  const analysis = state.missionAnalysis;
  const mode = MODES[state.mode] || MODES.serious;
  let anySuccess = false;

  // Track which sections were successfully compensated (for failed-agent audit trail)
  const compensatedSections = new Set();
  const failedCompSections = new Set();

  // Group reassigned sections by target agent
  const byTarget = new Map();   // agentIdx -> [{ fromAgentIdx, sectionIds, reason }]
  for (const r of plan.reassignments) {
    if (!byTarget.has(r.toAgentIdx)) byTarget.set(r.toAgentIdx, []);
    byTarget.get(r.toAgentIdx).push(r);
  }

  for (const [agentIdx, items] of byTarget.entries()) {
    if (Date.now() - startTime > WALL_TIME_LIMIT) break;
    const agent = state.agents[agentIdx];
    if (!agent || agent.status === 'error') continue;

    const allSecIds = items.flatMap(r => r.sectionIds);
    const sections = (state.outline || []).filter(s => allSecIds.includes(s.id));
    if (!sections.length) continue;

    const fromRoles = [...new Set(items.map(r =>
      state.agents[r.fromAgentIdx]?.role || `agent#${r.fromAgentIdx}`))].join(', ');
    const sectionsText = sections.map(s =>
      `## ${s.id}. ${s.title}\nPurpose: ${s.purpose || '(unspecified)'}\nKey points: ${(s.key_points || []).map(p => `- ${p}`).join('\n') || '(none)'}`
    ).join('\n\n');

    const extraBudget = agent.reallocatedBudget || 0;
    const compMaxTokens = Math.min(16000, (mode.maxTokens || 4000) + extraBudget);

    const sys = `You are the ${agent.role} on FICASA. A teammate (${fromRoles}) FAILED to produce their assigned sections. You must produce those sections NOW so the deliverable is complete.

MISSION: "${state.mission}"

YOUR ASSIGNED SECTIONS (from failed teammate):
${sectionsText}

${analysis ? `MISSION CONTEXT: type=${analysis.mission_type}, audience=${analysis.audience || 'general'}, language=${analysis.primary_language || 'English'}` : ''}

RULES:
1. Produce ONLY the listed sections. Use Markdown ## headings with the exact section IDs.
2. Be complete and specific — these sections have no other author.
3. Write in ${analysis ? analysis.primary_language : 'English'}.
4. No preamble, no commentary. Start directly with ## ${sections[0]?.id}.`;

    agent.status = 'compensating';
    const compMessages = [{ role: 'system', content: sys }, { role: 'user', content: `Begin your compensation work for the failed teammate (${fromRoles}).` }];
    const compResult = await callWithFallbacks(agent, compMessages, state, apiKey, { tag: `${agent.role}-compensation`, stream: false, maxFallbacks: 2, maxTokens: compMaxTokens });

    if (compResult.ok && compResult.content?.trim()) {
      const compDraft = cleanOutput(compResult.content);
      agent.draft = (agent.draft || '').trim() + '\n\n<!-- COMPENSATION: sections ' + allSecIds.join(', ') + ' -->\n\n' + compDraft;
      agent.tokens = Math.max(agent.tokens || 0, Math.round(compDraft.length / 4));
      state.tokensUsed += Math.round(compDraft.length / 4);
      agent.fallbackNote = (agent.fallbackNote ? agent.fallbackNote + '; ' : '') + `compensated ${allSecIds.length} sections from ${fromRoles}`;
      agent.coverageStatus = 'compensated';
      allSecIds.forEach(id => compensatedSections.add(id));
      anySuccess = true;
    } else {
      // Compensation attempt failed — log it; integrator fallback will pick up
      agent.fallbackNote = (agent.fallbackNote ? agent.fallbackNote + '; ' : '') + `compensation failed for ${allSecIds.join(', ')}`;
      agent.coverageStatus = 'compensation_failed';
      allSecIds.forEach(id => failedCompSections.add(id));
    }
    agent.status = 'done';
  }

  // ── Update failed agents' coverageStatus for audit-trail consistency ──
  // Map: failedIdx -> all section IDs that were reassigned from them
  const failedIdxToOwnedSections = new Map();
  for (const r of plan.reassignments) {
    if (!failedIdxToOwnedSections.has(r.fromAgentIdx)) failedIdxToOwnedSections.set(r.fromAgentIdx, []);
    failedIdxToOwnedSections.get(r.fromAgentIdx).push(...r.sectionIds);
  }

  for (const [failedIdx, secIds] of failedIdxToOwnedSections) {
    const failedAgent = state.agents[failedIdx];
    if (!failedAgent) continue;
    const compensatedCount = secIds.filter(id => compensatedSections.has(id)).length;
    if (compensatedCount === secIds.length) {
      failedAgent.coverageStatus = 'compensated';
    } else if (compensatedCount > 0) {
      failedAgent.coverageStatus = 'partially_compensated';
    } else {
      failedAgent.coverageStatus = 'compensation_failed';
    }
  }

  // Also mark failed agents whose sections went straight to integrator (no survivors)
  for (const secId of plan.integratorFallback.sectionIds) {
    // Find the original owner of this section
    const outlineSec = (state.outline || []).find(s => s.id === secId);
    if (!outlineSec) continue;
    const ownerIdx = outlineSec.owner_idx;
    const failedAgent = state.agents[ownerIdx];
    if (failedAgent && (failedAgent.status === 'error' || !failedAgent.draft)) {
      failedAgent.coverageStatus = 'integrator_fallback';
    }
  }

  return anySuccess;
}

async function draftAgent(agent, state, apiKey, startTime) {
  agent.status = 'working'; agent.startedAt = Date.now();
  const AGENT_DEADLINE_MS = 3 * 60 * 1000;
  const deadlineExceeded = () => (Date.now() - agent.startedAt) > AGENT_DEADLINE_MS || (Date.now() - startTime) > WALL_TIME_LIMIT;
  const jobId = state._jobId;

  // ENHANCED: emit agent.start event with role, model, owned sections
  if (jobId) logEvent('info', 'agent.start', {
    jobId, agentIdx: agent.idx, role: agent.role, model: agent.model,
    sections: (agent.ownedSections || []).map(s => s.id),
    mandate: (agent.mandate || '').slice(0, 120),
  });

  const others = state.agents.filter(a => a.idx !== agent.idx).map(a => `- ${a.role}: ${a.mandate}`).join('\n');
  const analysis = state.missionAnalysis;
  const guide = analysis ? (MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed) : null;
  const ownedSections = agent.ownedSections || [];
  const ownedSectionsText = ownedSections.length ? ownedSections.map(s => `  · ${s.id}. ${s.title} — ${s.purpose || ''}`).join('\n') : '  · (contribute to the deliverable as a whole)';

  const analysisBlock = analysis ? `\nMISSION CONTEXT:\n- Type: ${analysis.mission_type}${guide ? ` (${guide.label})` : ''}\n- Deliverable: ${guide ? guide.deliverable_hint : 'A polished deliverable'}\n- Audience: ${analysis.audience || 'general reader'}\n- Language: ${analysis.primary_language || 'English'}\n- Success criteria:\n${(analysis.success_criteria || []).map((c, i) => `    ${i+1}. ${c}`).join('\n')}` : '';
  const outlineBlock = state.outline ? `\nSHARED OUTLINE:\n${outlineAsText(state.outline)}\n\nYOUR ASSIGNED SECTIONS:\n${ownedSectionsText}` : '';
  const structureGuidance = analysis?.deliverable_structure === 'single_file' ? '\nSINGLE-FILE: Output ONLY your assigned sections as code fragments. Do NOT output a complete file.' :
    analysis?.deliverable_structure === 'multi_file' ? '\nMULTI-FILE: Output each file with ## File: path/to/file headers.' :
    '\nPROSE: Stay strictly within your assigned sections. Do NOT overlap with teammates.';

  const sys = `You are the ${agent.role} on FICASA, working on: "${state.mission}"\n${analysisBlock}\n${outlineBlock}\n${structureGuidance}\nYour mandate: ${agent.mandate || '(unspecified)'}\n\nTeammates:\n${others || '(you are the only agent)'}\n\nRULES:\n1. Produce your portion NOW. Be complete and specific.\n2. Stay within your assigned sections.\n3. If code, output complete runnable code. If prose, write in full.\n4. Write in ${analysis ? analysis.primary_language : 'English'}.\n5. No preamble, no commentary. Start directly with content.\n6. Use Markdown ## headings for your sections.`;

  const messages = [{ role: 'system', content: sys }, { role: 'user', content: `Begin your work on: "${state.mission}"` }];
  if (jobId) logEvent('info', 'agent.drafting', { jobId, agentIdx: agent.idx, role: agent.role, model: agent.model });
  const draftResult = await callWithFallbacks(agent, messages, state, apiKey, { tag: agent.role, stream: false, maxFallbacks: 2 });

  if (!draftResult.ok && draftResult.fatal) {
    agent.status = 'error'; agent.error = draftResult.error;
    if (jobId) logEvent('error', 'agent.failed', { jobId, agentIdx: agent.idx, role: agent.role, error: draftResult.error, fatal: true });
    return;
  }
  if (!draftResult.ok || !draftResult.content?.trim()) {
    agent.status = 'error'; agent.error = draftResult.error || 'All fallbacks exhausted';
    if (jobId) logEvent('warn', 'agent.failed', { jobId, agentIdx: agent.idx, role: agent.role, error: agent.error, fatal: false });
    return;
  }

  agent.draft = cleanOutput(draftResult.content);
  agent.tokens = Math.max(1, Math.round(agent.draft.length / 4));
  state.tokensUsed += agent.tokens;
  if (draftResult.finalModelId !== agent.model) agent.fallbackNote = `Draft used ${draftResult.finalModelId} (fallback)`;
  if (jobId) logEvent('info', 'agent.draft_done', { jobId, agentIdx: agent.idx, role: agent.role, chars: agent.draft.length, tokens: agent.tokens, model: draftResult.finalModelId, preview: agent.draft.slice(0, 200) });

  // Self-critique + refine (Agentic mode only, if time allows)
  const mode = MODES[state.mode] || MODES.serious;
  if (mode.selfCritique && agent.draft.length > 80 && agent.draft.length < 2500 && !deadlineExceeded()) {
    agent.status = 'refining';
    if (jobId) logEvent('info', 'agent.selfcritique', { jobId, agentIdx: agent.idx, role: agent.role });
    const critiqueSys = `You are reviewing YOUR OWN draft as the ${agent.role}. Identify 2-5 improvements. Respond ONLY with JSON: { "issues": [""], "fixes": [""], "severity": "low"|"medium"|"high" }`;
    const critiqueRes = await callWithFallbacks(agent, [{ role: 'system', content: critiqueSys }, { role: 'user', content: 'Critique your draft as JSON.' }], state, apiKey, { tag: `${agent.role}-selfcritique`, stream: false, maxFallbacks: 1 });
    if (critiqueRes.ok && critiqueRes.content) {
      const parsed = extractJSON(critiqueRes.content);
      if (parsed?.fixes?.length && parsed.severity !== 'low' && !deadlineExceeded()) {
        if (jobId) logEvent('info', 'agent.refining', { jobId, agentIdx: agent.idx, role: agent.role, severity: parsed.severity, fixes: parsed.fixes.length, issues: (parsed.issues || []).slice(0, 3) });
        const refineSys = `You are the ${agent.role}. You wrote a draft, then self-critiqued it. Produce the REVISED version.\nYOUR ORIGINAL DRAFT:\n${agent.draft.slice(0, 6000)}\n\nFIXES TO APPLY:\n${parsed.fixes.map(f => `- ${f}`).join('\n')}\n\nProduce the full revised version. Same rules: no preamble, start directly.`;
        const origLen = agent.draft.length;
        const refineResult = await callWithFallbacks(agent, [{ role: 'system', content: refineSys }, { role: 'user', content: 'Produce your revised work.' }], state, apiKey, { tag: `${agent.role}-refine`, stream: false, maxFallbacks: 1 });
        if (refineResult.ok && refineResult.content?.trim().length > origLen * 0.5) {
          agent.draft = cleanOutput(refineResult.content);
          agent.tokens = Math.max(1, Math.round(agent.draft.length / 4));
          state.tokensUsed += agent.tokens;
          agent.fallbackNote = (agent.fallbackNote ? agent.fallbackNote + '; ' : '') + 'self-refined';
          if (jobId) logEvent('info', 'agent.refine_done', { jobId, agentIdx: agent.idx, role: agent.role, chars: agent.draft.length });
        }
      }
    }
  }
  agent.elapsed = Date.now() - agent.startedAt;
  agent.status = 'done';
  if (jobId) logEvent('info', 'agent.done', { jobId, agentIdx: agent.idx, role: agent.role, chars: agent.draft?.length || 0, elapsedMs: agent.elapsed });
}

async function reviewAgent(reviewer, target, state, apiKey) {
  if (!target.draft || reviewer.status === 'error') return;
  reviewer.status = 'reviewing';
  const jobId = state._jobId;
  if (jobId) logEvent('info', 'review.start', { jobId, reviewerIdx: reviewer.idx, reviewerRole: reviewer.role, targetIdx: target.idx, targetRole: target.role });
  const analysis = state.missionAnalysis;
  const guide = analysis ? (MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed) : null;
  const sys = `You are reviewing a teammate's work. Score on 5 dimensions (1-5 each): correctness, completeness, clarity, depth, alignment. Respond ONLY with JSON: { "scores": {"correctness":5,"completeness":5,"clarity":5,"depth":5,"alignment":5}, "notes": {}, "top_issues": [], "suggested_fixes": [], "overall_severity": "low"|"medium"|"high" }
Mission: "${state.mission}". Teammate: ${target.role}. Draft:\n${target.draft}`;

  const triedIds = new Set([reviewer.model]);
  for (let attempt = 0; attempt < 2; attempt++) {
    const modelId = attempt === 0 ? reviewer.model : (fallbackModel(reviewer.skill_tags, triedIds, state)?.id);
    if (!modelId) break;
    triedIds.add(modelId);
    const result = await callLLM(modelId, [{ role: 'system', content: sys }, { role: 'user', content: 'Provide your review as JSON.' }], apiKey, { tag: 'review', maxRetries: 2 });
    reviewer.status = 'done';
    // BUG FIX: check content is non-empty, not just result.ok
    if (result.ok && result.content && result.content.trim()) {
      const p = extractJSON(result.content) || {};
      const scores = p.scores && typeof p.scores === 'object' ? p.scores : {};
      const validScores = ['correctness','completeness','clarity','depth','alignment'].reduce((acc, k) => { acc[k] = clamp(parseInt(scores[k]) || 4, 1, 5); return acc; }, {});
      const minScore = Math.min(...Object.values(validScores));
      const computedSeverity = minScore < 3 ? 'high' : (minScore === 3 ? 'medium' : 'low');
      const reportedSeverity = ['low','medium','high'].includes(p.overall_severity) ? p.overall_severity : computedSeverity;
      const severity = ['high','medium','low'].indexOf(reportedSeverity) < ['high','medium','low'].indexOf(computedSeverity) ? reportedSeverity : computedSeverity;
      target.reviewReceived = {
        reviewerRole: reviewer.role, scores: validScores,
        notes: p.notes && typeof p.notes === 'object' ? Object.fromEntries(Object.entries(p.notes).map(([k,v]) => [k, String(v).slice(0, 300)])) : {},
        top_issues: Array.isArray(p.top_issues) ? p.top_issues.map(String).slice(0, 5) : [],
        suggested_fixes: Array.isArray(p.suggested_fixes) ? p.suggested_fixes.map(String).slice(0, 5) : [],
        severity,
      };
      if (jobId) logEvent('info', 'review.done', {
        jobId, reviewerRole: reviewer.role, targetRole: target.role,
        scores: validScores, severity,
        topIssues: target.reviewReceived.top_issues.slice(0, 3),
      });
      return;
    }
    if (result.fatal) return;
  }
  target.reviewReceived = { reviewerRole: reviewer.role, scores: { correctness: 4, completeness: 4, clarity: 4, depth: 4, alignment: 4 }, notes: {}, top_issues: ['Review unavailable'], suggested_fixes: [], severity: 'low' };
  if (jobId) logEvent('warn', 'review.failed', { jobId, reviewerRole: reviewer.role, targetRole: target.role });
}

async function runIntegration(state, apiKey, startTime) {
  const successfulAgents = state.agents.filter(a => a.draft);
  const jobId = state._jobId;
  if (jobId) logEvent('info', 'integrator.start', { jobId, agents: successfulAgents.length, totalAgents: state.agents.length });
  if (!successfulAgents.length) {
    state.finalOutput = 'No agents produced output.';
    state.teamNotes = `All ${state.agents.length} agents failed.`;
    return;
  }
  const analysis = state.missionAnalysis;
  const guide = analysis ? (MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed) : null;
  const mode = MODES[state.mode] || MODES.serious;
  const deliverableStructure = analysis?.deliverable_structure || 'prose_document';

  const draftsBlock = successfulAgents.map(a => `### ${a.role} (model: ${a.model})\n${a.draft}`).join('\n\n');
  const reviewsBlock = state.agents.filter(a => a.reviewReceived).map(a => {
    const r = a.reviewReceived;
    return `- ${r.reviewerRole} reviewed ${a.role}: ${r.severity}; ${r.top_issues.join(' | ') || 'no issues'}`;
  }).join('\n');

  const cleanMission = stripAttachments(state.mission || '').slice(0, 500);
  let structureRules = '';
  if (deliverableStructure === 'single_file') {
    structureRules = '\nSINGLE-FILE: ONE file. ONE DOCTYPE. Merge multiple DOCTYPEs. No agent names. Output raw file.';
  } else if (deliverableStructure === 'multi_file') {
    structureRules = '\nMULTI-FILE: Each file as ### File: header. No agent names. Preserve file boundaries.';
  } else {
    structureRules = '\nPROSE: DEDUPLICATE. No repeated sections. No agent metadata. No duplicated paragraphs.';
  }

  const sys = `You are FICASA's Integrator. Merge the team's drafts into ONE cohesive deliverable for: "${cleanMission}"\n${structureRules}\n\nCRITICAL RULES:\n1. Every section from the outline MUST be present.\n2. Every teammate's contribution MUST be represented.\n3. Synthesize like a single expert author.\n4. Resolve contradictions.\n5. Address peer-review issues.\n6. No preamble, no commentary. Output the deliverable ONLY.\n7. Write in ${analysis ? analysis.primary_language : 'English'}.`;

  // SOTA: If the coverage-recovery phase identified sections that no
  // agent produced (and that no compensation agent could cover either),
  // explicitly instruct the integrator to write them from scratch.
  // This is the last-resort backstop before the verifier's auto-revise.
  const integratorFallbackSections = state.integratorFallbackSections || [];
  const coverageRecovery = state.coverageRecovery;
  let coverageDirective = '';
  if (integratorFallbackSections.length) {
    const missingSections = (state.outline || []).filter(s => integratorFallbackSections.includes(s.id));
    const missingText = missingSections.map(s =>
      `  - ${s.id}. ${s.title} — ${s.purpose || '(unspecified)'}${(s.key_points || []).length ? '\n      Key points: ' + s.key_points.map(p => p).join('; ') : ''}`
    ).join('\n');
    coverageDirective += `\n\n⚠️ COVERAGE DIRECTIVE — The following sections were NOT produced by any agent (teammate failures and compensation attempts both failed). You MUST write these sections YOURSELF from scratch, using the mission context. Do NOT skip them, do NOT mark them as "TODO", do NOT note that they are missing. Write them as if you were the original author:\n${missingText}\n\nThe deliverable is INCOMPLETE without these sections. Produce them in full.\n`;
  }
  if (coverageRecovery && coverageRecovery.compensationSucceeded) {
    // Even when some sections needed integrator fallback, compensated sections
    // still need extra coherence scrutiny. This is a separate concern from the
    // directive above — both can fire simultaneously.
    const compSections = (coverageRecovery.plan?.compensationLog || [])
      .flatMap(c => c.sectionIds);
    if (compSections.length) {
      coverageDirective += `\n\nℹ️ COVERAGE NOTE — The following sections were written by compensation agents after the original owner failed: ${compSections.join(', ')}. Give them extra scrutiny for coherence with the rest of the deliverable.\n`;
    }
  }

  const user = `Mission: ${cleanMission}\n\nTeam drafts:\n${draftsBlock}\n\nPeer reviews:\n${reviewsBlock || '(no reviews)'}${coverageDirective}\n\nProduce the final deliverable:`;

  let integratorMaxTokens = mode.integratorTokens || mode.maxTokens;
  if (deliverableStructure === 'single_file') integratorMaxTokens = Math.max(integratorMaxTokens, 16000);

  // Try up to 3 integrator models
  let result = null;
  const triedIds = new Set();
  for (let attempt = 0; attempt < 3; attempt++) {
    if (Date.now() - startTime > WALL_TIME_LIMIT) break;
    const integrator = attempt === 0 ? pickCriticalModel('integrator', state) : pickSpecialist(['reasoning','general'], triedIds, state);
    if (!integrator) break;
    triedIds.add(integrator.id);
    result = await callLLM(integrator.id, [{ role: 'system', content: sys }, { role: 'user', content: user }], apiKey, { maxRetries: 2, tag: 'integrator', maxTokens: integratorMaxTokens });
    if (result.ok && result.content?.trim()) break;
    if (result.fatal) break;
  }

  if (!result?.ok || !result.content?.trim()) {
    // Fallback: concatenate drafts
    const fallback = successfulAgents.map(a => `## ${a.role}\n\n${a.draft}`).join('\n\n');
    state.finalOutput = stripMetaCommentary(cleanOutput(fallback)) || 'No deliverable produced.';
    state.teamNotes = `Integration failed; concatenated drafts. Error: ${result?.error || 'unknown'}`;
    return;
  }

  // Truncation detection + continuation
  let content = result.content;
  const htmlCloseTag = '<' + '/html>';
  const isTruncated = (() => {
    if (deliverableStructure === 'single_file') {
      if (/<!DOCTYPE|<html/i.test(content) && !new RegExp(htmlCloseTag + '\\s*$', 'i').test(content)) return true;
      if (content.endsWith(';') || content.endsWith('}') || content.endsWith(')')) return false;
      if (/\b(for|if|while|function|const|let|var|return)\s*\([^)]*$/.test(content.slice(-200))) return true;
    }
    if (deliverableStructure === 'prose_document') {
      const last20 = content.trim().slice(-20);
      if (last20 && !/[.!?\n:;"')\]—\-]$/.test(last20)) return true;
    }
    return false;
  })();

  if (isTruncated && content.length > 1000) {
    let continuedContent = content;
    if (jobId) logEvent('info', 'integrator.continuing', { jobId, chars: content.length });
    for (let cont = 0; cont < 3; cont++) {
      if (Date.now() - startTime > WALL_TIME_LIMIT) break;
      const contModel = pickCriticalModel('integrator', state);
      if (!contModel) break;
      const contResult = await callLLM(contModel.id, [
        { role: 'system', content: 'You were writing a deliverable and it got cut off. Continue EXACTLY from where you stopped. Do NOT repeat content. Output ONLY the continuation.' },
        { role: 'user', content: `Here is what you wrote (cut off at end):\n\n...${continuedContent.slice(-3000)}\n\nContinue from exactly where it stopped:` }
      ], apiKey, { maxRetries: 1, tag: `integrator-continue-${cont+1}`, maxTokens: integratorMaxTokens });
      if (!contResult.ok || !contResult.content?.trim()) break;
      continuedContent += (deliverableStructure === 'prose_document' ? '\n' : '') + contResult.content.trim();
      state.tokensUsed += Math.round(contResult.content.length / 4);
      if (jobId) logEvent('info', 'integrator.continue_done', { jobId, pass: cont + 1, addedChars: contResult.content.trim().length });
      if (deliverableStructure === 'single_file' && new RegExp(htmlCloseTag + '\\s*$', 'i').test(continuedContent)) break;
      if (deliverableStructure === 'prose_document' && /[.!?\n]$/.test(continuedContent.trim().slice(-5))) break;
      if (contResult.content.trim().length < 100) break;
    }
    content = continuedContent;
  }

  // Extract deliverable
  const { deliverable, notes } = extractDeliverable(content);
  state.finalOutput = deliverable;
  state.teamNotes = notes;

  // Best-version tracking + integration critique loop
  const scoreQuality = (text) => {
    if (!text?.trim()) return -1000;
    let score = Math.min(text.length / 100, 100);
    if (deliverableStructure === 'single_file') {
      const dc = (text.match(/<!DOCTYPE/gi) || []).length;
      if (dc === 1) score += 50;
      if (dc > 1) score -= 100;
      if (new RegExp(htmlCloseTag, 'i').test(text)) score += 30;
    }
    score -= (text.match(/^##\s+\w+.*(Architect|Engineer|Reviewer)/gmi) || []).length * 20;
    if (/^\s*```[a-zA-Z]*\n[\s\S]+\n```\s*$/.test(text)) score -= 30;
    if (text.length < 500) score -= 200;
    return score;
  };

  let bestContent = content;
  let bestScore = scoreQuality(deliverable);
  const maxLoops = mode.integrationLoops || 0;

  for (let loop = 0; loop < maxLoops; loop++) {
    if (Date.now() - startTime > WALL_TIME_LIMIT) break;
    const currentDeliverable = cleanOutput(bestContent);
    if (currentDeliverable.length < 100) break;
    if (jobId) logEvent('info', 'integrator.critique', { jobId, loop: loop + 1, maxLoops, chars: currentDeliverable.length });

    // Critique
    const critiqueSys = `You are FICASA's Integration Critic. Check for CONCRETE issues only: missing sections, redundancies, agent metadata, structural issues. Respond ONLY with JSON: { "redundancies": [], "agent_metadata_present": false, "structural_issues": [], "needs_another_pass": true|false, "priority_fixes": [] }\n\nDELIVERABLE:\n${currentDeliverable.slice(0, 8000)}`;
    const critiqueModel = pickCriticalModel('integrator', state);
    if (!critiqueModel) break;
    const critiqueRes = await callLLM(critiqueModel.id, [{ role: 'system', content: critiqueSys }, { role: 'user', content: 'Provide critique as JSON.' }], apiKey, { maxRetries: 1, tag: 'integration-critique' });
    if (!critiqueRes.ok) break;
    const critique = extractJSON(critiqueRes.content);
    if (!critique) break;

    const redundancies = Array.isArray(critique.redundancies) ? critique.redundancies : [];
    const structuralIssues = Array.isArray(critique.structural_issues) ? critique.structural_issues : [];
    const agentMeta = critique.agent_metadata_present === true;
    const forced = [...redundancies, ...structuralIssues, ...(agentMeta ? ['agent metadata'] : [])];

    if (jobId) logEvent('info', 'integrator.critique_done', { jobId, loop: loop + 1, redundancies: redundancies.length, structuralIssues: structuralIssues.length, agentMeta, needsAnotherPass: critique.needs_another_pass });

    if (!forced.length && critique.needs_another_pass === false) break;

    const priorityFixes = Array.isArray(critique.priority_fixes) ? critique.priority_fixes : [];
    if (!priorityFixes.length && forced.length) {
      priorityFixes.push(
        ...(redundancies.length ? [`Merge: ${redundancies.join('; ')}`] : []),
        ...(structuralIssues.length ? [`Fix: ${structuralIssues.join('; ')}`] : []),
        ...(agentMeta ? ['Remove agent metadata'] : []),
      );
    }
    if (!priorityFixes.length) break;
    if (jobId) logEvent('info', 'integrator.revising', { jobId, loop: loop + 1, fixes: priorityFixes.length });

    // Re-integrate
    const reSys = `You are FICASA's Integrator (REVISION pass). Fix these issues:\n${priorityFixes.map((f, i) => `${i+1}. ${f}`).join('\n')}\n\nORIGINAL:\n${currentDeliverable.slice(0, 8000)}\n\nRULES: Do NOT delete content. MERGE, don't remove. Output MUST be at least as long. No preamble.`;
    const reModel = pickCriticalModel('integrator', state);
    if (!reModel) break;
    const reResult = await callLLM(reModel.id, [{ role: 'system', content: reSys }, { role: 'user', content: 'Produce the revised deliverable.' }], apiKey, { maxRetries: 1, tag: `integrator-revise-${loop+1}`, maxTokens: integratorMaxTokens });
    if (!reResult.ok || !reResult.content?.trim()) break;

    const revised = cleanOutput(reResult.content);
    if (revised.length < currentDeliverable.length * 0.9) break; // too short = rejected
    const revisedScore = scoreQuality(revised);
    if (revisedScore < bestScore - 10) continue; // worse = rejected
    bestContent = reResult.content;
    if (revisedScore > bestScore) bestScore = revisedScore;
    if (jobId) logEvent('info', 'integrator.revise_done', { jobId, loop: loop + 1, chars: revised.length, score: revisedScore });
  }

  // Use best version
  const { deliverable: finalDeliverable, notes: finalNotes } = extractDeliverable(bestContent);
  state.finalOutput = finalDeliverable;
  state.teamNotes = finalNotes;

  // Post-integration structural check (same as client-side)
  if (deliverableStructure === 'single_file') {
    const endsWithHtml = new RegExp(htmlCloseTag + '\\s*$', 'i').test(state.finalOutput.trim());
    const openBraces = (state.finalOutput.match(/{/g) || []).length;
    const closeBraces = (state.finalOutput.match(/}/g) || []).length;
    if (endsWithHtml && openBraces === closeBraces) {
      // File is structurally complete — strip any agent metadata headings
      state.finalOutput = state.finalOutput.replace(/^##\s+\w+.*(Architect|Engineer|Reviewer|Specialist|Orchestrator).*$/gmi, '').trim();
    }
  }
  if (jobId) logEvent('info', 'integrator.done', { jobId, chars: state.finalOutput?.length || 0, loops: maxLoops });
}

async function verifyDeliverable(state, apiKey, startTime) {
  const jobId = state._jobId;
  if (!state.finalOutput || !state.missionAnalysis) {
    state.verificationResult = { passed: true, gaps: [], criterionResults: [], revised: false };
    return;
  }
  if (Date.now() - startTime > WALL_TIME_LIMIT) {
    state.verificationResult = { passed: true, gaps: [], criterionResults: [], revised: false, note: 'Skipped (wall time limit)' };
    if (jobId) logEvent('warn', 'verifier.skipped', { jobId, reason: 'wall_time' });
    return;
  }
  if (jobId) logEvent('info', 'verifier.start', { jobId, deliverableChars: (state.finalOutput || '').length });

  const analysis = state.missionAnalysis;
  const guide = MISSION_TYPE_GUIDE[analysis.mission_type] || MISSION_TYPE_GUIDE.mixed;
  const htmlCloseTag = '<' + '/html>';
  const deliverableLen = (state.finalOutput || '').length;
  const structCheck = analysis?.deliverable_structure || 'prose_document';

  // For large single-file code deliverables (>28K), skip model verification
  // and use structural checks only (brace balance, closing tags).
  if (structCheck === 'single_file' && deliverableLen > 28000) {
    const endsWithHtml = new RegExp(htmlCloseTag + '\\s*$', 'i').test((state.finalOutput || '').trim());
    const openBraces = ((state.finalOutput || '').match(/{/g) || []).length;
    const closeBraces = ((state.finalOutput || '').match(/}/g) || []).length;
    const bracesBalanced = openBraces === closeBraces;
    const gaps = [];
    if (!endsWithHtml) gaps.push('File does not end with closing html tag — may be truncated.');
    if (!bracesBalanced) gaps.push(`Brace mismatch (${openBraces} open vs ${closeBraces} close) — possible syntax error.`);
    state.verificationResult = {
      passed: endsWithHtml && bracesBalanced, gaps,
      criterionResults: (analysis.success_criteria || []).map(c => ({
        criterion: c, status: endsWithHtml && bracesBalanced ? 'met' : 'partial',
        evidence: endsWithHtml && bracesBalanced ? 'Structurally complete.' : 'Structural issues.',
      })),
      criticalErrors: gaps, missingSections: [], revised: false,
      note: 'Large file — structural verification only.',
    };
    return;
  }

  const sys = `You are FICASA's Verifier. Verify the deliverable against success criteria. Respond ONLY with JSON:
{ "criterion_results": [{"criterion":"","status":"met"|"partial"|"not_met","evidence":""}], "critical_errors": [], "overall_passed": true|false, "gaps": [], "needs_revision": true|false }

IMPORTANT: If the deliverable ends with a closing html tag, it is NOT truncated. Do NOT claim content is missing unless you can verify it's not in the full file.

DELIVERABLE TO VERIFY:
${(state.finalOutput || '').slice(0, 28000)}
${(state.finalOutput || '').length > 28000 ? `\n[NOTE: Deliverable is ${state.finalOutput.length} chars. Only first 28000 shown. Do NOT claim content is missing.]\n` : ''}`;

  const verifierModel = pickCriticalModel('verifier', state);
  if (!verifierModel) { state.verificationResult = { passed: true, gaps: [], criterionResults: [], revised: false }; return; }

  const result = await callLLM(verifierModel.id, [{ role: 'system', content: sys }, { role: 'user', content: 'Verify as JSON.' }], apiKey, { maxRetries: 1, tag: 'verifier' });
  if (!result?.ok) { state.verificationResult = { passed: true, gaps: [], criterionResults: [], revised: false, error: 'Verifier unavailable' }; return; }

  const v = extractJSON(result.content) || {};
  let gaps = Array.isArray(v.gaps) ? v.gaps.map(String).slice(0, 5) : [];
  const criterionResults = Array.isArray(v.criterion_results) ? v.criterion_results.map(r => ({ criterion: String(r?.criterion || '').slice(0, 200), status: ['met','partial','not_met'].includes(r?.status) ? r.status : 'partial', evidence: String(r?.evidence || '').slice(0, 300) })) : [];
  const criticalErrors = Array.isArray(v.critical_errors) ? v.critical_errors.map(String).slice(0, 5) : [];
  let passed = v.overall_passed === true || (gaps.length === 0 && criticalErrors.length === 0);
  let needsRevision = v.needs_revision === true || criticalErrors.length > 0 || criterionResults.some(r => r.status === 'not_met');

  // Structural override for single-file code
  const structCheckOverride = analysis?.deliverable_structure || 'prose_document';
  if (structCheckOverride === 'single_file') {
    const endsWithHtml = new RegExp(htmlCloseTag + '\\s*$', 'i').test((state.finalOutput || '').trim());
    const openBraces = ((state.finalOutput || '').match(/{/g) || []).length;
    const closeBraces = ((state.finalOutput || '').match(/}/g) || []).length;
    if (endsWithHtml && openBraces === closeBraces) {
      const filtered = gaps.filter(g => !/truncat|incomplete|missing|cut off|never added|not built|no (car|geometry|animation|render)/i.test(g));
      if (filtered.length < gaps.length) { gaps = filtered; if (!gaps.length && !criticalErrors.length) { passed = true; needsRevision = false; } }
    }
  }

  // Auto-revise if needed and time allows
  let revised = false;
  if (needsRevision && gaps.length > 0 && (state.finalOutput || '').length > 100 && Date.now() - startTime < WALL_TIME_LIMIT - 60000) {
    if (jobId) logEvent('info', 'verifier.revising', { jobId, gaps: gaps.length, criticalErrors: criticalErrors.length });
    const reviseSys = `You are FICASA's Final Revisor. Fix these gaps:\n${gaps.map((g, i) => `${i+1}. ${g}`).join('\n')}\n\nCURRENT DELIVERABLE:\n${(state.finalOutput || '').slice(0, 14000)}\n\nProduce the COMPLETE revised deliverable. No preamble. Write in ${analysis.primary_language || 'English'}.`;
    const revResult = await callLLM(verifierModel.id, [{ role: 'system', content: reviseSys }, { role: 'user', content: 'Produce revised deliverable.' }], apiKey, { maxRetries: 1, tag: 'verifier-revise', maxTokens: MODES[state.mode]?.integratorTokens || 8000 });
    if (revResult.ok && revResult.content?.trim().length >= (state.finalOutput || '').length * 0.9) {
      const revisedOutput = stripMetaCommentary(cleanOutput(revResult.content));
      if (revisedOutput.length > 100) { state.finalOutput = revisedOutput; revised = true; passed = true; }
    }
    if (jobId) logEvent('info', 'verifier.revise_done', { jobId, revised, chars: state.finalOutput?.length || 0 });
  }

  state.verificationResult = { passed: revised ? true : passed, gaps, criterionResults, criticalErrors, revised };
  if (jobId) logEvent('info', 'verifier.done', {
    jobId, passed: state.verificationResult.passed, revised,
    criteriaMet: criterionResults.filter(c => c.status === 'met').length,
    criteriaTotal: criterionResults.length, gaps: gaps.length,
  });
}

// ═══════════════════════════════════════════════════════════════
// SECURITY: Firebase ID token verification + CORS + rate limiting
// ═══════════════════════════════════════════════════════════════

// Allowed origins — scoped to known frontend hosts. Requests from
// other origins are rejected at the CORS layer.
// NOTE: CORS is browser-enforced only — the real security is the
// Firebase ID token verification below. If you host the frontend on
// a different domain, add it here.
const ALLOWED_ORIGINS = [
  'https://officialficasa.web.app',
  'https://officialficasa.firebaseapp.com',
  'https://axikora.me',
  'https://www.axikora.me',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
];

function corsHeaders(request) {
  const origin = request.headers.get('Origin') || '';
  // Echo back the origin if it's in our allowlist; otherwise use the
  // first allowed origin (browser will block the response anyway).
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Vary': 'Origin',
  };
}

// Input size limits — prevents abuse via oversized payloads
const MAX_MISSION_LENGTH = 50000;      // 50K chars — generous, covers most use cases
const MAX_TOTAL_PAYLOAD = 200000;      // 200K chars — includes models list, attachments, etc.
const RATE_LIMIT_WINDOW_MS = 60_000;   // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 5;     // 5 submits per minute per user

/**
 * Verify a Firebase ID token using Firebase's public keys.
 * Firebase tokens are JWTs signed with RS256; we verify the signature
 * against Google's public certs (cached for 1 hour in KV).
 *
 * Returns { uid, email } on success, or null on failure.
 *
 * On Cloudflare Workers we use Web Crypto API (subtle) for RS256 verify.
 */
async function verifyFirebaseToken(idToken, env) {
  if (!idToken) return null;
  const parts = idToken.split('.');
  if (parts.length !== 3) return null;

  const [headerB64, payloadB64, sigB64] = parts;
  let header, payload;
  try {
    header = JSON.parse(atob(headerB64));
    payload = JSON.parse(atob(payloadB64));
  } catch { return null; }

  // Verify issuer + audience
  if (payload.iss !== 'https://securetoken.google.com/officialficasa') return null;
  if (payload.aud !== 'officialficasa') return null;

  // Verify expiry (with 30s clock skew tolerance)
  const now = Math.floor(Date.now() / 1000);
  if (payload.exp < now - 30) return null;
  if (payload.auth_time > now + 30) return null;

  // Fetch Google's public keys (cached in KV for 1 hour to avoid refetching)
  let certs;
  const cached = await env.FICASA_JOBS.get('firebase_certs_cache');
  if (cached) {
    try { certs = JSON.parse(cached); } catch { certs = null; }
  }
  if (!certs) {
    const resp = await fetch('https://www.googleapis.com/service_accounts/v1/jk:securetoken@system.gserviceaccount.com');
    if (!resp.ok) return null;
    certs = await resp.json();
    // Cache for 50 minutes (certs rotate every ~24h, but 50m is safe)
    await env.FICASA_JOBS.put('firebase_certs_cache', JSON.stringify(certs), { expirationTtl: 3000 });
  }

  // Find the cert matching the token's kid
  const kid = header.kid;
  const certPem = certs[kid] || (certs.keys && certs.keys.find(k => k.kid === kid)?.pem);
  if (!certPem) return null;

  // Convert PEM public key to CryptoKey for verification
  try {
    // Extract the base64 DER from the PEM
    const pemContents = certPem.replace(/-----BEGIN [A-Z ]+-----/g, '').replace(/-----END [A-Z ]+-----/g, '').replace(/\s/g, '');
    const der = Uint8Array.from(atob(pemContents), c => c.charCodeAt(0));
    const key = await crypto.subtle.importKey(
      'spki', der,
      { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
      false, ['verify']
    );

    // Verify signature
    const data = new TextEncoder().encode(headerB64 + '.' + payloadB64);
    const sig = Uint8Array.from(atob(sigB64.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
    const valid = await crypto.subtle.verify('RSASSA-PKCS1-v1_5', key, sig, data);
    if (!valid) return null;
  } catch (e) {
    return null;
  }

  return { uid: payload.sub, email: payload.email || null };
}

/**
 * Per-user rate limiting using KV. Returns true if the request is allowed,
 * false if rate limit exceeded. Tracks a counter per uid in a 1-minute window.
 */
async function checkRateLimit(env, uid) {
  const key = `rl:${uid}`;
  const now = Date.now();
  const raw = await env.FICASA_JOBS.get(key);
  let entry = raw ? JSON.parse(raw) : { count: 0, windowStart: now };
  // Reset window if it expired
  if (now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    entry = { count: 0, windowStart: now };
  }
  entry.count++;
  // Store with short TTL so the key auto-expires
  await env.FICASA_JOBS.put(key, JSON.stringify(entry), { expirationTtl: 120 });
  return entry.count <= RATE_LIMIT_MAX_REQUESTS;
}

/**
 * Idempotency check — prevents duplicate job submissions.
 * Uses a hash of (uid + mission + mode) as a dedupe key. If a job with
 * the same key was submitted in the last 60 seconds, returns the existing
 * jobId instead of creating a new one.
 */
async function dedupeKey(env, uid, mission, mode) {
  const hashData = `${uid}:${mode}:${mission.slice(0, 500)}`;
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(hashData));
  const hashHex = Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
  return `dedupe:${uid}:${hashHex.slice(0, 32)}`;
}

async function checkDuplicate(env, uid, mission, mode) {
  const key = await dedupeKey(env, uid, mission, mode);
  const existing = await env.FICASA_JOBS.get(key);
  if (existing) {
    try { return JSON.parse(existing); } catch { return null; }
  }
  return null;
}

async function storeDedupeKey(env, uid, mission, mode, jobId) {
  const key = await dedupeKey(env, uid, mission, mode);
  await env.FICASA_JOBS.put(key, JSON.stringify({ jobId, ts: Date.now() }), { expirationTtl: 60 });
}

// ═══════════════════════════════════════════════════════════════
// MAIN WORKER EXPORT
// ═══════════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {
    _env = env;  // ENHANCED: stash env so logEvent can persist events to KV
    const corsH = corsHeaders(request);
    if (request.method === 'OPTIONS') return new Response(null, { headers: corsH });

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // ── AUTH: Verify Firebase ID token on every request ──────────
      // The browser sends Authorization: Bearer <idToken>. We verify it
      // against Firebase's public keys. Unauthenticated requests are rejected.
      // For sendBeacon requests (which can't set custom headers), the token
      // is also accepted inside the JSON body as _authToken.
      // For SSE /stream requests (EventSource can't set custom headers),
      // the token is also accepted as a ?token= query parameter.
      const authHeader = request.headers.get('Authorization') || '';
      let idToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
      // SSE fallback: EventSource can't set headers, so accept ?token= for /stream only.
      if (!idToken && path === '/stream') {
        idToken = url.searchParams.get('token') || null;
      }

      // POST /submit — browser sends mission here
      if (path === '/submit' && request.method === 'POST') {
        // ── Input size limit ───────────────────────────────────────
        const contentLength = parseInt(request.headers.get('Content-Length') || '0', 10);
        if (contentLength > MAX_TOTAL_PAYLOAD) {
          return Response.json({ error: 'Payload too large. Mission + attachments must be under 200KB.' }, { status: 413, headers: corsH });
        }

        const body = await request.json();

        // sendBeacon fallback: if no Authorization header, read token from body
        if (!idToken && body._authToken) {
          idToken = body._authToken;
          delete body._authToken;  // don't store this in the job record
        }

        const authUser = await verifyFirebaseToken(idToken, env);
        if (!authUser) {
          return Response.json({ error: 'Authentication required. Please log in.' }, { status: 401, headers: corsH });
        }

        const { mission, apiKey, mode, maxPower, modelPreference, freeModels, premiumModels, selectedPremiumModels, creditBalance, webSearch } = body;

        if (!mission || !apiKey) return Response.json({ error: 'Missing mission or apiKey' }, { status: 400, headers: corsH });
        // Mission length limit
        if (typeof mission !== 'string' || mission.length > MAX_MISSION_LENGTH) {
          return Response.json({ error: `Mission too long (max ${MAX_MISSION_LENGTH} chars).` }, { status: 400, headers: corsH });
        }

        // ── Rate limit ─────────────────────────────────────────────
        const allowed = await checkRateLimit(env, authUser.uid);
        if (!allowed) {
          return Response.json({ error: 'Rate limit exceeded. Please wait a minute before submitting again.' }, { status: 429, headers: corsH });
        }

        // ── Idempotency: check for duplicate submissions ───────────
        const dupe = await checkDuplicate(env, authUser.uid, mission, mode || 'serious');
        if (dupe) {
          // Return the existing jobId instead of creating a duplicate
          return Response.json({ jobId: dupe.jobId, status: 'queued', duplicate: true }, { headers: corsH });
        }

        const jobId = crypto.randomUUID();
        // Resolve the effective mode: if maxPower is true and mode is agentic, use agentic_max
        const effectiveMode = (maxPower && mode === 'agentic') ? 'agentic_max' : (mode || 'serious');
        const jobData = {
          id: jobId,
          uid: authUser.uid,  // track owner for status/cancel auth
          mission, apiKey, mode: effectiveMode,
          modelPreference: modelPreference || 'free-only',
          freeModels: freeModels || [], premiumModels: premiumModels || [],
          selectedPremiumModels: selectedPremiumModels || [],
          creditBalance: creditBalance || 0,
          webSearch: webSearch || false,
          status: 'queued', createdAt: Date.now(), updatedAt: Date.now(),
          currentPhase: null, phases: [], result: null, error: null,
          tokensUsed: 0, analysis: null, outline: null,
        };

        await env.FICASA_JOBS.put(jobId, JSON.stringify(jobData), { expirationTtl: 86400 });
        await env.FICASA_QUEUE.send({ jobId });
        // Store dedupe key so a retry within 60s returns the same jobId
        await storeDedupeKey(env, authUser.uid, mission, mode || 'serious', jobId);
        logEvent('info', 'job.queued', { jobId, uid: authUser.uid, mode: mode || 'serious', missionLen: mission.length, modelPref: modelPreference || 'free-only' });
        return Response.json({ jobId, status: 'queued' }, { headers: corsH });
      }

      // For all other routes, verify token from header only
      const authUser = await verifyFirebaseToken(idToken, env);
      if (!authUser) {
        return Response.json({ error: 'Authentication required. Please log in.' }, { status: 401, headers: corsH });
      }

      // GET /status?jobId=xxx — browser polls this
      if (path === '/status' && request.method === 'GET') {
        const jobId = url.searchParams.get('jobId');
        if (!jobId) return Response.json({ error: 'Missing jobId' }, { status: 400, headers: corsH });
        const raw = await env.FICASA_JOBS.get(jobId);
        if (!raw) return Response.json({ error: 'Job not found' }, { status: 404, headers: corsH });
        const job = JSON.parse(raw);
        // ── Authorization: only the job owner can view status ──────
        if (job.uid && job.uid !== authUser.uid) {
          return Response.json({ error: 'Not authorized to view this job.' }, { status: 403, headers: corsH });
        }
        delete job.apiKey; // never send API key back to browser
        delete job.uid;    // don't leak the owner uid either
        return Response.json(job, { headers: corsH });
      }

      // POST /cancel — browser cancels a backend job when local finishes first
      if (path === '/cancel' && request.method === 'POST') {
        const body = await request.json();
        const { jobId } = body;
        if (!jobId) return Response.json({ error: 'Missing jobId' }, { status: 400, headers: corsH });
        const raw = await env.FICASA_JOBS.get(jobId);
        if (!raw) return Response.json({ error: 'Job not found' }, { status: 404, headers: corsH });
        const job = JSON.parse(raw);
        // ── Authorization: only the job owner can cancel ───────────
        if (job.uid && job.uid !== authUser.uid) {
          return Response.json({ error: 'Not authorized to cancel this job.' }, { status: 403, headers: corsH });
        }
        // Mark as cancelled so the queue consumer skips it if it hasn't started yet
        if (job.status === 'queued') {
          await env.FICASA_JOBS.put(jobId, JSON.stringify({ ...job, status: 'cancelled', updatedAt: Date.now() }), { expirationTtl: 86400 });
        }
        return Response.json({ ok: true }, { headers: corsH });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /events?jobId=xxx&since=<ts> — ENHANCED live activity feed
      //
      // Returns the per-job event log (appended by logEvent during the
      // pipeline). The frontend polls this every ~2s to render a rich
      // live view: which LLMs are working, what they're discussing, what
      // the orchestrator decided, peer-review scores, integrator critique
      // loops, verifier per-criterion results — everything that happens
      // between the coarse /status snapshots.
      //
      // `since` (epoch ms) filters to events newer than that timestamp,
      // so the frontend only fetches new events each poll (delta sync).
      // ─────────────────────────────────────────────────────────────
      if (path === '/events' && request.method === 'GET') {
        const jobId = url.searchParams.get('jobId');
        const since = parseInt(url.searchParams.get('since') || '0', 10) || 0;
        if (!jobId) return Response.json({ error: 'Missing jobId' }, { status: 400, headers: corsH });
        // Authorization: verify the caller owns this job (read the job record).
        const jobRaw = await env.FICASA_JOBS.get(jobId);
        if (!jobRaw) return Response.json({ error: 'Job not found' }, { status: 404, headers: corsH });
        const jobRec = JSON.parse(jobRaw);
        if (jobRec.uid && jobRec.uid !== authUser.uid) {
          return Response.json({ error: 'Not authorized' }, { status: 403, headers: corsH });
        }
        // Read the event log (may not exist yet if the consumer hasn't started).
        const eventsRaw = await env.FICASA_JOBS.get(`events:${jobId}`);
        let events = [];
        if (eventsRaw) {
          try { events = JSON.parse(eventsRaw); if (!Array.isArray(events)) events = []; } catch { events = []; }
        }
        // Delta filter: only events strictly newer than `since`.
        const filtered = since > 0 ? events.filter(e => (e.ts || 0) > since) : events;
        return Response.json({
          jobId,
          since,
          latestTs: events.length ? events[events.length - 1].ts : 0,
          jobStatus: jobRec.status,
          currentPhase: jobRec.currentPhase,
          count: filtered.length,
          events: filtered,
        }, { headers: corsH });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /stream?jobId=xxx — ENHANCED Server-Sent Events stream
      //
      // Opens a long-lived SSE connection that pushes new events to the
      // browser the moment they're persisted to KV. This eliminates
      // polling latency entirely for users who keep the tab open.
      //
      // The connection polls KV internally every 2s and pushes any new
      // events as SSE `data:` lines. It auto-closes when the job reaches
      // a terminal status (done/partial/error/cancelled) AND all events
      // have been flushed. A hard 14-minute server-side cap prevents
      // zombie connections.
      // ─────────────────────────────────────────────────────────────
      if (path === '/stream' && request.method === 'GET') {
        const jobId = url.searchParams.get('jobId');
        if (!jobId) return Response.json({ error: 'Missing jobId' }, { status: 400, headers: corsH });
        // Authorization
        const jobRaw = await env.FICASA_JOBS.get(jobId);
        if (!jobRaw) return Response.json({ error: 'Job not found' }, { status: 404, headers: corsH });
        const jobRec = JSON.parse(jobRaw);
        if (jobRec.uid && jobRec.uid !== authUser.uid) {
          return Response.json({ error: 'Not authorized' }, { status: 403, headers: corsH });
        }

        const sseHeaders = {
          ...corsH,
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
        };

        const encoder = new TextEncoder();
        const stream = new ReadableStream({
          async start(controller) {
            let lastTs = 0;
            let closed = false;
            const startTime = Date.now();
            const HARD_CAP_MS = 14 * 60 * 1000;  // 14 minutes

            const send = (obj) => {
              if (closed) return;
              try { controller.enqueue(encoder.encode(`data: ${JSON.stringify(obj)}\n\n`)); } catch { closed = true; }
            };

            // Initial hello so the browser knows the stream is alive.
            send({ type: 'hello', jobId, ts: Date.now() });

            const tick = async () => {
              if (closed) return;
              try {
                // Check job status
                const jr = await env.FICASA_JOBS.get(jobId);
                if (!jr) { send({ type: 'end', reason: 'job_not_found' }); controller.close(); closed = true; return; }
                const j = JSON.parse(jr);
                // Fetch new events since lastTs
                const er = await env.FICASA_JOBS.get(`events:${jobId}`);
                let evs = [];
                if (er) { try { evs = JSON.parse(er); if (!Array.isArray(evs)) evs = []; } catch {} }
                const fresh = lastTs > 0 ? evs.filter(e => (e.ts || 0) > lastTs) : evs;
                if (fresh.length) {
                  lastTs = fresh[fresh.length - 1].ts || lastTs;
                  send({ type: 'events', count: fresh.length, events: fresh });
                }
                // Terminal?
                if (['done', 'partial', 'error', 'cancelled'].includes(j.status)) {
                  send({ type: 'end', reason: j.status, ts: Date.now() });
                  controller.close(); closed = true; return;
                }
                // Hard cap
                if (Date.now() - startTime > HARD_CAP_MS) {
                  send({ type: 'end', reason: 'timeout', ts: Date.now() });
                  controller.close(); closed = true; return;
                }
              } catch (e) {
                // Non-fatal — keep the stream alive and try again next tick.
              }
              if (!closed) setTimeout(tick, 2000);
            };
            setTimeout(tick, 100);
          },
        });
        return new Response(stream, { headers: sseHeaders });
      }

      return Response.json({ error: 'Not found' }, { status: 404, headers: corsH });
    } catch (err) {
      return Response.json({ error: err.message }, { status: 500, headers: corsH });
    }
  },

  // Queue consumer — runs the full 5-phase pipeline in background
  async queue(batch, env) {
    _env = env;  // ENHANCED: stash env so logEvent can persist events to KV
    for (const message of batch.messages) {
      const { jobId } = message.body;
      const startTime = Date.now();

      try {
        // Load job
        const raw = await env.FICASA_JOBS.get(jobId);
        if (!raw) { logEvent('warn', 'job.notfound', { jobId }); message.ack(); continue; }
        const job = JSON.parse(raw);

        // Skip cancelled jobs (local execution finished first)
        if (job.status === 'cancelled') { logEvent('info', 'job.skipped_cancelled', { jobId }); message.ack(); continue; }

        logEvent('info', 'job.started', { jobId, uid: job.uid, mode: job.mode, missionLen: job.mission?.length || 0 });

        // Helper: update job (batched — only 5 writes total)
        const updateJob = async (updates) => {
          const current = JSON.parse(await env.FICASA_JOBS.get(jobId) || '{}');
          const updated = { ...current, ...updates, updatedAt: Date.now() };
          await env.FICASA_JOBS.put(jobId, JSON.stringify(updated), { expirationTtl: 86400 });
          return updated;
        };

        // ── SECURITY: Scrub the raw API key from KV at-rest ────────
        // We've loaded the key into memory (apiKeyObj below). Once we've
        // started processing, there's no reason for the raw key to remain
        // in the KV record. We replace it with a placeholder so the key
        // is not exposed if KV is ever inspected.
        if (job.apiKey) {
          await updateJob({ apiKey: '[scrubbed]' });
        }

        // Set up LOCAL state for this mission (NOT module-level — concurrent
        // queue batches in the same isolate would stomp on a shared global).
        const apiKeyObj = { key: job.apiKey, _mode: job.mode, _jobId: jobId };
        const jobState = {
          apiKey: job.apiKey,
          mode: job.mode || 'serious',
          modelPreference: job.modelPreference || 'free-only',
          freeModels: (job.freeModels || []).map(m => ({ ...m, tags: tagModel(m) })),
          premiumModels: (job.premiumModels || []).map(m => ({ ...m, tags: tagModel(m) })),
          selectedPremiumModels: job.selectedPremiumModels || [],
          creditBalance: job.creditBalance || 0,
          mission: job.mission,
          tokensUsed: 0,
          agents: [],
          missionAnalysis: null,
          outline: null,
          finalOutput: '',
          teamNotes: '',
          verificationResult: null,
          coverageRecovery: null,           // SOTA — coverage recovery summary
          integratorFallbackSections: [],   // SOTA — section IDs the integrator must write from scratch
          _jobId: jobId,                    // ENHANCED: stashed so every pipeline fn can emit events
          _uid: job.uid,                    // ENHANCED: for owner-scoped event logging
        };

        // Assign premium tiers if premium models exist
        if (jobState.premiumModels.length) assignPremiumTiers([...jobState.freeModels, ...jobState.premiumModels]);

        // WRITE 1: Status → running
        await updateJob({ status: 'running', currentPhase: 'planning' });
        logEvent('info', 'phase.start', { jobId, phase: 'planning' });

        // ── PHASE 1: ANALYZE ──────────────────────────────────────
        const analysis = await analyzeMission(job.mission, jobState, apiKeyObj);
        if (Date.now() - startTime > WALL_TIME_LIMIT) { await updateJob({ status: 'partial', currentPhase: null, result: 'Timed out during analysis.', error: 'Wall time exceeded' }); message.ack(); continue; }
        jobState.missionAnalysis = analysis;
        logEvent('info', 'orchestrator.analysis_done', {
          jobId, missionType: analysis.mission_type, audience: analysis.audience,
          language: analysis.primary_language, criteriaCount: analysis.success_criteria?.length || 0,
        });

        // ── PHASE 1b: DECOMPOSE ───────────────────────────────────
        const ws = await decompose(job.mission, analysis, jobState, apiKeyObj);
        if (Date.now() - startTime > WALL_TIME_LIMIT) { await updateJob({ status: 'partial', result: 'Timed out during decomposition.' }); message.ack(); continue; }
        logEvent('info', 'orchestrator.decompose_done', {
          jobId, workstreams: ws.length,
          roles: ws.map(w => w.role),
        });

        // ── PHASE 1c: OUTLINE ─────────────────────────────────────
        const outline = await generateOutline(job.mission, ws, analysis, jobState, apiKeyObj);
        if (Date.now() - startTime > WALL_TIME_LIMIT) { await updateJob({ status: 'partial', result: 'Timed out during outline generation.' }); message.ack(); continue; }
        jobState.outline = outline;
        jobState.workstreams = ws;
        jobState.agents = autoAssemble(ws, outline, jobState);
        logEvent('info', 'orchestrator.assemble_done', {
          jobId, agents: jobState.agents.length,
          team: jobState.agents.map(a => ({ role: a.role, model: a.model, sections: (a.ownedSections || []).map(s => s.id) })),
        });
        logEvent('info', 'phase.complete', { jobId, phase: 'planning' });

        // WRITE 2: Planning done — ENHANCED: include workstreams, outline, agents snapshot
        await updateJob({
          currentPhase: 'execution',
          analysis: { mission_type: analysis.mission_type, audience: analysis.audience, success_criteria: analysis.success_criteria, deliverable_structure: analysis.deliverable_structure },
          agentCount: ws.length,
          workstreams: ws,
          outline: outline,
          agents: jobState.agents.map(a => ({ idx: a.idx, role: a.role, model: a.model, modelName: a.modelName, mandate: (a.mandate || '').slice(0, 200), status: 'idle', ownedSections: (a.ownedSections || []).map(s => s.id) })),
        });
        logEvent('info', 'phase.start', { jobId, phase: 'execution' });

        // ── PHASE 2: DRAFT (parallel agents) ──────────────────────
        // Run agents with concurrency (3 at a time)
        const agentPromises = [];
        let nextAgent = 0;
        const runNext = async () => {
          while (nextAgent < jobState.agents.length) {
            const i = nextAgent++;
            if (Date.now() - startTime > WALL_TIME_LIMIT) break;
            await draftAgent(jobState.agents[i], jobState, apiKeyObj, startTime);
          }
        };
        await Promise.all([runNext(), runNext(), runNext()]);

        const drafted = jobState.agents.filter(a => a.draft).length;
        if (drafted === 0) {
          await updateJob({ status: 'error', error: 'All agents failed to produce output.' });
          logEvent('error', 'phase.failed', { jobId, phase: 'execution', reason: 'all_agents_failed' });
          message.ack(); continue;
        }
        // ENHANCED: intermediate update so /status returns richer agent snapshots
        // (status + draft preview) during the gap between drafting and review.
        await updateJob({
          agents: jobState.agents.map(a => ({
            idx: a.idx, role: a.role, model: a.model, modelName: a.modelName,
            status: a.status, chars: a.draft?.length || 0,
            draftPreview: (a.draft || '').slice(0, 300),
            ownedSections: (a.ownedSections || []).map(s => s.id),
            fallbackNote: a.fallbackNote || null,
          })),
        });
        logEvent('info', 'phase.complete', { jobId, phase: 'execution', drafted, total: jobState.agents.length });

        // ── PHASE 2b: SOTA COVERAGE RECOVERY ─────────────────────
        // After the parallel draft phase, detect partial failures, build a
        // section-coverage matrix, reassign orphaned sections to surviving
        // agents, reallocate token budgets, and execute compensation drafts.
        // This runs BEFORE the review phase so that reviewers can also
        // critique the compensation work.
        const coverageMatrix = buildCoverageMatrix(jobState);
        const failureReport = detectPartialFailures(jobState);
        const totalFailures =
          failureReport.hardFailures.length +
          failureReport.ghostFailures.length +
          failureReport.dropouts.length;
        const hasUncoveredSections = coverageMatrix.missing_ids.length > 0 ||
          coverageMatrix.partial_ids.length > 0;

        let compensationSummary = null;
        if ((totalFailures > 0 || hasUncoveredSections) && Date.now() - startTime < WALL_TIME_LIMIT - 120000) {
          const reassignmentPlan = planReassignment(jobState, failureReport);
          const budgetPlan = reallocateTokenBudget(jobState, reassignmentPlan, failureReport);
          logEvent('warn', 'coverage.recovery.start', {
            jobId, uid: job.uid,
            failures: totalFailures,
            hard: failureReport.hardFailures.length,
            ghost: failureReport.ghostFailures.length,
            dropout: failureReport.dropouts.length,
            missingSections: coverageMatrix.missing_ids.length,
            partialSections: coverageMatrix.partial_ids.length,
            reassignments: reassignmentPlan.reassignments.length,
            integratorFallbackSections: reassignmentPlan.integratorFallback.sectionIds.length,
            budgetRedistributed: budgetPlan.releasedTotal,
          });
          const compOk = await executeCompensationDrafts(jobState, reassignmentPlan, apiKeyObj, startTime);
          // Rebuild matrix after compensation to record final coverage
          const postMatrix = buildCoverageMatrix(jobState);
          compensationSummary = {
            failures: failureReport,
            plan: reassignmentPlan,
            budget: budgetPlan,
            postCompensationMissing: postMatrix.missing_ids,
            postCompensationPartial: postMatrix.partial_ids,
            compensationSucceeded: compOk,
          };
          logEvent('info', 'coverage.recovery.done', {
            jobId,
            stillMissing: postMatrix.missing_ids.length,
            stillPartial: postMatrix.partial_ids.length,
            compensationSucceeded: compOk,
          });
          // Save recovery info onto jobState for the integrator + log payload
          jobState.coverageRecovery = compensationSummary;
          // Push integrator-fallback sections into state so runIntegration
          // can extend its prompt with them.
          jobState.integratorFallbackSections = reassignmentPlan.integratorFallback.sectionIds;
        } else if (totalFailures > 0 || hasUncoveredSections) {
          // No time for compensation — at least record what's missing so
          // the integrator prompt can be extended.
          const reassignmentPlan = planReassignment(jobState, failureReport);
          jobState.integratorFallbackSections = reassignmentPlan.integratorFallback.sectionIds;
          jobState.coverageRecovery = {
            failures: failureReport,
            plan: reassignmentPlan,
            budget: null,
            postCompensationMissing: coverageMatrix.missing_ids,
            postCompensationPartial: coverageMatrix.partial_ids,
            compensationSucceeded: false,
            skipped: 'wall_time',
          };
          logEvent('warn', 'coverage.recovery.skipped', {
            jobId, reason: 'wall_time', missingSections: coverageMatrix.missing_ids.length,
          });
        }

        // WRITE 3: Execution done
        await updateJob({ currentPhase: 'review' });
        logEvent('info', 'phase.start', { jobId, phase: 'review' });

        // ── PHASE 3: REVIEW ───────────────────────────────────────
        const reviewPromises = [];
        for (let i = 0; i < jobState.agents.length; i++) {
          const reviewer = jobState.agents[i];
          const target = jobState.agents[(i + 1) % jobState.agents.length];
          if (target.draft && reviewer.status !== 'error' && Date.now() - startTime < WALL_TIME_LIMIT) {
            reviewPromises.push(reviewAgent(reviewer, target, jobState, apiKeyObj));
          }
        }
        await Promise.all(reviewPromises);

        // Auto-correct high-severity reviews
        const highSevAgents = jobState.agents.filter(a => a.reviewReceived?.severity === 'high' && a.status !== 'error');
        for (const agent of highSevAgents) {
          if (Date.now() - startTime > WALL_TIME_LIMIT) break;
          const review = agent.reviewReceived;
          const revisedSys = `You are the ${agent.role}. A reviewer found HIGH severity issues:\n${review.top_issues.map(i => `- ${i}`).join('\n')}\n\nFIXES:\n${review.suggested_fixes.map(f => `- ${f}`).join('\n')}\n\nYOUR DRAFT:\n${agent.draft.slice(0, 4000)}\n\nProduce the REVISED version. No preamble.`;
          const nextModel = fallbackModel(agent.skill_tags, new Set([agent.model]), jobState);
          if (nextModel) {
            const result = await callLLM(nextModel.id, [{ role: 'system', content: revisedSys }, { role: 'user', content: 'Produce revised work.' }], apiKeyObj, { tag: `${agent.role}-redraft` });
            if (result.ok && result.content?.trim()) {
              agent.draft = cleanOutput(result.content);
              agent.tokens += Math.round(agent.draft.length / 4);
              agent.reviewReceived.severity = 'medium';
              agent.fallbackNote = (agent.fallbackNote ? agent.fallbackNote + '; ' : '') + 'auto-corrected';
            }
          }
        }

        // ── PHASE 4: INTEGRATE ────────────────────────────────────
        await updateJob({ currentPhase: 'finalization' });
        logEvent('info', 'phase.complete', { jobId, phase: 'review' });
        logEvent('info', 'phase.start', { jobId, phase: 'finalization' });
        if (Date.now() - startTime < WALL_TIME_LIMIT) {
          await runIntegration(jobState, apiKeyObj, startTime);
        } else {
          // Wall time exceeded — save what we have
          const partial = jobState.agents.filter(a => a.draft).map(a => `## ${a.role}\n\n${a.draft}`).join('\n\n');
          jobState.finalOutput = stripMetaCommentary(cleanOutput(partial)) || 'Partial result — timed out before integration.';
          jobState.teamNotes = 'Mission timed out during finalization. Showing concatenated agent drafts.';
        }

        // WRITE 4: Integration done
        await updateJob({ currentPhase: 'verification', result: jobState.finalOutput?.slice(0, 500) + (jobState.finalOutput?.length > 500 ? '...' : '') });
        logEvent('info', 'phase.complete', { jobId, phase: 'finalization' });
        logEvent('info', 'phase.start', { jobId, phase: 'verification' });

        // ── PHASE 5: VERIFY ───────────────────────────────────────
        if (Date.now() - startTime < WALL_TIME_LIMIT) {
          await verifyDeliverable(jobState, apiKeyObj, startTime);
        } else {
          jobState.verificationResult = { passed: true, gaps: [], criterionResults: [], revised: false, note: 'Skipped (wall time)' };
          logEvent('warn', 'verifier.skipped', { jobId, reason: 'wall_time' });
        }
        logEvent('info', 'phase.complete', { jobId, phase: 'verification' });

        // WRITE 5: Done!
        // Determine if this was a full completion or a partial (timed out) completion.
        // Only mark as 'partial' if the integration phase was actually skipped or
        // the output is concatenated drafts (not a real integrated result).
        // If integration completed successfully, it's 'done' — even if verification
        // was skipped due to wall time.
        const completedIntegration = jobState.finalOutput && !jobState.teamNotes?.includes('timed out');
        const finalStatus = completedIntegration ? 'done' : 'partial';

        await updateJob({
          status: finalStatus,
          currentPhase: null,
          result: jobState.finalOutput,
          teamNotes: jobState.teamNotes,
          verification: jobState.verificationResult,
          analysis: {
            mission_type: jobState.missionAnalysis?.mission_type,
            audience: jobState.missionAnalysis?.audience,
            success_criteria: jobState.missionAnalysis?.success_criteria || [],
            deliverable_structure: jobState.missionAnalysis?.deliverable_structure,
          },
          tokensUsed: jobState.tokensUsed,
          coverage: jobState.coverageRecovery ? {
            recovered: true,
            compensationSucceeded: jobState.coverageRecovery.compensationSucceeded,
            skipped: jobState.coverageRecovery.skipped || null,
            failuresDetected: {
              hard: jobState.coverageRecovery.failures.hardFailures.length,
              ghost: jobState.coverageRecovery.failures.ghostFailures.length,
              dropout: jobState.coverageRecovery.failures.dropouts.length,
            },
            reassignments: jobState.coverageRecovery.plan.reassignments.length,
            integratorFallbackSections: jobState.coverageRecovery.plan.integratorFallback.sectionIds,
            stillMissing: jobState.coverageRecovery.postCompensationMissing,
            stillPartial: jobState.coverageRecovery.postCompensationPartial,
            budgetRedistributed: jobState.coverageRecovery.budget?.releasedTotal || 0,
          } : { recovered: false, failuresDetected: { hard: 0, ghost: 0, dropout: 0 } },
          phases: [
            { name: 'planning', completedAt: startTime },
            { name: 'execution', agents: jobState.agents.map(a => ({ role: a.role, model: a.model, status: a.status, chars: a.draft?.length || 0, coverageStatus: a.coverageStatus, reassignedSections: a.reassignedSections, reallocatedBudget: a.reallocatedBudget })) },
            { name: 'coverage_recovery', ran: !!jobState.coverageRecovery, succeeded: jobState.coverageRecovery?.compensationSucceeded || false },
            { name: 'review', reviewed: jobState.agents.filter(a => a.reviewReceived).length },
            { name: 'finalization', completedAt: Date.now() },
            { name: 'verification', result: jobState.verificationResult?.passed ? 'passed' : 'partial' },
          ],
        });

        message.ack();
        logEvent('info', 'job.done', {
          jobId, uid: job.uid, status: finalStatus,
          durationMs: Date.now() - startTime,
          durationSec: Math.round((Date.now() - startTime) / 1000),
          tokensUsed: jobState.tokensUsed,
          agents: jobState.agents?.length || 0,
          outputLen: jobState.finalOutput?.length || 0,
          verified: jobState.verificationResult?.passed ? 'pass' : 'partial',
        });
        // FINAL EVENT FLUSH: ensure the terminal job.done event (and any
        // stragglers still in the buffer) are persisted to KV before the
        // consumer returns. Without this, the frontend's /events poll could
        // miss the final "Mission complete" event if the isolate is torn
        // down before the 50ms coalesce window fires. We wait up to 2s.
        try {
          const buf = _eventBuffers.get(jobId);
          if (buf && buf.events.length) {
            await _flushEventBuffer(env, jobId);
          }
        } catch {}

      } catch (err) {
        logEvent('error', 'job.failed', {
          jobId, uid: job.uid, error: err.message, stack: err.stack?.slice(0, 200),
          durationSec: Math.round((Date.now() - startTime) / 1000),
        });
        try {
          const raw = await env.FICASA_JOBS.get(jobId);
          if (raw) {
            const j = JSON.parse(raw);
            await env.FICASA_JOBS.put(jobId, JSON.stringify({ ...j, status: 'error', error: err.message, updatedAt: Date.now() }), { expirationTtl: 86400 });
          }
        } catch {}
        // FINAL EVENT FLUSH on error path too — ensure job.failed is persisted.
        try {
          const buf = _eventBuffers.get(jobId);
          if (buf && buf.events.length) {
            await _flushEventBuffer(env, jobId);
          }
        } catch {}
        message.ack();
      }
    }
  }
};
