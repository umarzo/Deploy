
// FICASA Backend Worker

export default {
  async fetch(request, env, ctx) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === '/submit' && request.method === 'POST') {
        const body = await request.json();
        const { mission, apiKey, mode, modelPreference } = body;

        if (!mission || !apiKey) {
          return Response.json({ error: 'Missing mission or apiKey' }, { status: 400, headers: corsHeaders });
        }

        const jobId = crypto.randomUUID();
        const now = Date.now();

        const jobData = {
          id: jobId,
          mission,
          apiKey,
          mode: mode || 'serious',
          modelPreference: modelPreference || 'free-only',
          status: 'queued',
          createdAt: now,
          updatedAt: now,
          phases: [],
          result: null,
          error: null,
        };

        await env.FICASA_JOBS.put(jobId, JSON.stringify(jobData), { expirationTtl: 86400 });
        await env.FICASA_QUEUE.send({ jobId });

        return Response.json({ jobId, status: 'queued' }, { headers: corsHeaders });
      }

      if (path === '/status' && request.method === 'GET') {
        const jobId = url.searchParams.get('jobId');
        if (!jobId) {
          return Response.json({ error: 'Missing jobId' }, { status: 400, headers: corsHeaders });
        }

        const raw = await env.FICASA_JOBS.get(jobId);
        if (!raw) {
          return Response.json({ error: 'Job not found' }, { status: 404, headers: corsHeaders });
        }

        const job = JSON.parse(raw);
        delete job.apiKey;
        return Response.json(job, { headers: corsHeaders });
      }

      return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });

    } catch (err) {
      return Response.json({ error: err.message }, { status: 500, headers: corsHeaders });
    }
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      const { jobId } = message.body;

      try {
        const raw = await env.FICASA_JOBS.get(jobId);
        if (!raw) { message.ack(); continue; }
        const job = JSON.parse(raw);

        const updateJob = async (updates) => {
          const current = JSON.parse(await env.FICASA_JOBS.get(jobId) || '{}');
          const updated = { ...current, ...updates, updatedAt: Date.now() };
          await env.FICASA_JOBS.put(jobId, JSON.stringify(updated), { expirationTtl: 86400 });
          return updated;
        };

        await updateJob({ status: 'running' });

        const callLLM = async (model, messages, maxTokens = 2000) => {
          const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
            method: 'POST',
            headers: {
              'Authorization': 'Bearer ' + job.apiKey,
              'Content-Type': 'application/json',
              'HTTP-Referer': 'https://ficasa-backend.officalumarx.workers.dev',
            },
            body: JSON.stringify({ model, messages, max_tokens: maxTokens }),
          });
          if (!resp.ok) throw new Error('LLM error: ' + resp.status);
          const data = await resp.json();
          return data.choices?.[0]?.message?.content || '';
        };

        const model = 'meta-llama/llama-3.1-8b-instruct:free';

        // Phase 1: Plan
        await updateJob({ currentPhase: 'planning' });
        const plan = await callLLM(model, [
          { role: 'system', content: 'You are a strategic planner. Break the user mission into 3-5 clear subtasks. Respond with a numbered list only.' },
          { role: 'user', content: 'Mission: ' + job.mission },
        ], 800);

        let current = JSON.parse(await env.FICASA_JOBS.get(jobId) || '{}');
        await updateJob({ phases: [...(current.phases || []), { name: 'planning', result: plan, completedAt: Date.now() }] });

        // Phase 2: Execute agents
        await updateJob({ currentPhase: 'executing' });
        const subtasks = plan.split('\n').filter(l => l.trim()).slice(0, 3);
        const agentResults = [];

        for (let i = 0; i < subtasks.length; i++) {
          const result = await callLLM(model, [
            { role: 'system', content: 'You are Agent ' + (i + 1) + ', specialist for: ' + subtasks[i] + '. Mission context: ' + job.mission },
            { role: 'user', content: 'Complete this subtask: ' + subtasks[i] },
          ], 1500);
          agentResults.push({ agent: i + 1, subtask: subtasks[i], result });
        }

        current = JSON.parse(await env.FICASA_JOBS.get(jobId) || '{}');
        await updateJob({ phases: [...(current.phases || []), { name: 'executing', agents: agentResults, completedAt: Date.now() }] });

        // Phase 3: Synthesize
        await updateJob({ currentPhase: 'synthesizing' });
        const drafts = agentResults.map(a => '=== Agent ' + a.agent + ' (' + a.subtask + ') ===\n' + a.result).join('\n\n');
        const finalResult = await callLLM(model, [
          { role: 'system', content: 'You are a synthesis expert. Combine agent outputs into one cohesive final deliverable. Eliminate redundancy.' },
          { role: 'user', content: 'Mission: ' + job.mission + '\n\nAgent outputs:\n' + drafts + '\n\nProduce the final result:' },
        ], 2500);

        current = JSON.parse(await env.FICASA_JOBS.get(jobId) || '{}');
        await updateJob({
          status: 'done',
          currentPhase: null,
          result: finalResult,
          phases: [...(current.phases || []), { name: 'synthesizing', result: finalResult, completedAt: Date.now() }],
        });

        message.ack();

      } catch (err) {
        try {
          const raw = await env.FICASA_JOBS.get(jobId);
          if (raw) {
            const j = JSON.parse(raw);
            await env.FICASA_JOBS.put(jobId, JSON.stringify({
              ...j, status: 'error', error: err.message, updatedAt: Date.now()
            }), { expirationTtl: 86400 });
          }
        } catch {}
        message.ack();
      }
    }
  }
};
