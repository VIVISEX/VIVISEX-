const A4_WATCHDOG = Object.freeze({
  timezone: 'Asia/Taipei',
  workflow: 'scraper.yml',
  firstCheckMinute: 8 * 60 + 27,
  lastCheckMinute: 8 * 60 + 35,
  maxRecoveryDispatches: 2,
  dispatchCooldownMs: 90 * 1000,
  startGraceMs: 90 * 1000,
  apiVersion: '2022-11-28',
  userAgent: 'QTS-A4-External-Watchdog/1.1'
});

function A4_001A_installAndVerify() {
  const verification = A4_001A_selfTest();
  A4_001A_installWatchdog();
  A4_dispatch_(A4_loadConfig_(), 'env_test');

  console.log(JSON.stringify({
    component: 'A4_001A',
    action: 'install_and_verify',
    status: 'PASS',
    workflow_id: verification.workflow_id,
    watchdog: 'INSTALLED',
    authorization_probe: 'ENV_TEST_DISPATCHED'
  }));
}

function A4_001A_selfTest() {
  const cfg = A4_loadConfig_();
  const workflow = A4_getWorkflow_(cfg);
  const runs = A4_listWorkflowRuns_(cfg);

  const result = {
    component: 'A4_001A',
    action: 'self_test',
    status: 'PASS',
    repository: `${cfg.owner}/${cfg.repo}`,
    branch: cfg.branch,
    workflow: A4_WATCHDOG.workflow,
    workflow_id: workflow.id || null,
    workflow_state: workflow.state || null,
    visible_runs: runs.length
  };

  console.log(JSON.stringify(result));
  return result;
}

function A4_001A_installWatchdog() {
  const handler = 'A4_001A_watchdogTick';
  ScriptApp.getProjectTriggers()
    .filter(trigger => trigger.getHandlerFunction() === handler)
    .forEach(trigger => ScriptApp.deleteTrigger(trigger));

  ScriptApp.newTrigger(handler)
    .timeBased()
    .everyMinutes(1)
    .create();

  console.log(JSON.stringify({
    component: 'A4_001A',
    action: 'install_watchdog',
    status: 'OK',
    cadence: 'every_1_minute',
    active_window: '08:27-08:35 Asia/Taipei'
  }));
}

function A4_001A_watchdogTick() {
  const now = new Date();
  const weekday = Number(Utilities.formatDate(now, A4_WATCHDOG.timezone, 'u'));
  if (weekday < 1 || weekday > 5) return;

  const hour = Number(Utilities.formatDate(now, A4_WATCHDOG.timezone, 'H'));
  const minute = Number(Utilities.formatDate(now, A4_WATCHDOG.timezone, 'm'));
  const minuteOfDay = hour * 60 + minute;
  if (minuteOfDay < A4_WATCHDOG.firstCheckMinute || minuteOfDay > A4_WATCHDOG.lastCheckMinute) return;

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;

  try {
    const cfg = A4_loadConfig_();
    const state = A4_loadState_();
    const today = Utilities.formatDate(now, A4_WATCHDOG.timezone, 'yyyy-MM-dd');

    if (state.date !== today) {
      state.date = today;
      state.recoveryCount = 0;
      state.lastDispatchMs = 0;
      A4_saveState_(state);
    }

    const runs = A4_listWorkflowRuns_(cfg);
    const liveRunsToday = runs
      .filter(run => A4_isLiveRunToday_(run, today))
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

    const successful = liveRunsToday.find(
      run => run.status === 'completed' && run.conclusion === 'success'
    );
    if (successful) {
      A4_log_('HEALTHY', 'successful_live_run_exists', successful, state, null);
      return;
    }

    const latest = liveRunsToday[0] || null;
    if (latest) {
      const health = A4_getRunHealth_(cfg, latest, now);
      if (health.started) {
        A4_log_('HEALTHY', 'runner_job_started', latest, state, health);
        return;
      }

      if (health.ageMs < A4_WATCHDOG.startGraceMs) {
        A4_log_('WAIT', 'run_exists_within_start_grace', latest, state, health);
        return;
      }
    }

    const nowMs = now.getTime();
    if (state.recoveryCount >= A4_WATCHDOG.maxRecoveryDispatches) {
      A4_log_('SOURCE_GAP', 'recovery_limit_reached', latest, state, latest ? A4_getRunHealth_(cfg, latest, now) : null);
      return;
    }

    if (state.lastDispatchMs > 0 && nowMs - state.lastDispatchMs < A4_WATCHDOG.dispatchCooldownMs) {
      A4_log_('WAIT', 'dispatch_cooldown', latest, state, null);
      return;
    }

    const reason = latest
      ? `runner_not_started_${latest.conclusion || latest.status || 'unknown'}`
      : 'no_live_run_created';

    A4_dispatch_(cfg, 'live');

    state.recoveryCount += 1;
    state.lastDispatchMs = nowMs;
    A4_saveState_(state);
    A4_log_('RECOVERY_DISPATCHED', reason, latest, state, null);
  } catch (error) {
    console.error(JSON.stringify({
      component: 'A4_001A',
      status: 'WATCHDOG_ERROR',
      at: new Date().toISOString(),
      error: String(error && error.stack ? error.stack : error)
    }));
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function A4_loadConfig_() {
  const props = PropertiesService.getScriptProperties();
  const token = String(props.getProperty('GITHUB_TOKEN') || '').trim();
  const owner = String(props.getProperty('GITHUB_OWNER') || 'VIVISEX').trim();
  const repo = String(props.getProperty('GITHUB_REPO') || 'VIVISEX-').trim();
  const branch = String(props.getProperty('GITHUB_DEFAULT_BRANCH') || 'main').trim();

  if (!token) throw new Error('Missing Script Property: GITHUB_TOKEN');
  if (!owner) throw new Error('Missing Script Property: GITHUB_OWNER');
  if (!repo) throw new Error('Missing Script Property: GITHUB_REPO');
  if (!branch) throw new Error('Missing Script Property: GITHUB_DEFAULT_BRANCH');

  return { token, owner, repo, branch };
}

function A4_loadState_() {
  const props = PropertiesService.getScriptProperties();
  return {
    date: String(props.getProperty('A4_WATCHDOG_DATE') || ''),
    recoveryCount: Number(props.getProperty('A4_WATCHDOG_RECOVERY_COUNT') || '0'),
    lastDispatchMs: Number(props.getProperty('A4_WATCHDOG_LAST_DISPATCH_MS') || '0')
  };
}

function A4_saveState_(state) {
  PropertiesService.getScriptProperties().setProperties({
    A4_WATCHDOG_DATE: String(state.date || ''),
    A4_WATCHDOG_RECOVERY_COUNT: String(Number(state.recoveryCount || 0)),
    A4_WATCHDOG_LAST_DISPATCH_MS: String(Number(state.lastDispatchMs || 0))
  }, false);
}

function A4_getWorkflow_(cfg) {
  const workflow = encodeURIComponent(A4_WATCHDOG.workflow);
  const url = `https://api.github.com/repos/${encodeURIComponent(cfg.owner)}/${encodeURIComponent(cfg.repo)}/actions/workflows/${workflow}`;
  const response = A4_githubFetch_(cfg, url, { method: 'get' });
  return JSON.parse(response.getContentText() || '{}');
}

function A4_listWorkflowRuns_(cfg) {
  const workflow = encodeURIComponent(A4_WATCHDOG.workflow);
  const url = `https://api.github.com/repos/${encodeURIComponent(cfg.owner)}/${encodeURIComponent(cfg.repo)}/actions/workflows/${workflow}/runs?per_page=30`;
  const response = A4_githubFetch_(cfg, url, { method: 'get' });
  const body = JSON.parse(response.getContentText() || '{}');
  return Array.isArray(body.workflow_runs) ? body.workflow_runs : [];
}

function A4_listRunJobs_(cfg, runId) {
  const url = `https://api.github.com/repos/${encodeURIComponent(cfg.owner)}/${encodeURIComponent(cfg.repo)}/actions/runs/${encodeURIComponent(String(runId))}/jobs?filter=latest&per_page=100`;
  const response = A4_githubFetch_(cfg, url, { method: 'get' });
  const body = JSON.parse(response.getContentText() || '{}');
  return Array.isArray(body.jobs) ? body.jobs : [];
}

function A4_getRunHealth_(cfg, run, now) {
  const jobs = A4_listRunJobs_(cfg, run.id);
  const startedJobs = jobs.filter(job =>
    job.status === 'in_progress' ||
    job.status === 'completed' ||
    Boolean(job.started_at)
  );

  const createdMs = run.created_at ? new Date(run.created_at).getTime() : 0;
  const ageMs = createdMs > 0 ? Math.max(0, now.getTime() - createdMs) : Number.MAX_SAFE_INTEGER;

  return {
    started: startedJobs.length > 0,
    ageMs,
    totalJobs: jobs.length,
    startedJobs: startedJobs.length,
    jobStatuses: jobs.map(job => ({
      id: job.id,
      name: job.name,
      status: job.status,
      conclusion: job.conclusion,
      started_at: job.started_at || null
    }))
  };
}

function A4_isLiveRunToday_(run, today) {
  const title = String(run.display_title || run.name || '').toLowerCase();
  if (!title.includes('live')) return false;

  const createdAt = run.created_at ? new Date(run.created_at) : null;
  if (!createdAt || Number.isNaN(createdAt.getTime())) return false;

  const localDate = Utilities.formatDate(createdAt, A4_WATCHDOG.timezone, 'yyyy-MM-dd');
  return localDate === today;
}

function A4_dispatch_(cfg, runMode) {
  if (runMode !== 'live' && runMode !== 'env_test') {
    throw new Error(`Unsupported dispatch mode: ${runMode}`);
  }

  const workflow = encodeURIComponent(A4_WATCHDOG.workflow);
  const url = `https://api.github.com/repos/${encodeURIComponent(cfg.owner)}/${encodeURIComponent(cfg.repo)}/actions/workflows/${workflow}/dispatches`;
  const response = A4_githubFetch_(cfg, url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({
      ref: cfg.branch,
      inputs: { run_mode: runMode }
    })
  });

  if (response.getResponseCode() !== 204) {
    throw new Error(`Unexpected workflow_dispatch response: ${response.getResponseCode()}`);
  }
}

function A4_githubFetch_(cfg, url, options) {
  const request = Object.assign({}, options || {});
  request.muteHttpExceptions = true;
  request.headers = Object.assign({}, request.headers || {}, {
    Authorization: `Bearer ${cfg.token}`,
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': A4_WATCHDOG.apiVersion,
    'User-Agent': A4_WATCHDOG.userAgent
  });

  let lastCode = 0;
  let lastBody = '';

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const response = UrlFetchApp.fetch(url, request);
    const code = response.getResponseCode();
    if (code >= 200 && code < 300) return response;

    lastCode = code;
    lastBody = response.getContentText() || '';
    const retryable = code === 429 || code >= 500;
    if (!retryable || attempt === 3) break;

    Utilities.sleep(Math.min(4000, 500 * Math.pow(2, attempt - 1)));
  }

  throw new Error(`GitHub API failed: HTTP ${lastCode}; body=${lastBody.slice(0, 500)}`);
}

function A4_log_(status, reason, run, state, health) {
  console.log(JSON.stringify({
    component: 'A4_001A',
    status,
    reason,
    at: Utilities.formatDate(new Date(), A4_WATCHDOG.timezone, "yyyy-MM-dd'T'HH:mm:ssXXX"),
    recovery_count: state.recoveryCount,
    latest_run_id: run ? run.id : null,
    latest_run_event: run ? run.event : null,
    latest_run_status: run ? run.status : null,
    latest_run_conclusion: run ? run.conclusion : null,
    latest_run_created_at: run ? run.created_at : null,
    runner_started: health ? health.started : null,
    runner_jobs: health ? health.totalJobs : null,
    runner_started_jobs: health ? health.startedJobs : null
  }));
}
