export const dashboardHtml = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>Log Service Console</title>
  <style>
    :root {
      --bg: #090d16;
      --panel: rgba(18, 25, 39, .82);
      --panel-solid: #121927;
      --panel-soft: #182133;
      --line: rgba(148, 163, 184, .14);
      --line-strong: rgba(148, 163, 184, .24);
      --text: #edf4ff;
      --muted: #8fa0b8;
      --accent: #7c8cff;
      --accent-2: #53d4c6;
      --success: #55d99d;
      --warning: #f5b75f;
      --danger: #ff7188;
      --shadow: 0 22px 60px rgba(0, 0, 0, .28);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }
    html { min-width: 320px; background: var(--bg); scroll-behavior: smooth; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at 78% 8%, rgba(86, 100, 255, .15), transparent 30rem),
        radial-gradient(circle at 5% 90%, rgba(39, 205, 181, .08), transparent 28rem),
        var(--bg);
    }

    button, input, select { font: inherit; }
    button { cursor: pointer; }
    .app-shell { min-height: 100vh; display: grid; grid-template-columns: 250px minmax(0, 1fr); }
    .sidebar {
      position: sticky;
      top: 0;
      height: 100vh;
      padding: 26px 20px;
      border-right: 1px solid var(--line);
      background: rgba(8, 12, 20, .76);
      backdrop-filter: blur(20px);
      display: flex;
      flex-direction: column;
      z-index: 2;
    }

    .brand { display: flex; gap: 12px; align-items: center; padding: 0 8px 28px; }
    .brand-mark {
      width: 38px;
      height: 38px;
      display: grid;
      place-items: center;
      border-radius: 12px;
      font-weight: 800;
      color: white;
      background: linear-gradient(140deg, var(--accent), #5966dc 55%, var(--accent-2));
      box-shadow: 0 10px 28px rgba(96, 111, 255, .28);
    }
    .brand strong { display: block; font-size: 14px; letter-spacing: .01em; }
    .brand span { display: block; margin-top: 2px; color: var(--muted); font-size: 11px; }
    .nav-label { padding: 12px 12px 8px; color: #66758b; font-size: 10px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; }
    .nav-item {
      display: flex;
      align-items: center;
      gap: 11px;
      min-height: 44px;
      padding: 0 12px;
      border: 1px solid transparent;
      border-radius: 11px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 650;
      text-decoration: none;
      transition: color .18s, border-color .18s, background .18s;
    }
    .nav-item:hover { color: #fff; border-color: var(--line); background: rgba(255,255,255,.025); }
    .nav-item.active { color: #fff; border-color: var(--line); background: rgba(124, 140, 255, .11); }
    .nav-icon { width: 20px; text-align: center; color: var(--accent-2); }
    .sidebar-status {
      margin-top: auto;
      padding: 15px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, .025);
    }
    .status-row { display: flex; align-items: center; gap: 9px; font-size: 12px; font-weight: 700; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #718096; box-shadow: 0 0 0 4px rgba(113,128,150,.1); }
    .status-dot.live { background: var(--success); box-shadow: 0 0 0 4px rgba(85,217,157,.12), 0 0 16px rgba(85,217,157,.45); }
    .status-dot.error { background: var(--danger); }
    .sidebar-status p { margin: 8px 0 0; color: var(--muted); font-size: 11px; line-height: 1.55; }

    main { min-width: 0; padding: 34px clamp(22px, 4vw, 58px) 58px; }
    .topbar { display: flex; align-items: flex-start; justify-content: space-between; gap: 22px; margin-bottom: 28px; }
    .top-actions { display: flex; align-items: center; gap: 9px; }
    .eyebrow { color: var(--accent-2); font-size: 10px; font-weight: 850; letter-spacing: .18em; text-transform: uppercase; }
    h1 { margin: 7px 0 7px; font-size: clamp(27px, 3vw, 40px); line-height: 1.08; letter-spacing: -.035em; }
    .subtitle { margin: 0; color: var(--muted); font-size: 13px; }
    .live-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      border: 1px solid rgba(85, 217, 157, .18);
      border-radius: 999px;
      color: #a4f2cb;
      background: rgba(85, 217, 157, .07);
      font-size: 11px;
      font-weight: 750;
      white-space: nowrap;
    }
    .access-button {
      height: 34px;
      padding: 0 12px;
      border: 1px solid var(--line-strong);
      border-radius: 999px;
      color: var(--muted);
      background: rgba(255,255,255,.025);
      font-size: 10px;
      font-weight: 750;
    }
    .access-button:hover { color: #fff; border-color: rgba(124,140,255,.45); }

    .stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin-bottom: 18px; }
    .stat-card, .panel {
      border: 1px solid var(--line);
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(15px);
    }
    .stat-card { min-height: 118px; padding: 18px; border-radius: 16px; }
    .stat-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 11px; font-weight: 700; }
    .stat-icon { width: 29px; height: 29px; display: grid; place-items: center; border-radius: 9px; color: #cdd4ff; background: rgba(124, 140, 255, .11); }
    .stat-value { margin-top: 12px; font-size: 25px; font-weight: 780; letter-spacing: -.03em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .stat-note { margin-top: 4px; color: #66758b; font-size: 10px; }

    .panel { border-radius: 18px; overflow: hidden; }
    .panel-head { display: flex; justify-content: space-between; align-items: center; gap: 18px; padding: 19px 20px; border-bottom: 1px solid var(--line); }
    .panel-title { margin: 0; font-size: 14px; letter-spacing: -.01em; }
    .panel-copy { margin: 4px 0 0; color: var(--muted); font-size: 11px; }
    .filter-wrap { padding: 18px 20px; border-bottom: 1px solid var(--line); background: rgba(255,255,255,.012); }
    .filters { display: grid; grid-template-columns: 1fr 170px 1.25fr 1fr auto; gap: 10px; }
    .field { min-width: 0; }
    .field label { display: block; margin: 0 0 7px 2px; color: #73839a; font-size: 9px; font-weight: 800; letter-spacing: .1em; text-transform: uppercase; }
    .control {
      width: 100%;
      height: 42px;
      border: 1px solid var(--line-strong);
      border-radius: 10px;
      outline: none;
      color: var(--text);
      background: #0d1421;
      padding: 0 12px;
      font-size: 12px;
      transition: border-color .18s, box-shadow .18s;
    }
    .control::placeholder { color: #56657a; }
    .control:focus { border-color: rgba(124, 140, 255, .65); box-shadow: 0 0 0 3px rgba(124,140,255,.1); }
    select.control { color-scheme: dark; }
    .actions { display: flex; align-items: end; gap: 8px; }
    .button {
      height: 42px;
      border: 1px solid transparent;
      border-radius: 10px;
      padding: 0 15px;
      color: white;
      font-size: 12px;
      font-weight: 760;
      background: linear-gradient(135deg, var(--accent), #6573e5);
      box-shadow: 0 10px 25px rgba(92, 107, 229, .2);
    }
    .button:hover { filter: brightness(1.08); }
    .button:disabled { cursor: not-allowed; opacity: .48; filter: none; }
    .button.secondary { color: var(--muted); border-color: var(--line-strong); background: transparent; box-shadow: none; }
    .query-strip { display: flex; justify-content: space-between; gap: 12px; padding: 11px 20px; color: var(--muted); font-size: 10px; border-bottom: 1px solid var(--line); }
    .query-strip strong { color: #bbc6d7; }
    .table-wrap { overflow-x: auto; min-height: 310px; }
    table { width: 100%; min-width: 850px; border-collapse: collapse; table-layout: fixed; }
    th { padding: 12px 16px; color: #68788f; font-size: 9px; font-weight: 850; letter-spacing: .12em; text-align: left; text-transform: uppercase; background: rgba(255,255,255,.015); }
    td { padding: 14px 16px; border-top: 1px solid rgba(148,163,184,.09); color: #c6d1e0; font-size: 11px; vertical-align: top; }
    tr:hover td { background: rgba(124,140,255,.025); }
    .col-time { width: 168px; }
    .col-level { width: 90px; }
    .col-service { width: 145px; }
    .col-attributes { width: 225px; }
    .time-cell { color: #8190a5; font-variant-numeric: tabular-nums; }
    .service-cell { color: #d9e4f5; font-weight: 700; }
    .message-cell { color: #b5c2d4; line-height: 1.5; overflow-wrap: anywhere; }
    .level-badge { display: inline-flex; padding: 4px 8px; border-radius: 999px; font-size: 9px; font-weight: 850; text-transform: uppercase; letter-spacing: .05em; }
    .level-debug { color: #b2bcff; background: rgba(124,140,255,.12); }
    .level-info { color: #7be6da; background: rgba(83,212,198,.11); }
    .level-warn { color: #ffd18b; background: rgba(245,183,95,.12); }
    .level-error { color: #ff9aaa; background: rgba(255,113,136,.12); }
    .attribute-list { display: flex; flex-wrap: wrap; gap: 5px; }
    .attribute-pill { max-width: 100%; padding: 4px 7px; border: 1px solid var(--line); border-radius: 6px; color: #8797ad; background: rgba(255,255,255,.018); font: 9px ui-monospace, SFMono-Regular, Menlo, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .empty { display: grid; place-items: center; min-height: 275px; padding: 30px; text-align: center; }
    .empty-mark { width: 46px; height: 46px; display: grid; place-items: center; margin: 0 auto 13px; border-radius: 14px; color: var(--accent-2); background: rgba(83,212,198,.08); font-size: 20px; }
    .empty h3 { margin: 0 0 6px; font-size: 14px; }
    .empty p { max-width: 390px; margin: 0; color: var(--muted); font-size: 11px; line-height: 1.6; }
    .panel-foot { display: flex; justify-content: space-between; align-items: center; gap: 15px; padding: 14px 20px; border-top: 1px solid var(--line); }
    .result-count { color: var(--muted); font-size: 10px; }

    .lower-grid { display: grid; grid-template-columns: 1.15fr .85fr; gap: 14px; margin-top: 14px; }
    .metric-body { min-height: 144px; padding: 18px 20px; }
    .metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .metric { padding: 13px; border: 1px solid var(--line); border-radius: 12px; background: rgba(255,255,255,.018); }
    .metric span { display: block; color: var(--muted); font-size: 9px; }
    .metric strong { display: block; margin-top: 7px; font-size: 19px; }
    .notice { display: flex; align-items: flex-start; gap: 12px; padding: 15px; border: 1px dashed var(--line-strong); border-radius: 12px; color: var(--muted); font-size: 11px; line-height: 1.6; }
    .notice strong { display: block; color: #c4cedd; margin-bottom: 2px; }
    .endpoint-list { display: grid; gap: 9px; padding: 18px 20px; }
    .endpoint { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 10px 12px; border: 1px solid var(--line); border-radius: 11px; background: rgba(255,255,255,.018); }
    .endpoint code { color: #aeb9ca; font-size: 10px; }
    .method { min-width: 40px; color: var(--accent-2); font-size: 9px; font-weight: 900; }
    .endpoint-state { color: var(--success); font-size: 9px; font-weight: 800; }

    .spinner { width: 12px; height: 12px; display: inline-block; margin-right: 7px; border: 2px solid rgba(255,255,255,.2); border-top-color: #fff; border-radius: 50%; animation: spin .7s linear infinite; vertical-align: -2px; }
    dialog {
      width: min(430px, calc(100vw - 32px));
      padding: 0;
      border: 1px solid var(--line-strong);
      border-radius: 17px;
      color: var(--text);
      background: var(--panel-solid);
      box-shadow: 0 30px 90px rgba(0,0,0,.6);
    }
    dialog::backdrop { background: rgba(3,6,12,.76); backdrop-filter: blur(5px); }
    .dialog-head { padding: 20px 20px 14px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 16px; }
    .dialog-head p { margin: 7px 0 0; color: var(--muted); font-size: 11px; line-height: 1.55; }
    .dialog-body { padding: 18px 20px; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; padding: 0 20px 20px; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .hidden { display: none !important; }

    @media (max-width: 1100px) {
      .filters { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .actions { grid-column: span 2; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .lower-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .app-shell { display: block; }
      .sidebar { position: static; width: auto; height: auto; padding: 16px 18px; border-right: 0; border-bottom: 1px solid var(--line); flex-direction: row; align-items: center; gap: 14px; }
      .brand { padding: 0; }
      .nav-label, .nav-item, .sidebar-status p { display: none; }
      .sidebar-status { margin: 0 0 0 auto; padding: 10px 12px; }
      main { padding: 25px 16px 40px; }
      .topbar { align-items: center; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .filters { grid-template-columns: 1fr; }
      .actions { grid-column: auto; }
      .metric-grid { grid-template-columns: repeat(2, 1fr); }
      .panel-head, .query-strip, .panel-foot { align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">L</div>
        <div><strong>Log Service</strong><span>Observability console</span></div>
      </div>
      <div class="nav-label">Workspace</div>
      <a class="nav-item active" href="#log-explorer"><span class="nav-icon">⌁</span>Log explorer</a>
      <a class="nav-item" href="#operational-counters"><span class="nav-icon">◫</span>Operational counters</a>
      <a class="nav-item" href="#core-api"><span class="nav-icon">↗</span>API endpoints</a>
      <div class="sidebar-status">
        <div class="status-row"><span class="status-dot" id="health-dot"></span><span id="health-label">Checking API</span></div>
        <p id="health-copy">Connecting to the ingestion and query service.</p>
      </div>
    </aside>

    <main>
      <header class="topbar">
        <div>
          <div class="eyebrow">Log observability</div>
          <h1>Explore your logs</h1>
          <p class="subtitle">Search structured events, inspect attributes, and monitor service activity.</p>
        </div>
        <div class="top-actions"><button class="access-button" id="access-button" type="button">API key</button><div class="live-pill"><span class="status-dot live"></span>Live workspace</div></div>
      </header>

      <section class="stats" aria-label="Page summary">
        <article class="stat-card"><div class="stat-head"><span>Loaded events</span><span class="stat-icon">≋</span></div><div class="stat-value" id="stat-events">0</div><div class="stat-note">Across loaded pages</div></article>
        <article class="stat-card"><div class="stat-head"><span>Error events</span><span class="stat-icon">!</span></div><div class="stat-value" id="stat-errors">0</div><div class="stat-note">In the current result set</div></article>
        <article class="stat-card"><div class="stat-head"><span>Services</span><span class="stat-icon">◇</span></div><div class="stat-value" id="stat-services">0</div><div class="stat-note">Unique services loaded</div></article>
        <article class="stat-card"><div class="stat-head"><span>Latest event</span><span class="stat-icon">◷</span></div><div class="stat-value" id="stat-latest">—</div><div class="stat-note">Newest timestamp loaded</div></article>
      </section>

      <section class="panel" id="log-explorer">
        <div class="panel-head">
          <div><h2 class="panel-title">Log explorer</h2><p class="panel-copy">Combine filters to narrow the retained event stream.</p></div>
          <button class="button secondary" id="refresh-button" type="button">Refresh</button>
        </div>
        <div class="filter-wrap">
          <form class="filters" id="filters">
            <div class="field"><label for="service">Service</label><input class="control" id="service" name="service" placeholder="e.g. checkout"></div>
            <div class="field"><label for="level">Level</label><select class="control" id="level" name="level"><option value="">All levels</option><option value="debug">Debug</option><option value="info">Info</option><option value="warn">Warn</option><option value="error">Error</option></select></div>
            <div class="field"><label for="q">Message contains</label><input class="control" id="q" name="q" placeholder="Search message text"></div>
            <div class="field"><label for="attribute">Attribute</label><input class="control" id="attribute" placeholder="region=eu-west"></div>
            <div class="actions"><button class="button" id="search-button" type="submit">Search logs</button><button class="button secondary" id="reset-button" type="button">Reset</button></div>
          </form>
        </div>
        <div class="query-strip"><span id="query-status">Ready to search the log stream.</span><span><strong>50</strong> rows per page</span></div>
        <div class="table-wrap">
          <table id="log-table" class="hidden">
            <thead><tr><th class="col-time">Timestamp</th><th class="col-level">Level</th><th class="col-service">Service</th><th>Message</th><th class="col-attributes">Attributes</th></tr></thead>
            <tbody id="log-rows"></tbody>
          </table>
          <div class="empty" id="empty-state"><div><div class="empty-mark">⌕</div><h3>No logs loaded yet</h3><p>Run a search to inspect the newest events. Filters can be freely combined without changing the underlying API contract.</p></div></div>
        </div>
        <div class="panel-foot"><span class="result-count" id="result-count">0 events loaded</span><button class="button secondary" id="next-button" type="button" disabled>Load next page</button></div>
      </section>

      <section class="lower-grid">
        <article class="panel" id="operational-counters">
          <div class="panel-head"><div><h2 class="panel-title">Operational counters</h2><p class="panel-copy">Prometheus-compatible process counters.</p></div></div>
          <div class="metric-body" id="metric-body"><div class="notice"><span>◌</span><div><strong>Loading metrics</strong>Checking whether optional metrics are enabled.</div></div></div>
        </article>
        <article class="panel" id="core-api">
          <div class="panel-head"><div><h2 class="panel-title">Core API</h2><p class="panel-copy">Required contract endpoints.</p></div></div>
          <div class="endpoint-list">
            <div class="endpoint"><span class="method">GET</span><code>/health</code><span class="endpoint-state">Ready</span></div>
            <div class="endpoint"><span class="method">POST</span><code>/logs</code><span class="endpoint-state">Ready</span></div>
            <div class="endpoint"><span class="method">GET</span><code>/logs</code><span class="endpoint-state">Ready</span></div>
            <div class="endpoint"><span class="method">GET</span><code>/logs/aggregate</code><span class="endpoint-state">Ready</span></div>
          </div>
        </article>
      </section>
    </main>
  </div>

  <dialog id="access-dialog">
    <div class="dialog-head"><h2>Authenticated access</h2><p>The baseline service needs no key. When AUTH_ENABLED=true, enter a bearer key for API requests; it stays only in this browser tab.</p></div>
    <div class="dialog-body"><div class="field"><label for="api-key">Bearer API key</label><input class="control" id="api-key" type="password" autocomplete="off" placeholder="Paste an optional API key"></div></div>
    <div class="dialog-actions"><button class="button secondary" id="clear-key-button" type="button">Clear key</button><button class="button secondary" id="close-access-button" type="button">Cancel</button><button class="button" id="save-key-button" type="button">Apply key</button></div>
  </dialog>

  <script>
    (function () {
      var form = document.getElementById('filters');
      var table = document.getElementById('log-table');
      var rowsElement = document.getElementById('log-rows');
      var emptyState = document.getElementById('empty-state');
      var searchButton = document.getElementById('search-button');
      var refreshButton = document.getElementById('refresh-button');
      var resetButton = document.getElementById('reset-button');
      var nextButton = document.getElementById('next-button');
      var queryStatus = document.getElementById('query-status');
      var resultCount = document.getElementById('result-count');
      var loadedRows = [];
      var nextCursor = null;
      var activeLogRequest = null;
      var logRequestId = 0;
      var metricRequestId = 0;

      function apiHeaders() {
        var headers = { accept: 'application/json' };
        var key = sessionStorage.getItem('log-service-api-key');
        if (key) headers.authorization = 'Bearer ' + key;
        return headers;
      }

      function setLoading(loading, continuation) {
        searchButton.disabled = loading;
        refreshButton.disabled = loading;
        resetButton.disabled = loading;
        nextButton.disabled = loading || !nextCursor;
        if (loading) queryStatus.innerHTML = '<span class="spinner"></span>' + (continuation ? 'Loading the next page…' : 'Querying retained logs…');
      }

      function td(className, value) {
        var cell = document.createElement('td');
        if (className) cell.className = className;
        cell.textContent = value == null ? '—' : String(value);
        return cell;
      }

      function formatTimestamp(value) {
        var date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value || '—');
        return date.toLocaleString([], { year: 'numeric', month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
      }

      function renderRows() {
        rowsElement.replaceChildren();
        loadedRows.forEach(function (log) {
          var row = document.createElement('tr');
          row.appendChild(td('time-cell', formatTimestamp(log.timestamp)));

          var levelCell = document.createElement('td');
          var level = document.createElement('span');
          var levelName = String(log.level || 'unknown').toLowerCase();
          level.className = 'level-badge level-' + levelName;
          level.textContent = levelName;
          levelCell.appendChild(level);
          row.appendChild(levelCell);

          row.appendChild(td('service-cell', log.service));
          row.appendChild(td('message-cell', log.message));

          var attributesCell = document.createElement('td');
          var list = document.createElement('div');
          list.className = 'attribute-list';
          var entries = Object.entries(log.attributes || {});
          entries.slice(0, 4).forEach(function (entry) {
            var pill = document.createElement('span');
            pill.className = 'attribute-pill';
            pill.textContent = entry[0] + '=' + String(entry[1]);
            pill.title = pill.textContent;
            list.appendChild(pill);
          });
          if (entries.length > 4) {
            var more = document.createElement('span');
            more.className = 'attribute-pill';
            more.textContent = '+' + (entries.length - 4) + ' more';
            list.appendChild(more);
          }
          if (entries.length === 0) list.textContent = '—';
          attributesCell.appendChild(list);
          row.appendChild(attributesCell);
          rowsElement.appendChild(row);
        });

        var hasRows = loadedRows.length > 0;
        table.classList.toggle('hidden', !hasRows);
        emptyState.classList.toggle('hidden', hasRows);
        resultCount.textContent = loadedRows.length + (loadedRows.length === 1 ? ' event loaded' : ' events loaded');
        nextButton.disabled = !nextCursor;
        document.getElementById('stat-events').textContent = loadedRows.length.toLocaleString();
        document.getElementById('stat-errors').textContent = loadedRows.filter(function (log) { return log.level === 'error'; }).length.toLocaleString();
        document.getElementById('stat-services').textContent = new Set(loadedRows.map(function (log) { return log.service; })).size.toLocaleString();
        document.getElementById('stat-latest').textContent = hasRows ? new Date(loadedRows[0].timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '—';
      }

      function buildParams() {
        var params = new URLSearchParams();
        var service = document.getElementById('service').value.trim();
        var level = document.getElementById('level').value;
        var q = document.getElementById('q').value.trim();
        var attribute = document.getElementById('attribute').value.trim();
        if (service) params.set('service', service);
        if (level) params.set('level', level);
        if (q) params.set('q', q);
        if (attribute) {
          var separator = attribute.indexOf('=');
          var attributeKey = separator < 0 ? '' : attribute.slice(0, separator).trim();
          if (separator < 0 || !attributeKey) throw new Error('Attribute must use a non-empty key in key=value format.');
          params.set('attr.' + attributeKey, attribute.slice(separator + 1).trim());
        }
        params.set('limit', '50');
        return params;
      }

      async function loadLogs(continuation) {
        var requestId = ++logRequestId;
        if (activeLogRequest) activeLogRequest.abort();
        var controller = new AbortController();
        activeLogRequest = controller;
        setLoading(true, continuation);
        try {
          var url = continuation && nextCursor ? '/logs?cursor=' + encodeURIComponent(nextCursor) : '/logs?' + buildParams().toString();
          var response = await fetch(url, { headers: apiHeaders(), signal: controller.signal });
          var data = await response.json();
          if (requestId !== logRequestId) return;
          if (!response.ok) throw new Error(data.error || 'The query failed with HTTP ' + response.status);
          var page = Array.isArray(data.logs) ? data.logs : [];
          loadedRows = continuation ? loadedRows.concat(page) : page;
          nextCursor = data.next_cursor || null;
          renderRows();
          queryStatus.textContent = page.length === 0 ? 'The query completed with no matching events.' : 'Query completed successfully. ' + page.length + ' rows returned on this page.';
        } catch (error) {
          if (error instanceof DOMException && error.name === 'AbortError') return;
          if (requestId !== logRequestId) return;
          if (!continuation) { loadedRows = []; nextCursor = null; renderRows(); }
          queryStatus.textContent = error instanceof Error ? error.message : 'Unable to query logs.';
        } finally {
          if (requestId === logRequestId) {
            activeLogRequest = null;
            setLoading(false, continuation);
          }
        }
      }

      async function loadHealth() {
        var dot = document.getElementById('health-dot');
        var label = document.getElementById('health-label');
        var copy = document.getElementById('health-copy');
        try {
          var response = await fetch('/health');
          if (!response.ok) throw new Error('not ready');
          dot.className = 'status-dot live';
          label.textContent = 'API connected';
          copy.textContent = 'Ingestion, querying, and PostgreSQL are ready.';
        } catch (_) {
          dot.className = 'status-dot error';
          label.textContent = 'API unavailable';
          copy.textContent = 'The service did not answer its health check.';
        }
      }

      async function loadMetrics() {
        var requestId = ++metricRequestId;
        var body = document.getElementById('metric-body');
        try {
          var response = await fetch('/metrics', { headers: apiHeaders() });
          if (requestId !== metricRequestId) return;
          if (!response.ok) {
            if (response.status === 404) {
              body.innerHTML = '<div class="notice"><span>◌</span><div><strong>Optional metrics are disabled</strong>The baseline service keeps metrics off for benchmark isolation. Set METRICS_ENABLED=true for an operational demo.</div></div>';
              return;
            }
            if (response.status === 401 || response.status === 403) {
              body.innerHTML = '<div class="notice"><span>◌</span><div><strong>Authentication required</strong>Use the API key button above to authorize dashboard data requests.</div></div>';
              return;
            }
            throw new Error('Metrics request failed with HTTP ' + response.status);
          }
          var text = await response.text();
          if (requestId !== metricRequestId) return;
          var values = {};
          text.split('\\n').forEach(function (line) {
            if (!line || line.charAt(0) === '#') return;
            var parts = line.trim().split(/\\s+/);
            values[parts[0]] = Number(parts[1]);
          });
          var cards = [
            ['Requests', values.log_service_requests_total || 0],
            ['Failures', values.log_service_failures_total || 0],
            ['Accepted logs', values.log_service_accepted_logs_total || 0],
            ['Rejected logs', values.log_service_rejected_logs_total || 0]
          ];
          var grid = document.createElement('div');
          grid.className = 'metric-grid';
          cards.forEach(function (card) {
            var item = document.createElement('div');
            item.className = 'metric';
            var name = document.createElement('span');
            name.textContent = card[0];
            var value = document.createElement('strong');
            value.textContent = Number(card[1]).toLocaleString();
            item.append(name, value);
            grid.appendChild(item);
          });
          body.replaceChildren(grid);
        } catch (_) {
          body.innerHTML = '<div class="notice"><span>!</span><div><strong>Metrics unavailable</strong>The dashboard could not reach the optional metrics endpoint.</div></div>';
        }
      }

      form.addEventListener('submit', function (event) { event.preventDefault(); loadLogs(false); });
      refreshButton.addEventListener('click', function () { loadLogs(false); });
      resetButton.addEventListener('click', function () { form.reset(); loadLogs(false); });
      nextButton.addEventListener('click', function () { loadLogs(true); });
      var accessDialog = document.getElementById('access-dialog');
      var apiKeyInput = document.getElementById('api-key');
      var accessButton = document.getElementById('access-button');
      function updateAccessLabel() { accessButton.textContent = sessionStorage.getItem('log-service-api-key') ? 'API key set' : 'API key'; }
      accessButton.addEventListener('click', function () { apiKeyInput.value = sessionStorage.getItem('log-service-api-key') || ''; accessDialog.showModal(); });
      document.getElementById('close-access-button').addEventListener('click', function () { accessDialog.close(); });
      document.getElementById('clear-key-button').addEventListener('click', function () { sessionStorage.removeItem('log-service-api-key'); apiKeyInput.value = ''; updateAccessLabel(); accessDialog.close(); loadMetrics(); });
      document.getElementById('save-key-button').addEventListener('click', function () {
        var key = apiKeyInput.value.trim();
        if (key) sessionStorage.setItem('log-service-api-key', key); else sessionStorage.removeItem('log-service-api-key');
        updateAccessLabel();
        accessDialog.close();
        loadMetrics();
      });
      updateAccessLabel();
      loadHealth();
      loadMetrics();
    })();
  </script>
</body>
</html>`;
