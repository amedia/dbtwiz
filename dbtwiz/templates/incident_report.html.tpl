<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Incident report</title>
<style>
  :root {
    --bg: #f6f7f9; --surface: #ffffff; --text: #1b1f27; --muted: #5f6673;
    --line: #e3e6eb; --code-bg: #eef0f4;
    --header-bg: #1f2a44; --header-text: #ffffff; --header-muted: #b9c2d6;
    --red: #c4321c; --red-bg: #fde8e4;
    --orange: #b8580a; --orange-bg: #fdeedd;
    --yellow: #8a6a00; --yellow-bg: #fbf3d0;
    --green: #2c7a45; --green-bg: #e3f3e8;
    --job-0: #3b5bdb; --job-0-bg: #e7ecfd;
    --job-1: #0b7a75; --job-1-bg: #ddf3f1;
    --job-2: #8a3fb8; --job-2-bg: #f2e7fa;
    --job-3: #b23a6b; --job-3-bg: #fbe6ef;
    --job-4: #5c6b00; --job-4-bg: #eef3d6;
    --job-5: #4a5568; --job-5-bg: #e9ecf1;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #12151b; --surface: #1a1e26; --text: #e7eaf0; --muted: #98a0ae;
      --line: #2b313c; --code-bg: #252b35;
      --header-bg: #1c2740; --header-text: #ffffff; --header-muted: #9fabc4;
      --red: #ff8a75; --red-bg: #3d1d18;
      --orange: #f5a35c; --orange-bg: #3a2614;
      --yellow: #e6c454; --yellow-bg: #332b10;
      --green: #6fcf8f; --green-bg: #17301f;
      --job-0: #8fa6ff; --job-0-bg: #1f2850;
      --job-1: #5fd0c8; --job-1-bg: #133432;
      --job-2: #cf9bf0; --job-2-bg: #2f1f3e;
      --job-3: #f08cb5; --job-3-bg: #3a1a28;
      --job-4: #c3d35c; --job-4-bg: #2a2f12;
      --job-5: #b4bdcc; --job-5-bg: #2a303b;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  header { background: var(--header-bg); color: var(--header-text); }
  header .inner {
    max-width: 1200px; margin: 0 auto; padding: 20px 16px;
    display: flex; gap: 16px; align-items: center; justify-content: space-between; flex-wrap: wrap;
  }
  header h1 { font-size: 22px; margin: 0; }
  header p { margin: 2px 0 0; color: var(--header-muted); font-size: 13px; }
  .status { padding: 4px 12px; border-radius: 999px; font-weight: 600; font-size: 13px; }
  .status.bad { background: var(--red); color: #fff; }
  .status.ok { background: var(--green); color: #fff; }
  main { max-width: 1200px; margin: 0 auto; padding: 20px 16px 48px; }
  .summary { display: flex; gap: 12px; flex-wrap: wrap; }
  .tile {
    background: var(--surface); border: 1px solid var(--line); border-top: 4px solid var(--accent);
    border-radius: 8px; padding: 10px 14px; min-width: 160px; color: var(--muted);
  }
  .tile b { display: block; font-size: 24px; color: var(--accent); font-variant-numeric: tabular-nums; }
  .tile.red { --accent: var(--red); }
  .tile.orange { --accent: var(--orange); }
  .tile.green { --accent: var(--green); }
  .tile.quiet { --accent: var(--muted); }
  h2 { font-size: 16px; margin: 32px 0 10px; display: flex; align-items: center; gap: 8px; }
  h2::before { content: ""; width: 10px; height: 10px; border-radius: 50%; background: var(--dot); }
  h2.open { --dot: var(--red); }
  h2.cleared { --dot: var(--green); }
  .scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { font-weight: 600; color: var(--muted); font-size: 12px; white-space: nowrap; background: var(--bg); }
  tr:last-child td { border-bottom: 0; }
  tbody tr { box-shadow: inset 4px 0 0 var(--sev, transparent); }
  tr.sev-low { --sev: var(--yellow); }
  tr.sev-mid { --sev: var(--orange); }
  tr.sev-high { --sev: var(--red); }
  tr.cleared-row { --sev: var(--green); }
  td.num { font-variant-numeric: tabular-nums; white-space: nowrap; }
  td.when { white-space: nowrap; }
  code { background: var(--code-bg); padding: 1px 4px; border-radius: 4px; font-size: 12px; }
  .badge { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .sev-low .badge { background: var(--yellow-bg); color: var(--yellow); }
  .sev-mid .badge { background: var(--orange-bg); color: var(--orange); }
  .sev-high .badge { background: var(--red-bg); color: var(--red); }
  .cleared-row .badge { background: var(--green-bg); color: var(--green); }
  .job { display: inline-block; padding: 1px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; white-space: nowrap;
         background: var(--job-bg); color: var(--job-fg); }
  {% for n in range(6) %}.job-{{ n }} { --job-fg: var(--job-{{ n }}); --job-bg: var(--job-{{ n }}-bg); }
  {% endfor %}
  .radius { color: var(--orange); font-weight: 600; }
  details summary { cursor: pointer; }
  details ul { margin: 6px 0 0; padding-left: 18px; columns: 2; }
  .group { margin: 8px 0 0; font-size: 12px; font-weight: 600; }
  .group.shared { color: var(--orange); }
  .group.exposures { color: var(--red); }
  .group.all { color: var(--muted); }
  pre { white-space: pre-wrap; word-break: break-word; font-size: 12px; margin: 6px 0 0;
        background: var(--code-bg); padding: 8px; border-radius: 6px; }
  .empty { padding: 16px; margin: 0; color: var(--green); }
  .legend { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 8px; font-size: 12px; color: var(--muted); }
  a { color: inherit; }
  a.out { text-decoration: none; }
  a.out:hover { text-decoration: underline; }
  .link { font-size: 12px; color: var(--muted); white-space: nowrap; margin-left: 6px; }
  .stale { display: none; background: var(--red); color: #fff; }
  .stale.on { display: block; }
  .stale .inner { max-width: 1200px; margin: 0 auto; padding: 10px 16px; font-weight: 600; }
  .jobs { display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 12px; }
  .job-card { background: var(--surface); border: 1px solid var(--line); border-left: 4px solid var(--state);
              border-radius: 8px; padding: 10px 12px; cursor: pointer; color: var(--muted); font-size: 13px; --state: var(--green); }
  .job-card.failing { --state: var(--orange); }
  .job-card.overdue { --state: var(--red); }
  .job-card.selected { outline: 2px solid var(--state); }
  .job-card .head { display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-bottom: 4px; }
  .job-card .state { font-weight: 600; color: var(--state); }
  h2.jobs-h { --dot: var(--muted); }
  .filter-note { font-size: 12px; color: var(--muted); margin-top: 8px; min-height: 18px; }
  details.section > summary { list-style: none; cursor: pointer; }
  details.section > summary::-webkit-details-marker { display: none; }
  details.section > summary h2::after { content: "\25B8"; color: var(--muted); font-size: 12px; }
  details.section[open] > summary h2::after { content: "\25BE"; }
  tr.hidden { display: none; }
  .legend span::before { content: ""; display: inline-block; width: 8px; height: 8px; border-radius: 2px; margin-right: 4px; background: var(--c); }
</style>
</head>
<body>
<header>
  <div class="inner">
    <div>
      <h1>Incident report</h1>
      <p>Generated {{ generated_at }} ({{ tz_name }})<span id="age"></span> · an incident is a model's unbroken run of failed runs within one job · last {{ window_days }} days</p>
    </div>
    {% if open_incidents %}<span class="status bad">{{ open_incidents | length }} open</span>{% else %}<span class="status ok">All clear</span>{% endif %}
  </div>
</header>
<div class="stale" id="stale"><div class="inner">This report is <span id="stale-age"></span> old. It is refreshed by a scheduled job, so that job may have stopped running.</div></div>
<main>
  <div class="summary">
    <div class="tile {{ 'red' if open_incidents else 'quiet' }}"><b>{{ open_incidents | length }}</b>open incidents</div>
    <div class="tile {{ 'orange' if downstream_total else 'quiet' }}"><b>{{ downstream_total }}</b>downstream models</div>
    <div class="tile green"><b>{{ cleared | length }}</b>cleared in {{ window_days }} days</div>
  </div>

  {% if jobs %}
  <h2 class="jobs-h">Jobs</h2>
  <div class="jobs">
    {% for j in jobs %}
    <div class="job-card{% if j.is_overdue %} overdue{% elif j.open_count %} failing{% endif %}" data-job="{{ j.job_id }}"
         data-last-run="{{ j.last_run_iso }}" data-overdue-after="{{ j.overdue_after_seconds }}" title="Show only this job's incidents">
      <div class="head">
        <span class="job job-{{ j.job_color }}">{{ j.job_id }}</span>
        {% if j.job_url %}<a class="link out" href="{{ j.job_url }}" target="_blank" rel="noopener">Airflow &#8599;</a>{% endif %}
      </div>
      <div>Last run {{ j.last_run }}<span class="ago"></span></div>
      {% if j.interval %}<div>Runs every {{ j.interval }}</div>{% endif %}
      <div class="state">{% if j.is_overdue %}Not running{% elif j.open_count %}{{ j.open_count }} open{% else %}OK{% endif %}</div>
    </div>
    {% endfor %}
  </div>
  <div class="filter-note" id="filter-note"></div>
  {% endif %}

  <h2 class="open">Open</h2>
  {% if open_incidents %}
  <div class="scroll"><table>
    <thead><tr><th>Job</th><th>Model</th><th>Failing since</th><th>For</th><th>Failed runs</th><th>Cause</th><th>Blast radius</th></tr></thead>
    <tbody>
    {% for i in open_incidents %}
      <tr class="sev-{{ i.severity }}" data-job="{{ i.job_id }}">
        <td><span class="job job-{{ i.job_color }}">{{ i.job_id }}</span></td>
        <td>{% if i.model_url %}<a class="out" href="{{ i.model_url }}" target="_blank" rel="noopener"><code>{{ i.name }}</code></a>{% else %}<code>{{ i.name }}</code>{% endif %}</td>
        <td class="when">{% if i.is_start_truncated %}before {% endif %}{{ i.first_failed }}</td>
        <td class="num"><span class="badge">{{ i.duration }}</span></td>
        <td class="num">{{ i.failed_runs }}</td>
        <td><details><summary>{{ i.cause }}</summary><pre>{{ i.message }}</pre></details>{% if i.query_url %}<a class="link out" href="{{ i.query_url }}" target="_blank" rel="noopener">BigQuery job &#8599;</a>{% endif %}</td>
        <td>
          {% if i.downstream %}
          <details><summary><span class="radius">{{ i.downstream | length }} downstream</span>{% if i.shared %}, {{ i.shared | length }} public/protected{% endif %}{% if i.exposures %}, {{ i.exposures | length }} exposures{% endif %}</summary>
            {% if i.exposures %}<p class="group exposures">Exposures</p><ul>{% for n in i.exposures %}<li>{{ n }}</li>{% endfor %}</ul>{% endif %}
            {% if i.shared %}<p class="group shared">Public/protected</p><ul>{% for n in i.shared %}<li><code>{{ n }}</code></li>{% endfor %}</ul>{% endif %}
            <p class="group all">All downstream</p><ul>{% for n in i.downstream %}<li><code>{{ n }}</code></li>{% endfor %}</ul>
          </details>
          {% else %}<span class="meta">nothing downstream</span>{% endif %}
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table></div>
  <div class="legend">
    <span style="--c: var(--yellow)">under 3h</span>
    <span style="--c: var(--orange)">3–12h</span>
    <span style="--c: var(--red)">12h or more</span>
  </div>
  {% else %}
  <div class="scroll"><p class="empty">No open incidents.</p></div>
  {% endif %}

  {% if cleared %}
  <details class="section">
  <summary><h2 class="cleared">Cleared ({{ cleared | length }} in {{ window_days }} days)</h2></summary>
  <div class="scroll"><table>
    <thead><tr><th>Job</th><th>Model</th><th>First failed</th><th>Cleared</th><th>Lasted</th><th>Failed runs</th><th>Cause</th></tr></thead>
    <tbody>
    {% for i in cleared %}
      <tr class="cleared-row" data-job="{{ i.job_id }}">
        <td><span class="job job-{{ i.job_color }}">{{ i.job_id }}</span></td>
        <td>{% if i.model_url %}<a class="out" href="{{ i.model_url }}" target="_blank" rel="noopener"><code>{{ i.name }}</code></a>{% else %}<code>{{ i.name }}</code>{% endif %}</td>
        <td class="when">{% if i.is_start_truncated %}before {% endif %}{{ i.first_failed }}</td>
        <td class="when">{{ i.cleared }}</td>
        <td class="num"><span class="badge">{{ i.duration }}</span></td>
        <td class="num">{{ i.failed_runs }}</td>
        <td><details><summary>{{ i.cause }}</summary><pre>{{ i.message }}</pre></details></td>
      </tr>
    {% endfor %}
    </tbody>
  </table></div>
  </details>
  {% else %}
  <h2 class="cleared">Cleared</h2>
  <div class="scroll"><p class="empty">Nothing cleared in the last {{ window_days }} days.</p></div>
  {% endif %}
</main>
<script>
(function () {
  // Ages are worked out against the viewer's clock, so a page nobody has refreshed
  // still says how old it is, and which jobs have stopped running since.
  var now = Date.now();
  function ago(ms) {
    var m = Math.round(ms / 60000);
    if (m < 60) return m + "m";
    var h = Math.round(m / 60);
    return h < 48 ? h + "h" : Math.round(h / 24) + "d";
  }
  var age = now - Date.parse("{{ generated_at_iso }}");
  document.getElementById("age").textContent = " \u00b7 " + ago(age) + " ago";
  if (age > {{ stale_seconds }} * 1000) {
    document.getElementById("stale-age").textContent = ago(age);
    document.getElementById("stale").classList.add("on");
  }
  var cards = document.querySelectorAll(".job-card");
  cards.forEach(function (card) {
    var since = now - Date.parse(card.dataset.lastRun);
    card.querySelector(".ago").textContent = " \u00b7 " + ago(since) + " ago";
    var limit = Number(card.dataset.overdueAfter);
    if (limit && since > limit * 1000) {
      card.classList.remove("failing");
      card.classList.add("overdue");
      card.querySelector(".state").textContent = "Not running";
    }
  });

  // Clicking a job shows only its incidents; clicking it again shows all.
  var selected = null;
  var note = document.getElementById("filter-note");
  cards.forEach(function (card) {
    card.addEventListener("click", function (event) {
      if (event.target.closest("a")) return;
      selected = selected === card.dataset.job ? null : card.dataset.job;
      cards.forEach(function (c) { c.classList.toggle("selected", c.dataset.job === selected); });
      document.querySelectorAll("tr[data-job]").forEach(function (row) {
        row.classList.toggle("hidden", selected !== null && row.dataset.job !== selected);
      });
      note.textContent = selected ? "Showing " + selected + " only. Click it again to show all jobs." : "";
    });
  });
})();
</script>
</body>
</html>
