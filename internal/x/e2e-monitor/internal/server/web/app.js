/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

'use strict';

// ---------------------------------------------------------------------------
// Helpers. All text from the cluster goes through textContent, never HTML.

function el(tag, attrs, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === 'class') e.className = v;
    else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else if (k === 'style') e.style.cssText = v;
    else e.setAttribute(k, v === true ? '' : v);
  }
  for (const c of children.flat()) {
    if (c === undefined || c === null || c === false) continue;
    e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return e;
}

const SVG = 'http://www.w3.org/2000/svg';
function svg(tag, attrs, ...children) {
  const e = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of children) e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return e;
}

function pad(n, w = 2) { return String(n).padStart(w, '0'); }
function fmtTime(t) {
  if (!t) return '';
  const d = new Date(t);
  if (isNaN(d)) return '';
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}
function age(t) {
  if (!t) return '';
  const s = Math.max(0, (Date.now() - new Date(t)) / 1000);
  if (s < 60) return `${Math.floor(s)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  return `${Math.floor(s / 86400)}d`;
}
function secs(x) { return x ? `${x.toFixed(1)}s` : ''; }

async function api(path) {
  const r = await fetch(path, { cache: 'no-store' });
  if (!r.ok) throw new Error(`${path}: ${r.status} ${await r.text()}`);
  return r.json();
}

function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

function dot(cls, title) { return el('span', { class: `dot ${cls || ''}`, title: title || cls }); }

function table(headers, rows) {
  return el('table', {},
    el('thead', {}, el('tr', {}, headers.map(h => el('th', {}, h)))),
    el('tbody', {}, rows));
}

const STATE_ORDER = { fail: 0, interrupted: 0, running: 1, paused: 1, pass: 2, skip: 3 };

// ---------------------------------------------------------------------------
// Global state, refreshed on server-sent change events.

const S = {
  info: null,
  components: [],
  pools: [],
  e2e: null,
  substrate: null,
  view: null, // the mounted view: {update(topic), destroy()}
  routeKey: '',
};

async function load(topic) {
  try {
    if (!topic || topic === 'components') {
      [S.components, S.pools] = await Promise.all([api('api/components'), api('api/pools')]);
    }
    if (!topic || topic === 'e2e') S.e2e = await api('api/e2e');
    if (!topic || topic === 'substrate') S.substrate = await api('api/substrate');
  } catch (err) {
    console.error(err);
  }
}

function connectEvents() {
  const conn = document.getElementById('conn');
  const es = new EventSource('api/events');
  es.onopen = () => { conn.textContent = 'live'; conn.className = 'conn live'; };
  es.onerror = () => { conn.textContent = 'disconnected – retrying'; conn.className = 'conn down'; };
  for (const topic of ['components', 'e2e', 'substrate']) {
    es.addEventListener(topic, async () => {
      await load(topic);
      renderTabs();
      if (S.view) S.view.update(topic);
    });
  }
}

// ---------------------------------------------------------------------------
// Routing: #/overview, #/c/<component>, #/workers, #/e2e[/<test id>]

function route() {
  const parts = (location.hash.replace(/^#\/?/, '') || 'overview').split('/');
  const kind = parts[0];
  const arg = decodeURIComponent(parts.slice(1).join('/'));
  return { kind, arg };
}

function renderTabs() {
  const nav = document.getElementById('tabs');
  const r = route();
  const tab = (href, label, active, cls) => el('a', { href, class: active ? 'active' : '' }, cls !== undefined ? dot(cls) : null, label);
  const tabs = [tab('#/overview', 'Overview', r.kind === 'overview')];
  for (const c of S.components) {
    tabs.push(tab(`#/c/${encodeURIComponent(c.name)}`, c.name, r.kind === 'c' && r.arg === c.name, c.health));
  }
  const poolHealth = S.pools.some(p => p.ready < p.replicas) ? 'degraded' : 'ok';
  tabs.push(tab('#/workers', `Workers (${S.pools.length})`, r.kind === 'workers', S.pools.length ? poolHealth : 'unknown'));
  let e2eCls = 'unknown';
  if (S.e2e && S.e2e.counts.total) {
    e2eCls = S.e2e.counts.fail ? 'fail' : (S.e2e.active ? 'running' : 'pass');
  }
  tabs.push(tab('#/e2e', 'E2E', r.kind === 'e2e', e2eCls));
  nav.replaceChildren(...tabs);
}

function render() {
  const r = route();
  renderTabs();
  // Selecting another test stays within the mounted E2E view.
  const key = r.kind === 'e2e' ? 'e2e' : `${r.kind}/${r.arg}`;
  if (key === S.routeKey && S.view) {
    S.view.route && S.view.route(r);
    return;
  }
  if (S.view && S.view.destroy) S.view.destroy();
  S.routeKey = key;
  const main = document.getElementById('view');
  main.replaceChildren();
  switch (r.kind) {
    case 'c': S.view = componentView(main, r.arg); break;
    case 'workers': S.view = workersView(main); break;
    case 'e2e': S.view = e2eView(main, r); break;
    default: S.view = overviewView(main);
  }
}

// ---------------------------------------------------------------------------
// Log viewer.

const LEVELS = ['', 'DEBUG', 'INFO', 'WARN', 'ERROR'];
const MAX_LINES = 5000;

class LogView {
  // opts: {component, pods: [Pod], tall, placeholder}
  constructor(opts) {
    this.opts = opts;
    this.pod = opts.pod || '';
    this.container = '';
    this.level = '';
    this.q = '';
    this.follow = true;
    this.paused = false;
    this.pending = [];
    this.count = 0;

    this.podSel = el('select', { onchange: () => { this.pod = this.podSel.value; this.container = ''; this.fillContainers(); this.connect(); } });
    this.contSel = el('select', { onchange: () => { this.container = this.contSel.value; this.connect(); } });
    const levelSel = el('select', { onchange: e => { this.level = e.target.value; this.connect(); } },
      LEVELS.map(l => el('option', { value: l }, l ? `≥ ${l}` : 'all levels')));
    const search = el('input', { type: 'search', placeholder: 'filter text…', oninput: debounce(e => { this.q = e.target.value; this.connect(); }, 300) });
    this.followBtn = el('button', { class: 'on', onclick: () => this.setFollow(!this.follow), title: 'Keep scrolled to the newest line' }, 'follow');
    this.pauseBtn = el('button', { onclick: () => this.setPaused(!this.paused) }, 'pause');
    this.prevBtn = el('button', { onclick: () => this.previous(), title: 'Logs of the previous (crashed) container instance' }, 'previous');
    this.countEl = el('span', { class: 'sub', style: 'margin:0' });
    this.box = el('div', { class: `logs ${opts.tall ? 'tall' : ''}` });
    this.box.addEventListener('scroll', () => {
      const atBottom = this.box.scrollTop + this.box.clientHeight >= this.box.scrollHeight - 20;
      if (atBottom !== this.follow) this.setFollow(atBottom);
    });
    this.root = el('div', {},
      el('div', { class: 'toolbar' }, this.podSel, this.contSel, levelSel, search, this.followBtn, this.pauseBtn, this.prevBtn, this.countEl),
      this.box);
    this.setPods(opts.pods || []);
    this.connect();
  }

  setPods(pods) {
    this.pods = pods;
    const opts = [];
    if (this.opts.component) opts.push(el('option', { value: '' }, `all pods (${pods.length})`));
    for (const p of pods) opts.push(el('option', { value: `${p.namespace}/${p.name}` }, `${p.name}${p.ready ? ` (${p.ready})` : ''}`));
    if (!this.opts.component && !pods.length) opts.push(el('option', { value: '' }, '—'));
    this.podSel.replaceChildren(...opts);
    this.podSel.value = this.pod;
    if (this.podSel.value !== this.pod && this.pod) {
      // The pod is gone; keep showing its buffered lines.
      this.podSel.append(el('option', { value: this.pod }, `${this.pod} (gone)`));
      this.podSel.value = this.pod;
    }
    this.fillContainers();
  }

  fillContainers() {
    const p = (this.pods || []).find(p => `${p.namespace}/${p.name}` === this.pod);
    const opts = [el('option', { value: '' }, 'all containers')];
    for (const c of (p ? p.containers : [])) {
      opts.push(el('option', { value: c.name }, `${c.name}${c.init && !c.sidecar ? ' (init)' : ''}`));
    }
    this.contSel.replaceChildren(...opts);
    this.contSel.value = this.container;
    this.contSel.disabled = !this.pod;
    this.prevBtn.disabled = !this.pod || !this.container;
  }

  query() {
    const q = new URLSearchParams();
    if (this.pod) q.set('pod', this.pod);
    else if (this.opts.component) q.set('component', this.opts.component);
    if (this.pod && this.container) q.set('container', this.container);
    if (this.level) q.set('level', this.level);
    if (this.q) q.set('q', this.q);
    q.set('limit', '2000');
    return q;
  }

  connect() {
    if (this.es) this.es.close();
    this.prevBtn.disabled = !this.pod || !this.container;
    if (!this.pod && !this.opts.component) {
      this.box.replaceChildren(el('div', { class: 'empty' }, this.opts.placeholder || 'Select a pod.'));
      this.countEl.textContent = '';
      return;
    }
    this.box.replaceChildren(el('div', { class: 'empty' }, 'loading…'));
    this.es = new EventSource(`api/logs/stream?${this.query()}`);
    this.es.addEventListener('backlog', e => {
      const lines = JSON.parse(e.data) || [];
      this.box.replaceChildren();
      this.count = 0;
      if (!lines.length) this.box.append(el('div', { class: 'empty' }, 'No lines yet. New lines appear here as they arrive.'));
      this.append(lines);
    });
    this.es.addEventListener('lines', e => {
      const lines = JSON.parse(e.data) || [];
      if (this.paused) { this.pending.push(...lines); this.updateCount(); return; }
      const empty = this.box.querySelector('.empty');
      if (empty) empty.remove();
      this.append(lines);
    });
  }

  append(lines) {
    const frag = document.createDocumentFragment();
    for (const l of lines) frag.append(this.renderLine(l));
    this.box.append(frag);
    this.count += lines.length;
    while (this.box.childElementCount > MAX_LINES) { this.box.firstChild.remove(); }
    this.updateCount();
    if (this.follow) this.box.scrollTop = this.box.scrollHeight;
  }

  updateCount() {
    this.countEl.textContent = `${Math.min(this.count, MAX_LINES)} lines${this.pending.length ? `, ${this.pending.length} paused` : ''}`;
  }

  renderLine(l) {
    const src = this.pod ? l.container : `${l.pod}/${l.container}`;
    let msg = l.msg || l.raw;
    let kv = '';
    let parsed = null;
    if (l.raw && l.raw[0] === '{') {
      try { parsed = JSON.parse(l.raw); } catch (e) { /* not JSON */ }
    }
    if (parsed && l.msg) {
      kv = Object.entries(parsed)
        .filter(([k]) => !['time', 'level', 'msg', 'message'].includes(k))
        .map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`)
        .join(' ');
      if (kv.length > 400) kv = kv.slice(0, 400) + '…';
    }
    const row = el('div', { class: `ll ${l.level || ''}`, title: `${l.namespace}/${l.pod}/${l.container}` },
      el('span', { class: 't' }, fmtTime(l.time)),
      el('span', { class: 'src' }, src),
      el('span', { class: `lv ${l.level || ''}` }, (l.level || '').slice(0, 5)),
      el('span', { class: 'm' }, msg, kv ? el('span', { class: 'kv' }, '  ' + kv) : null));
    row.addEventListener('click', () => {
      const m = row.querySelector('.m');
      if (row.classList.toggle('expanded')) {
        m.replaceChildren(parsed ? JSON.stringify(parsed, null, 2) : l.raw);
      } else {
        m.replaceChildren(msg, kv ? el('span', { class: 'kv' }, '  ' + kv) : '');
      }
    });
    return row;
  }

  setFollow(on) {
    this.follow = on;
    this.followBtn.classList.toggle('on', on);
    if (on) this.box.scrollTop = this.box.scrollHeight;
  }

  setPaused(on) {
    this.paused = on;
    this.pauseBtn.classList.toggle('on', on);
    this.pauseBtn.textContent = on ? 'resume' : 'pause';
    if (!on && this.pending.length) {
      const lines = this.pending;
      this.pending = [];
      this.append(lines);
    }
  }

  async previous() {
    if (!this.pod || !this.container) return;
    const q = new URLSearchParams({ pod: this.pod, container: this.container });
    const r = await fetch(`api/logs/previous?${q}`);
    const text = await r.text();
    const w = window.open('', '_blank');
    if (w) {
      w.document.title = `${this.pod}/${this.container} (previous)`;
      const pre = w.document.createElement('pre');
      pre.textContent = r.ok ? text : `error: ${text}`;
      w.document.body.append(pre);
    }
  }

  selectPod(key) {
    this.pod = key;
    this.container = '';
    this.setPods(this.pods || []);
    this.connect();
  }

  destroy() { if (this.es) this.es.close(); }
}

// ---------------------------------------------------------------------------
// Overview.

function overviewView(main) {
  const body = el('div');
  main.append(el('h2', {}, 'Overview'), el('div', { class: 'sub' }, 'Every substrate workload discovered in the watched namespaces, the worker pools, and the attached e2e run.'), body);
  function update() {
    const cards = S.components.map(c => {
      const restarts = c.pods.reduce((n, p) => n + p.restarts, 0);
      return el('div', { class: 'card', onclick: () => { location.hash = `#/c/${encodeURIComponent(c.name)}`; } },
        el('div', { class: 'title' }, dot(c.health), c.name),
        el('div', { class: 'meta' }, `${c.kind} · ${c.namespace}`),
        el('div', { class: 'big' }, `${c.ready}/${c.desired} ready`),
        el('div', { class: 'meta' }, `${c.pods.length} pods · ${restarts} restarts`,
          c.events && c.events.some(e => e.type === 'Warning') ? el('span', { class: 'badge warn', style: 'margin-left:6px' }, 'warnings') : null));
    });
    const workerPods = S.pools.reduce((n, p) => n + p.pods.length, 0);
    const e2ePools = S.pools.filter(p => p.e2e).length;
    cards.push(el('div', { class: 'card', onclick: () => { location.hash = '#/workers'; } },
      el('div', { class: 'title' }, dot(S.pools.some(p => p.ready < p.replicas) ? 'degraded' : 'ok'), 'Workers'),
      el('div', { class: 'meta' }, `${S.pools.length} pools (${e2ePools} e2e)`),
      el('div', { class: 'big' }, `${workerPods} worker pods`),
      el('div', { class: 'meta' }, S.substrate && !S.substrate.apiError ? `${(S.substrate.workers || []).length} registered · ${(S.substrate.actors || []).length} actors` : 'ateapi state unavailable')));
    const e = S.e2e;
    cards.push(el('div', { class: 'card', onclick: () => { location.hash = '#/e2e'; } },
      el('div', { class: 'title' }, dot(!e || !e.counts.total ? 'unknown' : e.counts.fail ? 'fail' : e.active ? 'running' : 'pass'), 'E2E run'),
      el('div', { class: 'meta' }, e && e.source ? e.source : 'no run attached'),
      el('div', { class: 'big' }, e && e.counts.total ? `${e.counts.pass} pass · ${e.counts.fail} fail · ${e.counts.running} running` : '—'),
      el('div', { class: 'meta' }, e && e.active ? 'in progress' : e && e.end ? `finished ${age(e.end)} ago` : '')));
    body.replaceChildren(el('div', { class: 'cards' }, cards));
  }
  update();
  return { update };
}

// ---------------------------------------------------------------------------
// Component tab.

function podsTable(pods, onSelect) {
  return table(['Pod', 'Ready', 'Status', 'Restarts', 'Node', 'Age', 'Containers'], pods.map(p => {
    const status = p.reason || p.phase;
    const bad = p.reason && p.reason !== 'Terminating';
    return el('tr', { class: onSelect ? 'clickable' : '', onclick: onSelect ? () => onSelect(p) : null, title: onSelect ? 'Show this pod\'s logs' : null },
      el('td', { class: 'mono' }, p.name),
      el('td', {}, p.ready),
      el('td', {}, el('span', { class: `badge ${bad ? 'err' : p.phase === 'Running' ? 'ok' : ''}` }, status)),
      el('td', {}, p.restarts ? el('span', { class: 'badge warn' }, p.restarts) : '0'),
      el('td', { class: 'mono' }, p.node || ''),
      el('td', {}, age(p.created)),
      el('td', {}, el('div', { class: 'chips' }, p.containers.filter(c => !c.init || c.sidecar).map(c =>
        el('span', { class: `badge ${c.state === 'running' && c.ready ? 'ok' : c.state === 'waiting' || c.state === 'terminated' ? 'err' : ''}`, title: `${c.image}\n${c.state || 'unknown'} ${c.reason || ''}` }, c.name)))));
  }));
}

function eventsTable(events) {
  if (!events || !events.length) return el('div', { class: 'empty' }, 'No recent events.');
  return table(['Last seen', 'Type', 'Reason', 'Object', 'Message'], events.map(e => el('tr', {},
    el('td', {}, `${age(e.time)} ago`),
    el('td', {}, el('span', { class: `badge ${e.type === 'Warning' ? 'warn' : ''}` }, e.type)),
    el('td', {}, e.reason),
    el('td', { class: 'mono' }, e.object),
    el('td', {}, e.message, e.count > 1 ? el('span', { class: 'sub' }, ` (×${e.count})`) : ''))));
}

function routerPanel() {
  const s = S.substrate;
  if (!s) return el('div', { class: 'empty' }, 'loading…');
  if (s.routerError) return el('div', { class: 'banner err' }, `router /statusz: ${s.routerError}`);
  if (!s.router) return el('div', { class: 'empty' }, 'No /statusz data yet.');
  const r = s.router;
  const queries = (r.queries || []).slice(-50).reverse();
  return el('div', {},
    el('div', { class: 'chips' },
      el('span', { class: 'badge' }, `mode ${r.mode || '?'}`),
      el('span', { class: 'badge' }, `build ${r.build_tag || '?'}`),
      r.parking ? el('span', { class: 'badge' }, `parking ${r.parking.enabled ? 'on' : 'off'} · ${r.parking.active || 0}/${r.parking.max_parked || 0} parked`) : null),
    r.health ? el('pre', { class: 'json' }, JSON.stringify(r.health, null, 2)) : null,
    queries.length ? table(['Time', 'Method', 'Host', 'Path', 'Action', 'Target', 'Duration'], queries.map(q => el('tr', {},
      el('td', { class: 'mono' }, q.timestamp), el('td', {}, q.method), el('td', { class: 'mono' }, q.host),
      el('td', { class: 'mono' }, q.path), el('td', {}, q.action), el('td', { class: 'mono' }, q.target), el('td', {}, q.duration)))) :
      el('div', { class: 'empty' }, 'No recent queries.'));
}

function componentView(main, name) {
  const hdr = el('div');
  const pods = el('div');
  const statusz = el('div');
  const events = el('div');
  let logView = null;
  const logsBox = el('div');
  main.append(hdr, el('h3', {}, 'Pods'), pods, statusz, el('h3', {}, 'Logs'), logsBox, el('h3', {}, 'Events'), events);

  function update(topic) {
    const c = S.components.find(c => c.name === name);
    if (!c) {
      hdr.replaceChildren(el('h2', {}, name), el('div', { class: 'banner err' }, 'This component no longer exists.'));
      return;
    }
    if (!topic || topic === 'components') {
      hdr.replaceChildren(
        el('h2', {}, dot(c.health), ' ', c.name),
        el('div', { class: 'sub' }, `${c.kind} in ${c.namespace} · ${c.ready}/${c.desired} ready`, c.description ? ` · ${c.description}` : ''),
        c.images && c.images.length ? el('div', { class: 'chips' }, c.images.map(i => el('span', { class: 'badge mono' }, i))) : null);
      pods.replaceChildren(c.pods.length ? podsTable(c.pods, p => logView.selectPod(`${p.namespace}/${p.name}`)) : el('div', { class: 'empty' }, 'No pods.'));
      events.replaceChildren(eventsTable(c.events));
      if (!logView) {
        logView = new LogView({ component: c.name, pods: c.pods, tall: true });
        logsBox.append(logView.root);
      } else {
        logView.setPods(c.pods);
      }
    }
    if (c.statuszPort && (!topic || topic === 'substrate')) {
      statusz.replaceChildren(el('h3', {}, '/statusz'), routerPanel());
    }
  }
  update();
  return { update, destroy: () => logView && logView.destroy() };
}

// ---------------------------------------------------------------------------
// Workers tab.

function workersView(main) {
  const expanded = new Set();
  let showAll = false;
  const pools = el('div');
  const apiBox = el('div');
  const actorFilter = el('input', { type: 'search', placeholder: 'filter actors…', oninput: debounce(() => renderAPI(), 200) });
  const logView = new LogView({ pods: [], placeholder: 'Select a worker pod above to stream its logs (followed on demand).' });
  main.append(
    el('h2', {}, 'Workers'),
    el('div', { class: 'sub' }, `WorkerPools and their pods. Worker logs are followed continuously per --follow-workers=${S.info ? S.info.followWorkers : '?'}; any pod you open here is followed on demand.`),
    pools,
    el('h3', {}, 'Worker logs'), logView.root,
    el('h3', {}, 'ateapi state'), actorFilter, apiBox);

  function allPods() { return S.pools.flatMap(p => p.pods); }

  function renderPools() {
    const visible = S.pools.filter(p => showAll || !p.e2e || p.pods.length);
    const rows = [];
    for (const p of visible) {
      const key = `${p.namespace}/${p.name}`;
      const open = expanded.has(key);
      rows.push(el('tr', { class: 'clickable', onclick: () => { open ? expanded.delete(key) : expanded.add(key); renderPools(); } },
        el('td', {}, open ? '▾ ' : '▸ ', el('span', { class: 'mono' }, p.namespace)),
        el('td', { class: 'mono' }, p.name),
        el('td', {}, dot(p.ready >= p.replicas && p.replicas ? 'ok' : p.ready ? 'degraded' : 'down'), ` ${p.ready}/${p.replicas}`),
        el('td', {}, p.pods.length),
        el('td', {}, p.sandboxClass || ''),
        el('td', {}, p.e2e ? el('span', { class: 'badge e2e' }, 'e2e') : '', ' ', p.test ? el('a', { onclick: ev => { ev.stopPropagation(); gotoTestByName(p.test); } }, p.test) : '')));
      if (open) {
        rows.push(el('tr', {}, el('td', { colspan: 6, style: 'padding:0 0 8px 24px' },
          p.pods.length ? podsTable(p.pods, pod => { logView.pods = allPods(); logView.selectPod(`${pod.namespace}/${pod.name}`); }) : el('div', { class: 'empty' }, 'No pods.'))));
      }
    }
    pools.replaceChildren(
      el('div', { class: 'toolbar' }, el('label', {}, el('input', { type: 'checkbox', checked: showAll, onchange: e => { showAll = e.target.checked; renderPools(); } }), 'show empty e2e pools')),
      rows.length ? table(['Namespace', 'Pool', 'Ready', 'Pods', 'Sandbox', 'E2E'], rows) : el('div', { class: 'empty' }, 'No worker pools found.'));
    logView.setPods(allPods());
  }

  function renderAPI() {
    const s = S.substrate;
    if (!s) { apiBox.replaceChildren(el('div', { class: 'empty' }, 'loading…')); return; }
    const f = actorFilter.value.toLowerCase();
    const actors = (s.actors || []).filter(a => !f || `${a.atespace}/${a.name} ${a.state} ${a.workerPod || ''}`.toLowerCase().includes(f));
    apiBox.replaceChildren(
      s.apiError ? el('div', { class: 'banner err' }, s.apiError) : null,
      el('div', { class: 'grid2' },
        el('div', {}, el('h3', {}, `Registered workers (${(s.workers || []).length})`),
          table(['Pod', 'Pool', 'State', 'Actors'], (s.workers || []).map(w => el('tr', {},
            el('td', { class: 'mono' }, `${w.namespace}/${w.pod}`), el('td', {}, w.pool),
            el('td', {}, el('span', { class: `badge ${w.state === 'active' ? 'ok' : 'warn'}` }, w.state)),
            el('td', { class: 'mono' }, (w.actors || []).join(', ')))))),
        el('div', {}, el('h3', {}, `Actors (${actors.length})`),
          table(['Actor', 'State', 'Worker', 'Updated'], actors.slice(0, 500).map(a => el('tr', { title: a.crash || '' },
            el('td', { class: 'mono' }, `${a.atespace}/${a.name}`),
            el('td', {}, el('span', { class: `badge ${a.state === 'running' ? 'ok' : a.state === 'crashed' ? 'err' : ''}` }, a.state)),
            el('td', { class: 'mono' }, a.workerPod || ''),
            el('td', {}, a.updated ? `${age(a.updated)} ago` : '')))))));
  }

  function update(topic) {
    if (!topic || topic === 'components') renderPools();
    if (!topic || topic === 'substrate') renderAPI();
  }
  update();
  return { update, destroy: () => logView.destroy() };
}

// ---------------------------------------------------------------------------
// E2E tab.

function gotoTestByName(name) {
  const t = S.e2e && S.e2e.tests.find(t => t.name === name);
  if (t) location.hash = `#/e2e/${encodeURIComponent(t.package + '.' + t.name)}`;
  else location.hash = '#/e2e';
}

function shortPkg(p) { return p ? p.split('/').slice(-2).join('/') : '(unattributed output)'; }

function e2eView(main, r) {
  let selected = r.arg || '';
  let sub = 'timeline';
  let ownScope = false;
  let lastDetail = 0;
  let detailTimer = null;
  const hiddenLanes = new Set();
  const left = el('div');
  const right = el('div');
  main.append(el('div', { class: 'split' }, left, right));

  function renderLeft() {
    const e = S.e2e;
    if (!e || (!e.counts.total && !(e.packages || []).length)) {
      left.replaceChildren(el('h2', {}, 'E2E'), el('div', { class: 'banner' },
        el('div', {}, 'No e2e run attached. Start one through the monitor:'),
        el('pre', { class: 'json' }, 'go run ./internal/x/e2e-monitor run -- hack/run-e2e.sh -run TestDemo'),
        el('div', {}, 'or follow a test2json file another run is writing:'),
        el('pre', { class: 'json' }, 'go run ./internal/x/e2e-monitor serve --e2e-json=/tmp/e2e.json')));
      return;
    }
    const byPkg = new Map();
    for (const t of e.tests) {
      if (!byPkg.has(t.package)) byPkg.set(t.package, []);
      byPkg.get(t.package).push(t);
    }
    const pkgState = new Map((e.packages || []).map(p => [p.name, p]));
    const tree = el('div', { class: 'tree' });
    const pkgs = [...new Set([...byPkg.keys(), ...pkgState.keys()])].sort();
    for (const pkg of pkgs) {
      const p = pkgState.get(pkg);
      tree.append(el('div', { class: 'pkg' }, dot(p ? p.state : 'unknown'), shortPkg(pkg), el('span', { style: 'flex:1' }), p ? secs(p.elapsed) : ''));
      const tests = (byPkg.get(pkg) || []).slice();
      for (const t of tests) {
        const id = `${t.package}.${t.name}`;
        const depth = t.name.split('/').length - 1;
        tree.append(el('div', {
          class: `row ${id === selected ? 'selected' : ''}`,
          style: `padding-left:${8 + depth * 16}px`,
          onclick: () => { location.hash = `#/e2e/${encodeURIComponent(id)}`; },
          title: t.name,
        }, dot(t.state), el('span', { class: 'name' }, depth ? t.name.split('/').pop() : t.name),
        t.state === 'running' && t.current ? el('span', { class: 'cur', title: t.current }, t.current) : null,
        el('span', { class: 'sub', style: 'margin:0' }, t.elapsed ? secs(t.elapsed) : t.state === 'running' ? age(t.start) : '')));
      }
    }
    left.replaceChildren(
      el('h2', {}, 'E2E ', e.active ? el('span', { class: 'badge warn' }, 'running') : el('span', { class: `badge ${e.counts.fail ? 'err' : 'ok'}` }, 'finished')),
      el('div', { class: 'sub mono', title: e.source }, e.source || ''),
      e.exitErr ? el('div', { class: 'banner err' }, e.exitErr) : null,
      el('div', { class: 'counts' },
        el('div', {}, el('b', {}, e.counts.running), 'running'),
        el('div', {}, el('b', { style: 'color:var(--ok)' }, e.counts.pass), 'pass'),
        el('div', {}, el('b', { style: 'color:var(--err)' }, e.counts.fail), 'fail'),
        el('div', {}, el('b', {}, e.counts.skip), 'skip')),
      tree);
  }

  async function renderRight() {
    lastDetail = Date.now();
    if (!selected) {
      const failed = S.e2e ? S.e2e.tests.filter(t => t.state === 'fail' || t.state === 'interrupted') : [];
      right.replaceChildren(el('div', { class: 'empty' }, 'Select a test to see its output and how it moved through the components.'),
        failed.length ? el('div', {}, el('h3', {}, 'Failures'), el('div', { class: 'chips' }, failed.map(t =>
          el('a', { class: 'badge err', href: `#/e2e/${encodeURIComponent(t.package + '.' + t.name)}` }, t.name)))) : null);
      return;
    }
    let t, tl;
    try {
      const q = encodeURIComponent(selected);
      [t, tl] = await Promise.all([api(`api/e2e/test?id=${q}`), sub === 'timeline' ? api(`api/e2e/timeline?id=${q}${ownScope ? '&scope=own' : ''}`) : null]);
    } catch (err) {
      right.replaceChildren(el('div', { class: 'banner err' }, String(err)));
      return;
    }
    const tabs = el('div', { class: 'toolbar' },
      el('button', { class: sub === 'timeline' ? 'on' : '', onclick: () => { sub = 'timeline'; renderRight(); } }, 'Timeline'),
      el('button', { class: sub === 'output' ? 'on' : '', onclick: () => { sub = 'output'; renderRight(); } }, 'Output'),
      sub === 'timeline' ? el('label', { title: 'Correlate only the resources this test tracked itself, not those it shares with its parent and siblings' },
        el('input', { type: 'checkbox', checked: ownScope, onchange: e => { ownScope = e.target.checked; renderRight(); } }), 'own resources only') : null);
    const refs = (t.allRefs || []);
    const header = [
      el('h2', {}, dot(t.state), ' ', t.name),
      el('div', { class: 'sub' }, `${shortPkg(t.package)} · ${t.state}`, t.elapsed ? ` · ${secs(t.elapsed)}` : '', ` · started ${fmtTime(t.start)}`),
      el('div', { class: 'chips' }, refs.length ? refs.map(r => el('span', { class: 'badge e2e', title: JSON.stringify(r) }, `${r.kind}: ${r.atespace ? r.atespace + '/' : ''}${r.name}`)) :
        el('span', { class: 'sub' }, 'No tracked resources: component logs cannot be correlated with this test.')),
      tabs,
    ];
    if (sub === 'output') {
      const out = el('div', { class: 'tl' }, (t.output || []).map(o => el('div', { class: 'e', style: 'grid-template-columns:96px 1fr' },
        el('span', { class: 't' }, fmtTime(o.time)), el('span', { class: 'x' }, o.text))));
      right.replaceChildren(...header, t.dropped ? el('div', { class: 'sub' }, `${t.dropped} earlier lines dropped`) : null, out);
      return;
    }
    right.replaceChildren(...header, timeline(tl));
  }

  function laneColor(lane, i) {
    if (lane === 'test') return '#5aa9ff';
    const palette = ['#c792ea', '#7fdbca', '#f78c6c', '#ffcb6b', '#82aaff', '#c3e88d', '#ff5370', '#89ddff', '#bb80b3'];
    return palette[i % palette.length];
  }

  function timeline(tl) {
    const lanes = tl.lanes || ['test'];
    const colors = new Map(lanes.map((l, i) => [l, laneColor(l, i)]));
    const entries = (tl.entries || []).filter(e => !hiddenLanes.has(e.lane));
    const toggles = el('div', { class: 'chips' }, lanes.map(l => el('label', { style: `color:${colors.get(l)}` },
      el('input', { type: 'checkbox', checked: !hiddenLanes.has(l), onchange: e => { e.target.checked ? hiddenLanes.delete(l) : hiddenLanes.add(l); renderRight(); } }), l)));

    const list = el('div', { class: 'tl' });
    const rows = entries.map(e => el('div', { class: `e ${e.level || ''}`, style: `border-left-color:${colors.get(e.lane)}`, title: e.pod || e.test || '' },
      el('span', { class: 't' }, fmtTime(e.time)),
      el('span', { class: 'lane', style: `color:${colors.get(e.lane)}` }, e.lane),
      el('span', { class: 'x' }, e.text)));
    list.append(...rows);
    if (!rows.length) list.append(el('div', { class: 'empty' }, 'Nothing to show yet.'));
    return el('div', {},
      toggles,
      swimChart(entries, lanes.filter(l => !hiddenLanes.has(l)), colors, i => {
        const row = rows[i];
        if (!row) return;
        row.scrollIntoView({ block: 'center' });
        row.classList.add('flash');
        setTimeout(() => row.classList.remove('flash'), 1200);
      }),
      tl.truncated ? el('div', { class: 'sub' }, 'Log lines were capped; the earliest are not shown.') : null,
      list);
  }

  // swimChart draws one row per component and a mark per entry over time:
  // the shape of a test's path through the system at a glance.
  function swimChart(entries, lanes, colors, onPick) {
    if (!entries.length) return el('div');
    const W = 1000, labelW = 170, rowH = 20, top = 8, axisH = 18;
    const H = top + lanes.length * rowH + axisH;
    const times = entries.map(e => new Date(e.time).getTime());
    const t0 = Math.min(...times), t1 = Math.max(...times);
    const span = Math.max(t1 - t0, 1);
    const x = t => labelW + ((t - t0) / span) * (W - labelW - 10);
    const s = svg('svg', { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'none', style: `height:${H}px` });
    lanes.forEach((l, i) => {
      const y = top + i * rowH + rowH / 2;
      s.append(svg('line', { x1: labelW, x2: W - 10, y1: y, y2: y, stroke: '#2a3440' }));
      s.append(svg('text', { x: 6, y: y + 4 }, l.length > 24 ? l.slice(0, 23) + '…' : l));
    });
    const laneIdx = new Map(lanes.map((l, i) => [l, i]));
    entries.forEach((e, i) => {
      const li = laneIdx.get(e.lane);
      if (li === undefined) return;
      const y = top + li * rowH + rowH / 2;
      const fill = e.level === 'ERROR' ? '#f85149' : e.level === 'WARN' ? '#d29922' : colors.get(e.lane);
      const r = e.level === 'ERROR' || e.level === 'WARN' ? 4 : 2.5;
      const c = svg('circle', { cx: x(times[i]), cy: y, r, fill, style: 'cursor:pointer', onclick: () => onPick(i) });
      c.append(svg('title', {}, `${fmtTime(e.time)} ${e.lane}: ${e.text.slice(0, 200)}`));
      s.append(c);
    });
    const yAxis = top + lanes.length * rowH + 12;
    for (let k = 0; k <= 4; k++) {
      const t = t0 + (span * k) / 4;
      s.append(svg('text', { x: Math.min(x(t), W - 60), y: yAxis }, `+${((t - t0) / 1000).toFixed(1)}s`));
    }
    return el('div', { class: 'swim' }, s);
  }

  function update(topic) {
    if (topic && topic !== 'e2e') return;
    renderLeft();
    // A running test's detail refreshes at most every 3s.
    const t = S.e2e && S.e2e.tests.find(t => `${t.package}.${t.name}` === selected);
    if (selected && t && (t.state === 'running' || t.state === 'paused')) {
      clearTimeout(detailTimer);
      detailTimer = setTimeout(renderRight, Math.max(0, 3000 - (Date.now() - lastDetail)));
    }
  }

  renderLeft();
  renderRight();
  return {
    update,
    route: r2 => { selected = r2.arg || ''; hiddenLanes.clear(); renderLeft(); renderRight(); },
    destroy: () => clearTimeout(detailTimer),
  };
}

// ---------------------------------------------------------------------------

async function main() {
  try {
    S.info = await api('api/info');
    document.getElementById('cluster').textContent = `context ${S.info.context || '(current)'} · namespace ${S.info.systemNamespace}`;
  } catch (err) {
    console.error(err);
  }
  await load();
  window.addEventListener('hashchange', render);
  render();
  connectEvents();
}

main();
