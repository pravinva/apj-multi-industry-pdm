# Workstream 6 — Databricks App: Pixel-Perfect React Implementation

## Owner
Pravin Varma

## Depends on
WS1 (schema), WS3 (Gold tables), WS5 (agent endpoint live)

## Critical instruction
The file `ot_pdm_app_layout.html` is the **single source of truth** for every visual element, interaction, data shape, color, font size, and animation. This workstream converts it to React exactly. Do not redesign, simplify, or "improve" anything. If the HTML has it, the React app has it identically.

---

## Task 6.0 — Reference: complete design system

### Typography
```css
font-family: 'IBM Plex Sans', sans-serif;       /* all body text */
font-family: 'IBM Plex Mono', monospace;        /* all numeric values, tag names, timestamps, IDs */
```
Font sizes used: 8px, 9px, 10px, 11px, 12px, 13px, 14px, 15px, 16px, 18px, 20px, 22px, 24px, 36px

### CSS variables (copy verbatim into globals.css)
```css
:root {
  --db-red:#FF3621; --db-navy:#1B2431; --bg:#F0F2F5; --card:#fff;
  --border:#E2E8F0; --text:#1B2431; --muted:#64748B;
  --green:#16A34A; --amber:#D97706; --red:#DC2626;
  --green-bg:#F0FDF4; --amber-bg:#FFFBEB; --red-bg:#FEF2F2;
  --green-lt:#DCFCE7; --amber-lt:#FEF3C7; --red-lt:#FEE2E2;
  --sidebar:52px;
}
```

### Additional colors used in CSS (not in variables — include these too)
```
#111827  #166534  #4338CA  #475569  #7DD3FC  #86EFAC  #94A3B8
#9A3412  #CBD5E1  #E2E8F0  #EEF2FF  #EFF6FF  #F1F5F9  #F8FAFC
#FCA5A5  #FFF5F3  #FFFAFA  #FFFDF5
```

### Animations (define as @keyframes in globals.css)
```css
@keyframes pulse     { 0%,100%{opacity:1} 50%{opacity:.4} }
@keyframes fadeIn    { from{background:#EFF6FF} to{} }
@keyframes modalFadeIn { from{opacity:0} to{opacity:1} }
@keyframes modalSlideUp { from{transform:translateY(100%)} to{transform:translateY(0)} }
@keyframes spin      { to{transform:rotate(360deg)} }
@keyframes rowFadeIn { from{opacity:0;transform:translateX(-8px)} to{opacity:1;transform:none} }
```

### Chart library
Use `chart.js@4.4.0` via `npm install chart.js`. All charts are `new Chart(ctx, config)` — use a `<canvas>` ref in React. Destroy on unmount/re-render with `chartInstance.destroy()`.

---

## Task 6.1 — Complete file structure

```
app/
├── app.yaml
├── server.py                        # FastAPI backend (see WS6 original)
├── package.json
├── vite.config.js
├── index.html
└── src/
    ├── main.jsx
    ├── App.jsx                      # Root: IndustryContext, routing state
    ├── globals.css                  # ALL CSS from ot_pdm_app_layout.html verbatim
    │
    ├── data/
    │   ├── skins.js                 # SKINS object — exact copy from HTML
    │   ├── execData.js              # EXEC_DATA object — exact copy from HTML
    │   ├── partsData.js             # PARTS_DATA object — exact copy from HTML
    │   ├── simMeta.js               # SIM_META object — exact copy from HTML
    │   └── isa95.js                 # ISA95 object — exact copy from HTML
    │
    ├── api/
    │   ├── client.js                # fetch wrapper
    │   ├── fleet.js                 # /api/fleet/* calls
    │   ├── asset.js                 # /api/asset/* calls
    │   ├── agent.js                 # /api/agent/chat
    │   ├── parts.js                 # /api/parts/*
    │   └── simulator.js             # /api/simulator/*
    │
    ├── hooks/
    │   ├── useFleetData.js          # polls every 30s
    │   ├── useAssetData.js          # drilldown data
    │   ├── useSensorStream.js       # polls every 5s
    │   └── useAgent.js              # conversation state
    │
    └── components/
        ├── layout/
        │   ├── TopBar.jsx           # .topbar + logo + .ind-tabs + .isa-badge
        │   ├── Sidebar.jsx          # .sidebar + 6 .nav-btn
        │   └── Layout.jsx           # root layout: TopBar + Sidebar + page area
        │
        ├── shared/
        │   ├── HealthRing.jsx       # SVG donut ring — exact SVG from ring() fn
        │   ├── StatusBadge.jsx      # .sbadge.healthy/warning/critical
        │   ├── QualityBadge.jsx     # .q-badge.good/uncertain/bad
        │   ├── ProtoBadge.jsx       # .proto-badge.opcua/mqtt/modbus
        │   ├── KpiStrip.jsx         # .kpi-strip with .kpi items
        │   ├── Sparkline.jsx        # Chart.js line chart in canvas
        │   └── Modal.jsx            # exec-modal-overlay bottom-sheet
        │
        ├── p1_fleet/
        │   ├── FleetPage.jsx        # page p1 wrapper: topbar toggle + both views
        │   ├── OperatorView.jsx     # .view-panel#view-operator
        │   ├── ExecutiveView.jsx    # .view-panel#view-executive
        │   ├── AssetCard.jsx        # .asset-card with ring, metrics, crumb
        │   ├── AgentPanel.jsx       # .agent-panel: messages + input
        │   ├── AlertBar.jsx         # .alert-bar + .arow items
        │   ├── ExecPriorityRow.jsx  # .exec-priority-row (3 .exec-pcard)
        │   ├── ExecInsightCard.jsx  # .exec-insight-card with actions
        │   ├── ExecTrendChart.jsx   # Chart.js line in .exec-trend-card
        │   ├── ExecExposureBar.jsx  # .exec-exposure-card bar rows
        │   └── ExecModals.jsx       # parts modal + sensor modal bottom sheets
        │
        ├── p2_drilldown/
        │   ├── DrilldownPage.jsx    # page p2 wrapper
        │   ├── StatCards.jsx        # .stat-cards (4 cards)
        │   ├── SensorGrid.jsx       # .sensor-grid
        │   ├── SensorTile.jsx       # .sensor-tile with sparkline
        │   └── AnomalyTimeline.jsx  # Chart.js in .anomaly-timeline
        │
        ├── p3_hierarchy/
        │   ├── HierarchyPage.jsx    # page p3 wrapper: tree + detail
        │   ├── TreePanel.jsx        # .tree-panel: collapsible tree
        │   ├── TreeNode.jsx         # .tree-node recursive
        │   └── NodeDetail.jsx       # .detail-panel: health rollup + children grid
        │
        ├── p4_stream/
        │   └── StreamPage.jsx       # page p4: controls + table
        │
        ├── p5_model/
        │   ├── ModelPage.jsx        # page p5 wrapper
        │   ├── ModelMeta.jsx        # .model-meta grid
        │   ├── RulChart.jsx         # Chart.js in .chart-card.full
        │   ├── FeatureChart.jsx     # Chart.js horizontal bar
        │   └── AnomalyDecomp.jsx    # .anomaly-decomp bars
        │
        └── p6_simulator/
            ├── SimulatorPage.jsx    # page p6: 3 inner tabs
            ├── SimulatorTab.jsx     # .p6t-sim
            ├── AssetSimCard.jsx     # .sim-asset-card with fault injection
            ├── BronzeTable.jsx      # .bronze-panel live table
            ├── ConfigBuilderTab.jsx # .p6t-config
            ├── ConnectorSetupTab.jsx# .p6t-connector
            └── YamlModal.jsx        # .yaml-modal dark code modal
```

---

## Task 6.2 — Data files: copy JS objects verbatim from HTML

### src/data/skins.js
Open `ot_pdm_app_layout.html`, find `const SKINS = {` and copy the entire object verbatim into this file. Export as:
```javascript
export const SKINS = { /* paste exact object */ };
```
The SKINS object contains all static display data used by the app when the backend is unavailable or for the simulator tab. **Do not modify field names or structure** — all components reference them directly.

Key shape per industry (must match exactly):
```javascript
{
  panelTitle: String,
  agentTitle: String,
  agentSubtitle: String,
  agentPlaceholder: String,
  kpis: [{ l, v, c, d }],           // label, value, colorClass (g/a/r/''), delta
  assets: [{
    id, type, status, health,        // health is 0-100 integer
    crumb,                           // ISA-95 breadcrumb string with › separators
    metrics: [{ l, v, c }],         // label, value, color hex or null
    sensors: [{
      name, label, val, unit,
      status,                        // 'healthy'|'warning'|'critical'
      trend,                         // e.g. "+0.6/h" or "stable"
      history: [8 numbers]           // sparkline data
    }],
    anomalyHistory: [13 numbers],    // 72h anomaly score history
    rul: { current, initial, predicted: [8 strings], labels: [8 strings], failurePct },
    features: [{ name, score }],     // feature importance
    decomp:   [{ name, score }],     // anomaly decomposition
    model: { trained, accuracy, rmse, protocol }
  }],
  messages: [{ role, text } | { role, label, paras: [htmlString] }],
  alerts: [{ sev, text, time }],
  hierarchy: { label, level, health, icon, children: [...recursive] }
}
```

### src/data/execData.js
Copy `const EXEC_DATA = {` verbatim. Key shape per industry:
```javascript
{
  trendTitle, trendLabel,
  trendData: [7 numbers],           // Mon–Today
  trendColor: hexString,
  exposureTitle,
  assets: [{
    id, type, status,
    headline,                        // one-line plain text
    recommendation,                  // 2-3 sentence plain text
    costVal,                         // e.g. "$42K" or "¥4.2M"
    costLabel,                       // e.g. "per day if unplanned TOR"
    rul,                             // display string e.g. "23h"
    health,                          // display string e.g. "31%"
    anomaly,                         // display string e.g. "0.94"
    actions: [{ label, style }]      // style: 'danger'|'primary'|''
  }]
}
```

### src/data/partsData.js
Copy `const PARTS_DATA = {` verbatim.
```javascript
// { "HT-012": { depot, parts: [{ num, desc, qty, stock, loc, eta }] }, ... }
```

### src/data/simMeta.js
Copy `const SIM_META = {` verbatim.

### src/data/isa95.js
Copy `const ISA95 = {` verbatim.

---

## Task 6.3 — globals.css

**Copy the entire `<style>` block from `ot_pdm_app_layout.html` verbatim** into `src/globals.css`. This is 37,085 characters. Do not rewrite, reorganise, or convert to CSS modules. The class names are used as-is in JSX `className` props.

Import in `main.jsx`:
```javascript
import './globals.css';
import 'https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap';
```

---

## Task 6.4 — Shared components

### HealthRing.jsx
Replicates the `ring(pct, color)` function exactly:
```jsx
export function HealthRing({ pct, color, size = 46 }) {
  const r = size === 46 ? 17 : 19;
  const circ = 2 * Math.PI * r;
  const arc = (pct / 100) * circ;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#E2E8F0" strokeWidth="4"/>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke={color} strokeWidth="4"
        strokeDasharray={`${arc.toFixed(1)} ${(circ-arc).toFixed(1)}`}
        strokeLinecap="round"
        transform={`rotate(-90 ${size/2} ${size/2})`}/>
    </svg>
  );
}
```

### Sparkline.jsx
Chart.js line chart — matches exactly how sparklines render in sensor tiles:
```jsx
import { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

export function Sparkline({ data, color, height = 40 }) {
  const canvasRef = useRef(null);
  const chartRef  = useRef(null);

  useEffect(() => {
    if (!canvasRef.current) return;
    if (chartRef.current) chartRef.current.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: 'line',
      data: {
        labels: data.map((_, i) => i),
        datasets: [{ data, borderColor: color, borderWidth: 2,
          fill: true, backgroundColor: color + '18',
          pointRadius: 0, tension: 0.4 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
        animation: { duration: 0 }
      }
    });
    return () => chartRef.current?.destroy();
  }, [data, color]);

  return <canvas ref={canvasRef} style={{ height }} />;
}
```

### KpiStrip.jsx
```jsx
export function KpiStrip({ kpis }) {
  return (
    <div className="kpi-strip">
      {kpis.map((k, i) => (
        <div className="kpi" key={i}>
          <div className="kpi-l">{k.l}</div>
          <div className={`kpi-v ${k.c}`}>{k.v}</div>
          <div className="kpi-d">{k.d}</div>
        </div>
      ))}
    </div>
  );
}
```

---

## Task 6.5 — Layout

### TopBar.jsx
```jsx
export function TopBar({ currentInd, onSwitchInd }) {
  const INDUSTRIES = ['mining','energy','water','automotive','semiconductor'];
  const LABELS     = ['Mining','Energy','Water','Automotive','Semiconductor'];
  return (
    <header className="topbar">
      <div className="logo">
        {/* SVG hexagon logo — exact from HTML */}
        <svg width="24" height="24" viewBox="0 0 26 26" fill="none">
          <polygon points="13,1 24,7 24,19 13,25 2,19 2,7" fill="#FF3621"/>
          <polygon points="13,6 20,10 20,18 13,22 6,18 6,10" fill="rgba(0,0,0,.25)"/>
          <polygon points="13,10 17,12 17,17 13,19 9,17 9,12" fill="rgba(255,255,255,.35)"/>
        </svg>
        <span className="logo-text">Databricks</span>
      </div>
      <div className="topbar-div"/>
      <span className="app-name">OT PdM Intelligence</span>
      <div className="ind-tabs">
        {INDUSTRIES.map((ind, i) => (
          <button key={ind}
            className={`itab ${currentInd === ind ? 'active' : ''}`}
            onClick={() => onSwitchInd(ind)}>
            {LABELS[i]}
          </button>
        ))}
      </div>
      <span className="isa-badge">ISA-95 · Unity Catalog</span>
    </header>
  );
}
```

### Sidebar.jsx
Six nav buttons — exact SVG paths from HTML, exact labels:
```jsx
const NAV = [
  { page:'p1', title:'Fleet Overview', label:'Fleet',
    svg: <><rect x="2" y="2" width="7" height="7" rx="1.5"/><rect x="11" y="2" width="7" height="7" rx="1.5"/><rect x="2" y="11" width="7" height="7" rx="1.5"/><rect x="11" y="11" width="7" height="7" rx="1.5"/></> },
  { page:'p2', title:'Asset Drilldown', label:'Asset',
    svg: <><circle cx="10" cy="8" r="5"/><path d="M10 13v5M7 18h6" strokeLinecap="round"/></> },
  { page:'p3', title:'ISA-95 Hierarchy', label:'Hier.',
    svg: <><path d="M10 2v4M5 9H2v4h3M18 9h-3v4h3M10 6v3M5 11h5M10 9h5" strokeLinecap="round"/><rect x="7" y="13" width="6" height="5" rx="1"/></> },
  { page:'p4', title:'Sensor Stream', label:'Stream',
    svg: <><path d="M2 10h16M2 5h10M2 15h14" strokeLinecap="round"/><circle cx="16" cy="10" r="1.5" fill="currentColor" stroke="none"/></> },
  { page:'p5', title:'Model Explainability', label:'Model',
    svg: <path d="M3 16L7 10l4 3 3-6 3 4" strokeLinecap="round" strokeLinejoin="round"/> },
  // nav-sep before p6
  { page:'p6', title:'OT Simulator', label:'Sim',
    svg: <><circle cx="10" cy="10" r="3"/><path d="M10 2v2M10 16v2M2 10h2M16 10h2M4.22 4.22l1.42 1.42M14.36 14.36l1.42 1.42M4.22 15.78l1.42-1.42M14.36 5.64l1.42-1.42" strokeLinecap="round"/></> },
];
// Insert nav-sep div between index 4 and 5
```

---

## Task 6.6 — Page 1: Fleet (p1)

### FleetPage.jsx — structure
```jsx
<div className="page active" id="p1">
  {/* Top bar with KPI strip + view toggle */}
  <div className="p1-topbar">
    <KpiStrip kpis={skin.kpis} />
    <div className="view-toggle-wrap">
      <button className={`view-btn ${view==='operator'?'active':''}`} onClick={()=>setView('operator')}>Operator</button>
      <button className={`view-btn ${view==='executive'?'active':''}`} onClick={()=>setView('executive')}>Executive</button>
    </div>
  </div>

  {/* Operator view */}
  <div id="view-operator" className={`view-panel ${view==='operator'?'active':''}`}>
    <div className="p1-main">
      <AssetPanel skin={skin} onDrilldown={onDrilldown} />
      <AgentPanel skin={skin} industry={currentInd} />
    </div>
    <AlertBar alerts={skin.alerts} />
  </div>

  {/* Executive view */}
  <div id="view-executive" className={`view-panel ${view==='executive'?'active':''}`}>
    <ExecutiveView skin={skin} execData={execData} onAction={handleExecAction} />
  </div>
</div>
```

### AssetCard.jsx — exact structure matching HTML renderP1
```jsx
export function AssetCard({ asset, onDrilldown }) {
  const c = statusColor(asset.status);
  return (
    <div className={`asset-card ${asset.status}`} onClick={() => onDrilldown(asset.id)}>
      <div className="card-top">
        <div className="hring">
          <HealthRing pct={asset.health} color={c} />
          <span className="hpct" style={{ color: c }}>{asset.health}%</span>
        </div>
        <div className="ai">
          <div className="aid">{asset.id}</div>
          <div className="atype">{asset.type}</div>
          <div className="acrumb">{asset.crumb}</div>
        </div>
        <StatusBadge status={asset.status} />
      </div>
      <div className="card-metrics">
        {asset.metrics.map((m, i) => (
          <div className="cm" key={i}>
            <div className="cml">{m.l}</div>
            <div className="cmv" style={m.c ? { color: m.c } : undefined}>{m.v}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
function statusColor(s) {
  return s==='critical'?'#DC2626':s==='warning'?'#D97706':'#16A34A';
}
```

### AgentPanel.jsx — matches msgs + bubble structure exactly
Messages render as:
- `role==='user'` → `<div class="msg"><div class="av user">ME</div><div class="bubble user">{text}</div></div>`
- `role==='agent'` → `<div class="msg"><div class="av agent">AI</div><div class="bubble"><div class="bubble-lbl">{label}</div>{paras.map(p => <p dangerouslySetInnerHTML={{__html:p}}/>)}</div></div>`

Real API mode: POST to `/api/agent/chat`, render streaming response.

### AlertBar.jsx — exact .arow structure
```jsx
<div className={`arow ${alert.sev}`}>
  <div className={`apip ${alert.sev}`}/>
  <span className="atext" dangerouslySetInnerHTML={{ __html: alert.text }}/>
  <span className="atime">{alert.time}</span>
</div>
```

### ExecutiveView.jsx
Three sub-components rendered inside `.exec-wrap`:
1. `<ExecPriorityRow>` — `.exec-priority-row` with three `.exec-pcard` divs
2. `<div className="exec-main">` containing `<ExecInsights>` + `<div className="exec-right">`
3. Right column: `<ExecTrendChart>` + `<ExecExposureCard>`

### ExecInsightCard.jsx — exact structure
```jsx
<div className={`exec-insight-card ${asset.status}`}>
  <div className="eic-top">
    <span className={`eic-badge ${asset.status}`}>{asset.status==='critical'?'Act now':'Schedule'}</span>
    <div>
      <div className="eic-asset-id">{asset.id}</div>
      <div className="eic-asset-type">{asset.type}</div>
    </div>
    <div className="eic-cost">
      <div className="eic-cost-val" style={asset.status==='warning'?{color:'var(--amber)'}:{}}>{asset.costVal}</div>
      <div className="eic-cost-label">{asset.costLabel}</div>
    </div>
  </div>
  <div className="eic-metrics">
    <div className="eic-metric"><div className="eic-metric-l">RUL remaining</div><div className="eic-metric-v" style={{color:sColor}}>{asset.rul}</div></div>
    <div className="eic-metric"><div className="eic-metric-l">Asset health</div><div className="eic-metric-v" style={{color:sColor}}>{asset.health}</div></div>
    <div className="eic-metric"><div className="eic-metric-l">Confidence</div><div className="eic-metric-v">{Math.round(parseFloat(asset.anomaly)*100)}%</div></div>
  </div>
  <div className={`eic-recommendation ${asset.status}`}>
    <div className={`eic-rec-label ${asset.status}`}>Recommendation</div>
    {asset.recommendation}
  </div>
  <div className="eic-actions">
    {asset.actions.map((ac, i) => (
      <button key={i} className={`eic-action-btn ${ac.style}`}
        onClick={() => onAction(asset.id, ac.label)}>{ac.label}</button>
    ))}
  </div>
</div>
```

### ExecModals.jsx — Parts modal + Sensor modal
Both are bottom-sheet modals using `.exec-modal-overlay`:
- `.exec-modal-overlay.open` triggers animation `modalFadeIn`
- `.exec-modal-box` slides up with `modalSlideUp`
- Close on overlay click via `if (event.target === overlay) closeModal()`

**Parts modal** shows spinner for 680ms then animates table rows with `rowFadeIn` at `80ms × index` stagger delay.

**Sensor modal** shows spinner for 500ms then renders `.sensor-modal-grid` with Chart.js sparklines per sensor. Charts animate with `duration: 600, easing: 'easeOutQuart'`. Each chart uses the sensor's `history` array.

---

## Task 6.7 — Page 2: Asset Drilldown (p2)

### DrilldownPage.jsx
```jsx
<div className="page" id="p2">
  <div className="p2-wrap">
    {/* Header with back button */}
    <div className="p2-hdr">
      <button className="back-btn" onClick={() => navigate('p1')}>← Fleet</button>
      <div className="p2-meta">
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <div className="p2-id">{asset.id}</div>
          <StatusBadge status={asset.status} />
        </div>
        <div className="p2-type">{asset.type}</div>
        <div className="p2-crumb">{asset.crumb}</div>
      </div>
    </div>

    {/* 4 stat cards */}
    <StatCards asset={asset} />

    {/* Sensor grid with sparklines */}
    <div>
      <div className="sect-title">Sensor readings — live</div>
      <div className="sensor-grid">
        {asset.sensors.map(s => <SensorTile key={s.name} sensor={s} />)}
      </div>
    </div>

    {/* Anomaly timeline */}
    <AnomalyTimeline asset={asset} />
  </div>
</div>
```

### StatCards.jsx — 4 cards exactly
```jsx
const cards = [
  { label:'Health score', value:`${asset.health}%`, sub:asset.status, color:c },
  { label:'RUL remaining', value:`${asset.rul?.current}`, unit:'h', sub:'estimated hours to failure', color:c },
  { label:'Anomaly score', value:asset.metrics[2]?.v||'—', sub:'isolation forest output', color:c },
  { label:'Protocol', value:asset.model?.protocol||'OPC-UA', sub:'ingestion via Zerobus', color:null },
];
```

### SensorTile.jsx — exact structure including anomalous/warning class
```jsx
<div className={`sensor-tile ${sensor.status!=='healthy'?sensor.status:''}`}>
  <div className="st-top">
    <div><div className="st-name">{sensor.label}</div><div className="st-unit">{sensor.unit}</div></div>
  </div>
  <div className="st-val" style={{color:sColor}}>
    {sensor.val}<span style={{fontSize:12,color:'var(--muted)',fontWeight:400,fontFamily:"'IBM Plex Sans'"}}> {sensor.unit}</span>
  </div>
  <div className="st-trend" style={{color:sColor}}>{sensor.trend}</div>
  <div className="sparkline-wrap">
    <Sparkline data={sensor.history} color={sColor} height={40} />
  </div>
</div>
```

### AnomalyTimeline.jsx — Chart.js with 3 datasets
```javascript
datasets: [
  { label:'Anomaly score', data:asset.anomalyHistory, borderColor:'#FF3621',
    borderWidth:2, fill:true, backgroundColor:'rgba(255,54,33,.08)',
    pointRadius:3, pointBackgroundColor:'#FF3621', tension:.4 },
  { label:'Warning threshold', data:asset.anomalyHistory.map(()=>0.5),
    borderColor:'#D97706', borderWidth:1, borderDash:[4,4], pointRadius:0, fill:false },
  { label:'Critical threshold', data:asset.anomalyHistory.map(()=>0.8),
    borderColor:'#DC2626', borderWidth:1, borderDash:[4,4], pointRadius:0, fill:false }
]
// y axis: min:0, max:1
// x labels: ['-72h','-66h',...,'Now']
```

---

## Task 6.8 — Page 3: ISA-95 Hierarchy (p3)

### HierarchyPage.jsx
```jsx
<div className="p3-wrap">
  <TreePanel hierarchy={skin.hierarchy} onSelect={setSelectedNode} />
  <NodeDetail node={selectedNode} onDrilldown={onDrilldown} />
</div>
```

### TreeNode.jsx — recursive, collapsible
Replicates `buildTree()` exactly:
- `.tree-node` → `.tn-row` (clickable, toggles children) → `.tn-children`
- `.tn-chevron` rotates 90° when open (CSS transition)
- `.tn-badge` — health % badge, color: green ≥80, amber ≥60, red <60
- Same background colors: `#DCFCE7`, `#FEF3C7`, `#FEE2E2`
- `.selected` class on `.tn-row` when node is selected
- Root node starts open, children start closed

```jsx
const healthBg = h>=80?'#DCFCE7':h>=60?'#FEF3C7':'#FEE2E2';
const healthC  = h>=80?'#16A34A':h>=60?'#D97706':'#DC2626';
```

### NodeDetail.jsx
Replicates `selectNode()` exactly:
- Level color mapping: Site→`#EEF2FF`/`#4338CA`, Area→`#F0FDF4`/`#166534`, Unit→`#FFFBEB`/`#92400E`, Equipment→`#FEF2F2`/`#991B1B`, Component→`#F8FAFC`/`#475569`
- Health score displayed at 28px monospace
- `.health-breakdown` table: label + progress bar + % value
- `.children-grid` with `.child-card` items (clickable if has `assetId`)
- "View asset drilldown →" button appears only when node has `assetId`

---

## Task 6.9 — Page 4: Sensor Stream (p4)

### StreamPage.jsx
- Three `<select>` filters: asset, quality, protocol — with exact options
- Table with columns: Timestamp, Site, Area, Unit, Equipment, Tag name, Value, Unit, Quality, Protocol
- Polls `/api/stream/latest` every 5 seconds, prepends new rows
- Row classes: `bad` (red bg), `uncertain` (amber bg), `new-row` (fadeIn animation)
- Quality badge: `.q-badge.good/uncertain/bad`
- Protocol badge: `.proto-badge.opcua/mqtt/modbus`
- Reading counter: `"{n} readings"` right-aligned in controls
- Cap at 200 rows (remove from bottom)
- `.stream-count` updates on every poll

```jsx
// Header format for timestamp column:
new Date().toISOString().replace('T',' ').slice(0,23)  // "2026-03-24 14:23:45.123"
```

---

## Task 6.10 — Page 5: Model Explainability (p5)

### ModelPage.jsx
Asset selector `<select>` at top — populated from `SKINS[industry].assets`. On change, re-renders all charts.

### ModelMeta.jsx — 6 cells in `.model-meta` grid
```javascript
const cells = [
  { l:'Model trained',    v:asset.model.trained },
  { l:'RUL accuracy (R²)',v:asset.model.accuracy },
  { l:'RMSE',             v:asset.model.rmse },
  { l:'Protocol',         v:asset.model.protocol },
  { l:'Asset health',     v:asset.health+'%' },
  { l:'Anomaly score',    v:asset.metrics[2]?.v||'—' },
];
```

### RulChart.jsx — Chart.js line, 3 datasets
```javascript
datasets: [
  { label:'Actual RUL', data:asset.rul.predicted.map(Number),
    borderColor:healthColor, borderWidth:2.5, fill:true,
    backgroundColor:healthColor+'12', pointRadius:4, tension:.3 },
  { label:'Failure threshold', data:asset.rul.labels.map(()=>0),
    borderColor:'#DC2626', borderWidth:1.5, borderDash:[6,4], pointRadius:0, fill:false },
  { label:'Warning zone', data:asset.rul.labels.map(()=>asset.rul.initial*0.1),
    borderColor:'#D97706', borderWidth:1, borderDash:[4,4], pointRadius:0, fill:false }
]
// y axis title: "Remaining useful life (hours)"
// x labels: asset.rul.labels
```

### FeatureChart.jsx — horizontal bar, sorted by score descending
```javascript
// indexAxis: 'y'
// colors: index===0 → '#FF3621', index===1 → '#F97316', rest → '#94A3B8'
// borderRadius: 4
// x axis max: 1
```

### AnomalyDecomp.jsx — bar rows with inline bars
Replicates `renderP5Asset` decomp section:
```jsx
const maxScore = Math.max(...asset.decomp.map(d=>d.score), 0.01);
// Each row:
const col = d.score>0.3?'#DC2626':d.score>0.15?'#D97706':'#94A3B8';
const pct = Math.round(d.score/maxScore*100);
// .ad-bar width: pct%
```

---

## Task 6.11 — Page 6: OT Simulator (p6)

### SimulatorPage.jsx — 3 inner tabs
```jsx
<div className="page" id="p6">
  <div className="p6-inner-tabs">
    <button className={`p6itab ${tab==='sim'?'active':''}`} onClick={()=>setTab('sim')}>Simulator</button>
    <button className={`p6itab ${tab==='config'?'active':''}`} onClick={()=>setTab('config')}>Config Builder</button>
    <button className={`p6itab ${tab==='connector'?'active':''}`} onClick={()=>setTab('connector')}>Connector Setup</button>
  </div>
  {tab==='sim'       && <SimulatorTab industry={currentInd} />}
  {tab==='config'    && <ConfigBuilderTab industry={currentInd} />}
  {tab==='connector' && <ConnectorSetupTab />}
</div>
```

### SimulatorTab.jsx — fault injection + bronze stream
State:
```javascript
const [running, setRunning] = useState(false);
const [tick, setTick]       = useState(800);      // ms
const [noise, setNoise]     = useState(2);        // /100
const [faults, setFaults]   = useState({});       // { assetId: { active, mode, severity } }
const [rows, setRows]        = useState([]);       // bronze table rows
const [count, setCount]      = useState(0);
```

Asset config cards (`.sim-asset-card`) — one per asset in `SIM_META[industry].assets`:
- Toggle switch (`.ftoggle`): shows/hides `.fault-controls` div
- Fault mode `<select>`: options from `asset.faultModes`
- Severity `<input type="range" min="1" max="100">`
- Sensor pills: `.spill.affected` when sensor's `affects` includes active fault mode

Simulator engine runs in `useInterval` (custom hook wrapping setInterval):
- Calls `computeValue(sensor, faultState)` and `qualityCode(val, sensor)` — replicate exact logic from HTML
- Generates rows and prepends to table (cap 200)
- Updates count

Bronze table — exact column order: Timestamp, Site, Area, Unit, Equipment, Tag name, Value, Unit, Quality (badge), Protocol (badge)

### ConfigBuilderTab.jsx
Replicates `initConfigBuilder`/`loadIndustry`/`cfgPreview` exactly:
- Industry selector pre-fills all form fields from `SKINS[industry]` + `SIM_META[industry]`
- Live YAML preview with syntax highlighting (`.yk`=blue keys, `.yv`=green values, `.yc`=gray comments)
- Asset rows dynamically added/removed with `addAssetRow`/`removeAsset`
- Sensor rows within each asset dynamically added/removed
- Copy button strips HTML tags and copies plain YAML

### ConnectorSetupTab.jsx
Replicates connector setup logic:
- Protocol selector changes URL placeholder
- "Test connection" button shows 900ms spinner then "✓ Connected — 12ms RTT"
- "Discover tags" shows 700ms spinner then renders tag list from `SIMULATED_TAGS[protocol]`
- Tag checkboxes populate mapping table
- "Add to Config Builder" switches to config tab and adds new asset

### YamlModal.jsx
Dark bottom-sheet modal (`.yaml-modal` → `.yaml-box`):
```jsx
<div className={`yaml-modal ${open?'visible':''}`} onClick={e=>e.target===e.currentTarget&&onClose()}>
  <div className="yaml-box">
    <div className="yaml-box-hdr">
      <span className="yaml-box-title">{title}</span>
      <button className="yaml-close" onClick={onClose}>✕</button>
    </div>
    <div className="yaml-pre" dangerouslySetInnerHTML={{__html:highlighted}} />
    <button className="copy-btn" onClick={onCopy}>Copy to clipboard</button>
  </div>
</div>
```

---

## Task 6.12 — App.jsx: routing and global state

All routing is client-side state (no React Router). Single `currentPage` string controls which page renders:

```jsx
export function App() {
  const [currentInd,  setCurrentInd]  = useState('mining');
  const [currentPage, setCurrentPage] = useState('p1');
  const [currentAsset,setCurrentAsset]= useState(null);
  const [view, setView]               = useState('operator');  // 'operator'|'executive'

  // currentView must be declared before any render — replicate the TDZ fix from HTML
  // (was the cause of "Cannot access currentView before initialization" bug)

  function navigate(page, assetId = null) {
    if (assetId) setCurrentAsset(assetId);
    if (page === 'p4') startStream();
    else stopStream();
    setCurrentPage(page);
  }

  function switchIndustry(ind) {
    setCurrentInd(ind);
    setCurrentAsset(null);
  }

  return (
    <div style={{display:'flex',flexDirection:'column',height:'100vh',overflow:'hidden'}}>
      <TopBar currentInd={currentInd} onSwitchInd={switchIndustry} />
      <div className="body-wrap">
        <Sidebar currentPage={currentPage} onNavigate={navigate} />
        <FleetPage       className={currentPage==='p1'?'active':''} ... />
        <DrilldownPage   className={currentPage==='p2'?'active':''} assetId={currentAsset} ... />
        <HierarchyPage   className={currentPage==='p3'?'active':''} ... />
        <StreamPage      className={currentPage==='p4'?'active':''} ... />
        <ModelPage       className={currentPage==='p5'?'active':''} ... />
        <SimulatorPage   className={currentPage==='p6'?'active':''} ... />
      </div>
    </div>
  );
}
```

Pages use `.page` and `.page.active` classes (from globals.css) to show/hide — `display:none` vs `display:flex`.

---

## Task 6.13 — API wiring: replace static data with real backend

When `import.meta.env.VITE_USE_API === 'true'` (set in production app.yaml), replace SKINS static data with API calls:

| Static data source | API endpoint | Hook |
|---|---|---|
| `SKINS[ind].assets` health/anomaly/rul | `GET /api/fleet/assets` | `useFleetData` |
| `SKINS[ind].kpis` | `GET /api/fleet/kpis` | `useFleetData` |
| `asset.sensors` live values | `GET /api/asset/{id}/sensors?hours=8` | `useAssetData` |
| `asset.anomalyHistory` | `GET /api/asset/{id}/anomaly_history?hours=72` | `useAssetData` |
| `asset.rul`, `asset.metrics` | `GET /api/asset/{id}/prediction` | `useAssetData` |
| `asset.features`, `asset.decomp` | `GET /api/asset/{id}/feature_importance` | `useAssetData` |
| Stream table rows | `GET /api/stream/latest` every 5s | `useSensorStream` |
| Parts modal data | `GET /api/parts/{id}` | inline fetch |
| Agent messages | `POST /api/agent/chat` | `useAgent` |
| EXEC_DATA cost figures | derive from `/api/fleet/assets` + `asset_metadata` | `useFleetData` |

**Fallback**: when API call fails or returns empty, fall back to `SKINS[ind]` static data. This ensures the app always renders something (important for FEVM demo if simulator hasn't run yet).

---

## Task 6.14 — Real-time behaviour

Match exact timing from HTML:
- Industry switch: immediate re-render, no loading state
- Fleet page: 30-second poll, silent background refresh
- Sensor stream: 5-second poll, rows prepended with `fadeIn` animation on `.new-row`
- Simulator: tick interval from slider (200ms–2000ms), runs in `setInterval`
- Parts modal: 680ms artificial delay before showing table (matches HTML)
- Sensor modal: 500ms artificial delay before showing charts
- Connection test: 900ms–1300ms random delay (matches HTML)
- Tag discovery: 700ms–1200ms random delay

---

## Task 6.15 — Chart.js instances

All Chart.js instances must be destroyed before re-creating. Use a `useRef` map pattern:

```javascript
const charts = useRef({});

function createChart(id, config) {
  if (charts.current[id]) {
    charts.current[id].destroy();
    delete charts.current[id];
  }
  const ctx = document.getElementById(id);
  if (ctx) charts.current[id] = new Chart(ctx, config);
}

useEffect(() => {
  return () => {
    Object.values(charts.current).forEach(c => c.destroy());
  };
}, []);
```

This replicates `chartInstances` / `destroyChart` from HTML exactly.

---

## Success criteria — every point must pass

- [ ] `globals.css` is the exact HTML `<style>` block — verified by diffing
- [ ] All 5 industry tabs switch instantly with no layout shift
- [ ] HealthRing SVG matches pixel-for-pixel with HTML version
- [ ] Asset card drilldown navigates to Page 2 and loads that asset
- [ ] ISA-95 tree collapses/expands with chevron rotation exactly as in HTML
- [ ] Sensor stream table updates every 5s, prepends rows, max 200 rows
- [ ] All 4 Chart.js charts render with correct datasets, colors, and axes
- [ ] Executive view: 3 priority cards + insight cards + trend chart + exposure bars
- [ ] Parts modal: spinner → animated rows, correct stock badges, source attribution
- [ ] Sensor modal: spinner → sparkline charts with 600ms animation
- [ ] Simulator tab: fault injection controls update sensor pill colors
- [ ] Config Builder: industry selector pre-fills all fields, YAML updates on every keystroke
- [ ] Connector Setup: test → discover tags → select → map → add to config
- [ ] YAML modal: dark theme, syntax highlighting, copy to clipboard
- [ ] All page transitions use `.page`/`.page.active` CSS classes (not visibility/opacity)
- [ ] Agent chat sends to `/api/agent/chat`, falls back gracefully on error
- [ ] No hardcoded industry strings in pipeline components (only in data files)
- [ ] App deploys via `databricks bundle deploy` and opens without errors
