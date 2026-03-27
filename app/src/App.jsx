import { useEffect, useMemo, useState } from "react";

const INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"];
const CURRENCIES = ["AUTO", "USD", "AUD", "JPY"];
const EMPTY_EXECUTIVE = {
  audience: "finance_executive",
  window: "last_30_days_simulated",
  currency: "USD",
  value_statement: "Impact on EBIT saved through prescriptive maintenance: USD 0",
  ebit_saved: 0,
  ebit_saved_fmt: "USD 0",
  net_benefit: 0,
  net_benefit_fmt: "USD 0",
  roi_pct: 0,
  payback_days: 0,
  ebit_margin_bps: 0,
  baseline_monthly_ebit_fmt: "USD 0",
  explainability: {},
  kpis: {
    avoided_downtime_cost_fmt: "USD 0",
    avoided_quality_cost_fmt: "USD 0",
    avoided_energy_cost_fmt: "USD 0",
    intervention_cost_fmt: "USD 0",
    platform_cost_fmt: "USD 0"
  },
  erp: { plant_code: "", fiscal_period: "", cost_centers: [], work_centers: [], planner_group: "", reference_account: "" },
  value_bridge: [],
  ebit_trend: [],
  work_orders: []
};
const EMPTY_OVERVIEW = {
  assets: [],
  actioned_assets: [],
  kpis: { fleet_health_score: 0, critical_assets: 0, asset_count: 0 },
  alerts: [],
  messages: [],
  executive: EMPTY_EXECUTIVE
};
const PAGE_META = [
  ["p1", "Fleet", "▦"],
  ["p2", "Asset", "◉"],
  ["p3", "Hier.", "⫶"],
  ["p4", "Stream", "≋"],
  ["p5", "Model", "↗"],
  ["p6", "Sim", "◎"],
  ["p7", "Finance", "¥"]
];

async function getJson(url, fallback) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error("bad response");
    return await res.json();
  } catch {
    return fallback;
  }
}

async function postJson(url, body, fallback) {
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error("bad response");
    return await res.json();
  } catch {
    return fallback;
  }
}

function statusColor(status) {
  if (status === "critical") return "#DC2626";
  if (status === "warning") return "#D97706";
  return "#16A34A";
}

function healthRing(health) {
  const color = health < 60 ? "#DC2626" : health < 80 ? "#D97706" : "#16A34A";
  return {
    background: `conic-gradient(${color} ${health * 3.6}deg, #E2E8F0 0deg)`
  };
}

function TreeNode({ node, onSelect }) {
  const [open, setOpen] = useState(true);
  const hasChildren = !!(node.children && node.children.length);
  return (
    <div className="tree-node">
      <div
        className="tn-row"
        onClick={() => {
          onSelect(node);
          if (hasChildren) setOpen((v) => !v);
        }}
      >
        <span className={`tn-chevron ${open ? "open" : ""} ${hasChildren ? "" : "leaf"}`}>›</span>
        <span className="tn-icon">{node.icon || "📦"}</span>
        <span className="tn-label">{node.label}</span>
        <span className="tn-badge">{node.health}%</span>
      </div>
      {hasChildren && open && <div className="tn-children">{node.children.map((c, i) => <TreeNode key={`${c.label}-${i}`} node={c} onSelect={onSelect} />)}</div>}
    </div>
  );
}

function parseTargetFqn(fqn) {
  const parts = String(fqn || "")
    .split(".")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length !== 3) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
}

function hexToRgba(hex, alpha) {
  const c = String(hex || "").replace("#", "");
  if (c.length !== 6) return `rgba(255,54,33,${alpha})`;
  const r = parseInt(c.slice(0, 2), 16);
  const g = parseInt(c.slice(2, 4), 16);
  const b = parseInt(c.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function TrendLine({ values = [], color = "#FF3621", height = 88, showXLabels = false }) {
  const points = (values || []).map((v) => Number(v)).filter((v) => Number.isFinite(v));
  if (!points.length) return <div className="trend-empty">No trend data</div>;

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(1e-6, max - min);
  const width = 100;
  const poly = points
    .map((v, i) => {
      const x = (i / Math.max(1, points.length - 1)) * width;
      const y = 100 - ((v - min) / span) * 100;
      return `${x},${y}`;
    })
    .join(" ");
  const area = `0,100 ${poly} 100,100`;
  const lastIdx = points.length - 1;
  const lastX = (lastIdx / Math.max(1, lastIdx)) * width;
  const lastY = 100 - ((points[lastIdx] - min) / span) * 100;

  return (
    <div className={`trend-wrap ${showXLabels ? "with-labels" : ""}`} style={{ height }}>
      <svg className="trend-svg" viewBox="0 0 100 100" preserveAspectRatio="none">
        <polygon className="trend-area" points={area} style={{ fill: hexToRgba(color, 0.16) }} />
        <polyline className="trend-line" points={poly} style={{ "--trend-color": color }} />
        <circle cx={lastX} cy={lastY} r="1.6" fill="#fff" stroke={color} strokeWidth="0.9" />
      </svg>
      {showXLabels && (
        <div className="trend-labels">
          <span>-72h</span>
          <span>-24h</span>
          <span>Now</span>
        </div>
      )}
      <div className="trend-baseline" />
    </div>
  );
}

function renderInlineMarkdown(text) {
  const parts = String(text || "").split(/(`[^`]+`|\*\*[^*]+\*\*)/g);
  return parts.map((part, idx) => {
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
      return <strong key={`md-strong-${idx}`}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`") && part.length > 2) {
      return <code key={`md-code-${idx}`}>{part.slice(1, -1)}</code>;
    }
    return <span key={`md-span-${idx}`}>{part}</span>;
  });
}

function renderSimpleMarkdown(text) {
  const lines = String(text || "").split("\n");
  const blocks = [];
  let listItems = [];

  const flushList = () => {
    if (!listItems.length) return;
    blocks.push(
      <ul key={`md-list-${blocks.length}`} className="bubble-md-list">
        {listItems.map((item, idx) => (
          <li key={`md-li-${idx}`}>{renderInlineMarkdown(item)}</li>
        ))}
      </ul>
    );
    listItems = [];
  };

  lines.forEach((raw, idx) => {
    const line = raw.trimEnd();
    const bullet = line.match(/^\s*[-*]\s+(.+)$/);
    if (bullet) {
      listItems.push(bullet[1]);
      return;
    }
    flushList();
    if (!line.trim()) {
      blocks.push(<div key={`md-br-${idx}`} className="bubble-md-break" />);
      return;
    }
    blocks.push(
      <p key={`md-p-${idx}`} className="bubble-md-p">
        {renderInlineMarkdown(line)}
      </p>
    );
  });
  flushList();
  return <div className="bubble-md">{blocks}</div>;
}

export default function App() {
  const [industry, setIndustry] = useState("mining");
  const [demoCurrency, setDemoCurrency] = useState("AUTO");
  const [page, setPage] = useState("p1");
  const [view, setView] = useState("operator");
  const [simTab, setSimTab] = useState("sim");

  const [overview, setOverview] = useState(EMPTY_OVERVIEW);
  const [recActionPending, setRecActionPending] = useState({});
  const [recCommentByAsset, setRecCommentByAsset] = useState({});
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [assetDetail, setAssetDetail] = useState(null);
  const [hierarchy, setHierarchy] = useState(null);
  const [hierSelection, setHierSelection] = useState(null);
  const [model, setModel] = useState(null);

  const [streamRows, setStreamRows] = useState([]);
  const [streamCount, setStreamCount] = useState(0);
  const [streamFilters, setStreamFilters] = useState({ asset: "", quality: "", proto: "" });

  const [agentInput, setAgentInput] = useState("");
  const [agentMsgs, setAgentMsgs] = useState([]);
  const [genieConversationByIndustry, setGenieConversationByIndustry] = useState({});
  const [agentPending, setAgentPending] = useState(false);
  const [financeInput, setFinanceInput] = useState("");
  const [financeMsgs, setFinanceMsgs] = useState([]);
  const [financeConversationByIndustry, setFinanceConversationByIndustry] = useState({});
  const [financePending, setFinancePending] = useState(false);

  const [simState, setSimState] = useState({
    running: false,
    reading_count: 0,
    tick_interval_ms: 800,
    noise_factor: 0.02,
    rows: [],
    assets: [],
    faults: {},
    asset_sensors: {},
    catalog: "pdm_mining"
  });
  const [simFlow, setSimFlow] = useState({
    industry: "mining",
    bronze: { stage: "bronze_curated", tier: "bronze", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
    silver: { stage: "silver_features", tier: "silver", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
    gold: { stage: "gold_predictions", tier: "gold", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
    stages: []
  });
  const [cfg, setCfg] = useState({
    industry_key: "mining",
    display_name: "",
    catalog: "",
    protocol: "OPC-UA",
    cost_unit: "",
    timezone: "",
    persona: "",
    asset_noun: "",
    downtime_event: "",
    isa_levels: ["Site", "Area", "Unit", "Equipment", "Component"],
    assets: [],
    connector: {
      protocol: "opcua",
      endpoint: "opc.tcp://192.168.1.100:4840",
      security: "None",
      oauth_client_id: "",
      oauth_client_secret: "",
      zerobus_endpoint: "https://1444828305810485.zerobus.us-west-2.zerobuss.cloud.databricks.com",
      zerobus_host: "localhost",
      zerobus_web_port: 8080,
      zerobus_metrics_port: 9090,
      connector_cmd: "python -m opcua2uc",
      workspace_url: "https://e2-demo-field-eng.cloud.databricks.com",
      target_catalog: "pdm_mining",
      target_schema: "bronze",
      target_table: "pravin_zerobus"
    }
  });
  const [cfgYaml, setCfgYaml] = useState("# Fill in the form to generate YAML");

  const [conn, setConn] = useState({
    protocol: "opcua",
    endpoint: "opc.tcp://192.168.1.100:4840",
    oauth_client_id: "6ff2b11b-fdb8-4c2c-9360-ed105d5f6dcb",
    oauth_client_secret: "",
    has_saved_secret: false,
    zerobus_endpoint: "https://1444828305810485.zerobus.us-west-2.zerobuss.cloud.databricks.com",
    workspace_url: "https://e2-demo-field-eng.cloud.databricks.com",
    target_fqn: "pdm_mining.bronze.pravin_zerobus"
  });
  const [connStatus, setConnStatus] = useState({ status: {}, loading: false, processing: false });
  const [connResult, setConnResult] = useState("—");
  const [sdtReport, setSdtReport] = useState({
    summary: [],
    tags: [],
    industry_window_snapshots: [],
    trend_by_industry: {},
    generated_at: "",
    loading: false,
    ticks: 300,
    available_ticks: [300]
  });
  const [sdtMetric, setSdtMetric] = useState("drop");
  const [sdtTicks, setSdtTicks] = useState(300);
  const [discoveredTags, setDiscoveredTags] = useState([]);
  const [tagQuery, setTagQuery] = useState("");
  const [selectedTags, setSelectedTags] = useState([]);
  const [tagMappings, setTagMappings] = useState([]);
  const currencyParam = demoCurrency === "AUTO" ? "" : `&currency=${encodeURIComponent(demoCurrency)}`;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const [ov, h, sim] = await Promise.all([
        getJson(`/api/ui/overview?industry=${industry}${currencyParam}`, EMPTY_OVERVIEW),
        getJson(`/api/ui/hierarchy?industry=${industry}`, null),
        getJson(`/api/ui/simulator/state?industry=${industry}`, null)
      ]);
      if (cancelled) return;
      setOverview(ov);
      setHierarchy(h);
      setHierSelection(h);
      setSimState(sim || {});
      setAgentMsgs((ov.messages || []).map((m) => ({ role: m.role, text: m.text, label: m.label || "AI" })));
      setFinanceMsgs([{
        role: "agent",
        label: "Finance Command AI",
        text: `I can answer financial predictive maintenance scenarios for ${industry} in ${(ov.executive || {}).currency || demoCurrency}.`
      }]);
      const defaultAsset = ov.assets?.[0]?.id || "";
      setSelectedAssetId(defaultAsset);
      const template = await getJson(`/api/ui/config/template?industry=${industry}`, null);
      if (!cancelled && template) {
        setCfg(template);
        const tc = template.connector || {};
        const targetFqn = [tc.target_catalog || template.catalog || `pdm_${industry}`, tc.target_schema || "bronze", tc.target_table || "_zerobus_staging"].join(".");
        setConn((prev) => ({
          ...prev,
          protocol: tc.protocol || prev.protocol,
          endpoint: tc.endpoint || prev.endpoint,
          oauth_client_id: tc.oauth_client_id || "",
          oauth_client_secret: tc.oauth_client_secret || "",
          zerobus_endpoint: tc.zerobus_endpoint || prev.zerobus_endpoint,
          workspace_url: tc.workspace_url || prev.workspace_url,
          target_fqn: targetFqn
        }));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [industry, currencyParam]);

  useEffect(() => {
    if (!selectedAssetId) return;
    let cancelled = false;
    (async () => {
      const [detail, modelData] = await Promise.all([
        getJson(`/api/ui/asset/${selectedAssetId}?industry=${industry}${currencyParam}`, null),
        getJson(`/api/ui/model/${selectedAssetId}?industry=${industry}${currencyParam}`, null)
      ]);
      if (cancelled) return;
      setAssetDetail(detail);
      setModel(modelData);
    })();
    return () => {
      cancelled = true;
    };
  }, [industry, selectedAssetId, currencyParam]);

  useEffect(() => {
    if (page !== "p4") return undefined;
    let alive = true;
    const run = async () => {
      const data = await getJson(`/api/stream/latest?industry=${industry}&limit=60`, { rows: [] });
      if (!alive) return;
      setStreamCount((v) => v + (data.rows || []).length);
      setStreamRows((data.rows || []).slice(0, 200));
    };
    run();
    const timer = setInterval(run, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [page, industry]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const preview = await postJson("/api/ui/config/preview", cfg, { yaml: "# preview unavailable" });
      if (!cancelled) setCfgYaml(preview.yaml || "# preview unavailable");
    })();
    return () => {
      cancelled = true;
    };
  }, [cfg]);

  useEffect(() => {
    const parsedTarget = parseTargetFqn(conn.target_fqn) || {};
    setCfg((prev) => ({
      ...prev,
      connector: {
        protocol: conn.protocol,
        endpoint: conn.endpoint,
        oauth_client_id: conn.oauth_client_id,
        oauth_client_secret: conn.oauth_client_secret,
        zerobus_endpoint: conn.zerobus_endpoint,
        workspace_url: conn.workspace_url,
        workspace_host: conn.workspace_url,
        target_catalog: parsedTarget.catalog || cfg.catalog || `pdm_${industry}`,
        target_schema: parsedTarget.schema || "bronze",
        target_table: parsedTarget.table || "_zerobus_staging",
        target_fqn: conn.target_fqn || ""
      }
    }));
  }, [conn, industry, cfg.catalog]);

  useEffect(() => {
    if (simTab !== "connector") return;
    refreshZerobusStatus();
  }, [simTab, conn.protocol]);

  useEffect(() => {
    if (page !== "p6" || simTab !== "sdt") return;
    loadSdtReport();
  }, [page, simTab, industry, sdtTicks]);

  useEffect(() => {
    if (page !== "p6" || !simState?.running || simTab !== "sim") return undefined;
    let alive = true;
    const run = async () => {
      const data = await postJson("/api/ui/simulator/tick", { industry }, simState);
      if (alive) setSimState((prev) => ({ ...prev, ...data }));
    };
    run();
    const timer = setInterval(run, simState.tick_interval_ms || 800);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [page, simTab, industry, simState?.running, simState?.tick_interval_ms]);

  useEffect(() => {
    if (page !== "p6" || simTab !== "sim") return undefined;
    let alive = true;
    const fallback = {
      industry,
      bronze: { stage: "bronze_curated", tier: "bronze", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
      silver: { stage: "silver_features", tier: "silver", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
      gold: { stage: "gold_predictions", tier: "gold", table: "", rows_30m: 0, rows_5m: 0, rows_prev_5m: 0, rate_change_pct: 0, latest_ts: "", rows: [] },
      stages: []
    };
    const run = async () => {
      const data = await getJson(`/api/ui/simulator/flow?industry=${industry}&limit=24`, fallback);
      if (alive) setSimFlow(data || fallback);
    };
    run();
    const timer = setInterval(run, 2500);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [page, simTab, industry]);

  const selectedAsset = useMemo(
    () => overview.assets.find((a) => a.id === selectedAssetId) || overview.assets[0] || null,
    [overview.assets, selectedAssetId]
  );

  const filteredStream = useMemo(
    () =>
      streamRows.filter((r) => {
        if (streamFilters.asset && r.equipment_id !== streamFilters.asset) return false;
        if (streamFilters.quality && r.quality !== streamFilters.quality) return false;
        if (streamFilters.proto && r.source_protocol !== streamFilters.proto) return false;
        return true;
      }),
    [streamRows, streamFilters]
  );

  const executive = overview.executive || EMPTY_EXECUTIVE;
  const execTips = executive.explainability || {};

  const sdtWindowInsights = useMemo(() => {
    const summary = sdtReport.summary || [];
    const tags = sdtReport.tags || [];
    if (!summary.length) {
      return {
        avgDropPct: 0,
        avgKeptPct: 0,
        bestIndustry: null,
        worstIndustry: null,
        topDropTag: null
      };
    }
    const avgDropPct = summary.reduce((acc, r) => acc + Number(r.drop_pct || 0), 0) / summary.length;
    const avgKeptPct = summary.reduce((acc, r) => acc + Number(r.kept_pct || 0), 0) / summary.length;
    const bestIndustry = [...summary].sort((a, b) => Number(b.drop_pct || 0) - Number(a.drop_pct || 0))[0] || null;
    const worstIndustry = [...summary].sort((a, b) => Number(a.drop_pct || 0) - Number(b.drop_pct || 0))[0] || null;
    const topDropTag = [...tags].sort((a, b) => Number(b.drop_pct || 0) - Number(a.drop_pct || 0))[0] || null;
    return { avgDropPct, avgKeptPct, bestIndustry, worstIndustry, topDropTag };
  }, [sdtReport.summary, sdtReport.tags]);

  const sdtSortedTags = useMemo(() => {
    const tags = [...(sdtReport.tags || [])];
    const metricKey = sdtMetric === "drop" ? "drop_pct" : "kept_pct";
    return tags.sort((a, b) => Number(b[metricKey] || 0) - Number(a[metricKey] || 0));
  }, [sdtReport.tags, sdtMetric]);

  async function sendMessage() {
    if (!agentInput.trim() || agentPending) return;
    const userText = agentInput.trim();
    setAgentMsgs((prev) => [...prev, { role: "user", text: userText, label: "ME" }]);
    setAgentInput("");
    setAgentPending(true);
    try {
      const reply = await postJson(
        "/api/agent/chat",
        {
          industry,
          conversation_id: genieConversationByIndustry[industry] || "",
          messages: [{ role: "user", content: userText }]
        },
        { choices: [{ message: { content: "Unable to get response." } }] }
      );
      if (reply?.conversation_id) {
        setGenieConversationByIndustry((prev) => ({ ...prev, [industry]: reply.conversation_id }));
      }
      const answer = reply?.choices?.[0]?.message?.content || "No response.";
      setAgentMsgs((prev) => [...prev, { role: "agent", text: answer, label: "Maintenance Supervisor AI" }]);
    } finally {
      setAgentPending(false);
    }
  }

  async function setSimRunning(running) {
    const next = await postJson(
      "/api/ui/simulator/control",
      { industry, action: running ? "start" : "stop", tick_interval_ms: simState.tick_interval_ms, noise_factor: simState.noise_factor },
      simState
    );
    setSimState((prev) => ({ ...prev, ...next }));
  }

  async function actOnRecommendation(equipmentId, decision) {
    if (!equipmentId || !decision) return;
    const key = `${equipmentId}:${decision}`;
    setRecActionPending((prev) => ({ ...prev, [key]: true }));
    try {
      const note = String(recCommentByAsset[equipmentId] || "").trim();
      const res = await postJson(
        "/api/ui/recommendation/action",
        { industry, equipment_id: equipmentId, decision, note },
        { ok: false }
      );
      if (res?.ok) {
        const ov = await getJson(
          `/api/ui/overview?industry=${industry}${currencyParam}`,
          { ...EMPTY_OVERVIEW, messages: overview.messages || [] }
        );
        setOverview(ov);
        setRecCommentByAsset((prev) => ({ ...prev, [equipmentId]: "" }));
      }
    } finally {
      setRecActionPending((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
    }
  }

  async function sendFinanceMessage() {
    if (!financeInput.trim() || financePending) return;
    const userText = financeInput.trim();
    setFinanceMsgs((prev) => [...prev, { role: "user", text: userText, label: "ME" }]);
    setFinanceInput("");
    setFinancePending(true);
    try {
      const reply = await postJson(
        "/api/agent/finance_chat",
        {
          industry,
          currency: demoCurrency === "AUTO" ? executive.currency : demoCurrency,
          conversation_id: financeConversationByIndustry[industry] || "",
          messages: [{ role: "user", content: userText }]
        },
        { choices: [{ message: { content: "Unable to get response." } }] }
      );
      if (reply?.conversation_id) {
        setFinanceConversationByIndustry((prev) => ({ ...prev, [industry]: reply.conversation_id }));
      }
      const answer = reply?.choices?.[0]?.message?.content || "No response.";
      setFinanceMsgs((prev) => [...prev, { role: "agent", text: answer, label: "Finance Command AI" }]);
    } finally {
      setFinancePending(false);
    }
  }

  async function updateFault(assetId, patch) {
    const result = await postJson("/api/ui/simulator/fault", { industry, asset_id: assetId, ...patch }, { faults: simState.faults || {} });
    setSimState((prev) => ({ ...prev, faults: result.faults || prev.faults }));
  }

  function updateCfgField(field, value) {
    setCfg((prev) => ({ ...prev, [field]: value }));
  }

  function updateCfgIsa(idx, value) {
    setCfg((prev) => {
      const next = [...prev.isa_levels];
      next[idx] = value;
      return { ...prev, isa_levels: next };
    });
  }

  function addCfgAsset() {
    setCfg((prev) => ({
      ...prev,
      assets: [...prev.assets, { id: `ASSET-${prev.assets.length + 1}`, type: "Equipment", path: "Site / Area / Unit", sensors: [{ name: "sensor_01", unit: "units", warn: "", crit: "" }] }]
    }));
  }

  function removeCfgAsset(idx) {
    setCfg((prev) => ({ ...prev, assets: prev.assets.filter((_, i) => i !== idx) }));
  }

  function updateCfgAsset(idx, field, value) {
    setCfg((prev) => ({
      ...prev,
      assets: prev.assets.map((a, i) => (i === idx ? { ...a, [field]: value } : a))
    }));
  }

  function addCfgSensor(assetIdx) {
    setCfg((prev) => ({
      ...prev,
      assets: prev.assets.map((a, i) => (i === assetIdx ? { ...a, sensors: [...(a.sensors || []), { name: "sensor", unit: "", warn: "", crit: "" }] } : a))
    }));
  }

  function removeCfgSensor(assetIdx, sensorIdx) {
    setCfg((prev) => ({
      ...prev,
      assets: prev.assets.map((a, i) => (i === assetIdx ? { ...a, sensors: (a.sensors || []).filter((_, si) => si !== sensorIdx) } : a))
    }));
  }

  function updateCfgSensor(assetIdx, sensorIdx, field, value) {
    setCfg((prev) => ({
      ...prev,
      assets: prev.assets.map((a, i) =>
        i === assetIdx
          ? { ...a, sensors: (a.sensors || []).map((s, si) => (si === sensorIdx ? { ...s, [field]: value } : s)) }
          : a
      )
    }));
  }

  function resetConnEndpoint(protocol) {
    const defaults = {
      opcua: "opc.tcp://192.168.1.100:4840",
      mqtt: "mqtt://192.168.1.50:1883",
      modbus: "192.168.1.200:502"
    };
    setConn((prev) => ({ ...prev, protocol, endpoint: defaults[protocol] || "" }));
  }

  function connectorConfigFromState() {
    const target = parseTargetFqn(conn.target_fqn) || {
      catalog: cfg.catalog || `pdm_${industry}`,
      schema: "bronze",
      table: "_zerobus_staging"
    };
    return {
      workspace_host: conn.workspace_url,
      zerobus_endpoint: conn.zerobus_endpoint,
      endpoint: conn.endpoint,
      auth: {
        client_id: conn.oauth_client_id,
        client_secret: conn.oauth_client_secret,
        use_saved_secret: !!conn.has_saved_secret && !conn.oauth_client_secret
      },
      target
    };
  }

  async function refreshZerobusStatus() {
    setConnStatus((s) => ({ ...s, loading: true }));
    const status = await getJson("/api/zerobus/status", { status: {} });
    setConnStatus((s) => ({ ...s, loading: false, status: status.status || {} }));
  }

  async function loadSdtReport() {
    setSdtReport((p) => ({ ...p, loading: true }));
    const data = await getJson(`/api/ui/sdt/report?industry=${industry}&ticks=${sdtTicks}`, {
      summary: [],
      tags: [],
      industry_window_snapshots: [],
      trend_by_industry: {},
      generated_at: "",
      ticks: sdtTicks,
      available_ticks: [sdtTicks]
    });
    setSdtReport({
      summary: data.summary || [],
      tags: data.tags || [],
      industry_window_snapshots: data.industry_window_snapshots || [],
      trend_by_industry: data.trend_by_industry || {},
      generated_at: data.generated_at || "",
      loading: false,
      ticks: data.ticks || sdtTicks,
      available_ticks: data.available_ticks || [sdtTicks]
    });
  }

  async function loadSavedZerobusConfig() {
    setConnStatus((s) => ({ ...s, processing: true }));
    const result = await postJson("/api/zerobus/config/load", { protocol: conn.protocol }, { success: false, message: "No saved config" });
    if (result?.config) {
      const c = result.config;
      const fqn = [c?.target?.catalog || cfg.catalog || `pdm_${industry}`, c?.target?.schema || "bronze", c?.target?.table || "_zerobus_staging"].join(".");
      setConn((prev) => ({
        ...prev,
        workspace_url: c.workspace_host || prev.workspace_url,
        zerobus_endpoint: c.zerobus_endpoint || prev.zerobus_endpoint,
        endpoint: c.endpoint || prev.endpoint,
        oauth_client_id: c?.auth?.client_id || "",
        oauth_client_secret: "",
        has_saved_secret: !!c?.auth?.has_client_secret,
        target_fqn: fqn
      }));
    }
    setConnResult(JSON.stringify(result, null, 2));
    setConnStatus((s) => ({ ...s, processing: false }));
    await refreshZerobusStatus();
  }

  async function saveZerobusConfig() {
    setConnStatus((s) => ({ ...s, processing: true }));
    const result = await postJson("/api/zerobus/config", { protocol: conn.protocol, config: connectorConfigFromState() }, { success: false, message: "Save failed" });
    setConnResult(JSON.stringify(result, null, 2));
    if (result?.success) {
      setConn((prev) => ({
        ...prev,
        oauth_client_secret: "",
        has_saved_secret: !!result?.has_client_secret
      }));
    }
    setConnStatus((s) => ({ ...s, processing: false }));
    await refreshZerobusStatus();
  }

  async function testZerobus() {
    setConnStatus((s) => ({ ...s, processing: true }));
    const result = await postJson("/api/zerobus/test", { protocol: conn.protocol, config: connectorConfigFromState() }, { success: false, message: "Test failed" });
    setConnResult(JSON.stringify(result, null, 2));
    setConnStatus((s) => ({ ...s, processing: false }));
  }

  async function startZerobus() {
    setConnStatus((s) => ({ ...s, processing: true }));
    const result = await postJson("/api/zerobus/start", { protocol: conn.protocol }, { success: false, message: "Start failed" });
    setConnResult(JSON.stringify(result, null, 2));
    setConnStatus((s) => ({ ...s, processing: false }));
    await refreshZerobusStatus();
  }

  async function stopZerobus() {
    setConnStatus((s) => ({ ...s, processing: true }));
    const result = await postJson("/api/zerobus/stop", { protocol: conn.protocol }, { success: false, message: "Stop failed" });
    setConnResult(JSON.stringify(result, null, 2));
    setConnStatus((s) => ({ ...s, processing: false }));
    await refreshZerobusStatus();
  }

  function toggleTag(tag) {
    setSelectedTags((prev) => {
      if (prev.includes(tag.id)) return prev.filter((id) => id !== tag.id);
      return [...prev, tag.id];
    });
    setTagMappings((prev) => {
      const exists = prev.find((m) => m.id === tag.id);
      if (exists) return prev.filter((m) => m.id !== tag.id);
      return [
        ...prev,
        {
          id: tag.id,
          name: tag.name,
          isa_level: "Equipment",
          sensor_name: tag.name.split(".").pop().replace(/[^a-z0-9]/gi, "_").toLowerCase(),
          failure_mode: ""
        }
      ];
    });
  }

  function updateMapping(id, field, value) {
    setTagMappings((prev) => prev.map((m) => (m.id === id ? { ...m, [field]: value } : m)));
  }

  function addMappedTagsToConfig() {
    if (!tagMappings.length) return;
    const sensors = tagMappings.map((m) => ({ name: m.sensor_name || "sensor", unit: "units", warn: "", crit: "" }));
    setCfg((prev) => ({
      ...prev,
      assets: [...prev.assets, { id: `ASSET-${prev.assets.length + 1}`, type: "Equipment", path: "Site / Area / Unit", sensors }]
    }));
    setSimTab("config");
  }

  return (
    <>
      <header className="topbar">
        <div className="logo">
          <svg width="24" height="24" viewBox="0 0 26 26" fill="none">
            <polygon points="13,1 24,7 24,19 13,25 2,19 2,7" fill="#FF3621" />
            <polygon points="13,6 20,10 20,18 13,22 6,18 6,10" fill="rgba(0,0,0,.25)" />
            <polygon points="13,10 17,12 17,17 13,19 9,17 9,12" fill="rgba(255,255,255,.35)" />
          </svg>
          <span className="logo-text">Databricks</span>
        </div>
        <div className="topbar-div" />
        <span className="app-name">OT PdM Intelligence</span>
        <div className="ind-tabs">
          {INDUSTRIES.map((ind) => (
            <button key={ind} className={`itab ${industry === ind ? "active" : ""}`} onClick={() => setIndustry(ind)}>
              {ind.charAt(0).toUpperCase() + ind.slice(1)}
            </button>
          ))}
        </div>
        <div className="currency-wrap">
          <span className="currency-lbl">Currency</span>
          <select className="currency-sel" value={demoCurrency} onChange={(e) => setDemoCurrency(e.target.value)}>
            {CURRENCIES.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <span className="isa-badge">ISA-95 · Unity Catalog</span>
      </header>

      <div className="body-wrap">
        <nav className="sidebar">
          {PAGE_META.map(([pid, label, icon]) => (
            <button key={pid} className={`nav-btn ${page === pid ? "active" : ""}`} onClick={() => setPage(pid)}>
              <span style={{ fontSize: 16, lineHeight: 1 }}>{icon}</span>
              <span>{label}</span>
            </button>
          ))}
        </nav>

        <div className={`page ${page === "p1" ? "active" : ""}`} id="p1">
          <div className="p1-topbar">
            <div className="kpi-strip" style={{ flex: 1, borderBottom: "none" }}>
              <div className="kpi">
                <div className="kpi-l">Fleet Health</div>
                <div className="kpi-v g">{overview.kpis.fleet_health_score}%</div>
                <div className="kpi-d">Average health score</div>
              </div>
              <div className="kpi">
                <div className="kpi-l">Critical Assets</div>
                <div className="kpi-v r">{overview.kpis.critical_assets}</div>
                <div className="kpi-d">Need immediate action</div>
              </div>
              <div className="kpi">
                <div className="kpi-l">Asset Count</div>
                <div className="kpi-v a">{overview.kpis.asset_count}</div>
                <div className="kpi-d">In monitored fleet</div>
              </div>
            </div>
            <div className="view-toggle-wrap">
              <button className={`view-btn ${view === "operator" ? "active" : ""}`} onClick={() => setView("operator")}>Operator</button>
              <button className={`view-btn ${view === "executive" ? "active" : ""}`} onClick={() => setView("executive")}>Executive</button>
            </div>
          </div>

          <div className={`view-panel ${view === "operator" ? "active" : ""}`} id="view-operator">
            <div className="p1-main">
              <div className="asset-panel">
                <div className="panel-hdr">
                  <span className="panel-title">Live asset risk matrix</span>
                  <div className="chips">
                    <button className="chip active">All</button>
                    <button className="chip">Critical</button>
                    <button className="chip">Warning</button>
                  </div>
                </div>
                <div className="asset-grid">
                  {overview.assets.map((a) => (
                    <div
                      key={a.id}
                      className={`asset-card ${a.status}`}
                      onClick={() => {
                        setSelectedAssetId(a.id);
                        setPage("p2");
                      }}
                    >
                      <div className="card-top">
                        <div className="hring" style={healthRing(a.health_score_pct)}>
                          <span className="hpct" style={{ color: statusColor(a.status) }}>{a.health_score_pct}%</span>
                        </div>
                        <div className="ai">
                          <div className="aid">{a.id}</div>
                          <div className="atype">{a.type}</div>
                          <div className="acrumb">{a.crumb}</div>
                        </div>
                        <span className={`sbadge ${a.status}`}>{a.status}</span>
                      </div>
                      <div className="card-metrics">
                        <div className="cm"><div className="cml">Anomaly</div><div className="cmv">{a.anomaly_score}</div></div>
                        <div className="cm"><div className="cml">RUL</div><div className="cmv">{a.rul_hours}h</div></div>
                        <div className="cm"><div className="cml">Exposure</div><div className="cmv">{a.cost_exposure}</div></div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="agent-panel">
                <div className="agent-hdr">
                  <div className="adot" />
                  <div>
                    <div className="atitle">Maintenance Supervisor AI</div>
                    <div className="asubt">Operational diagnosis and actions</div>
                  </div>
                </div>
                <div className="msgs">
                  {agentMsgs.map((m, i) => (
                    <div className="msg" key={`${m.role}-${i}`}>
                      <div className={`av ${m.role === "user" ? "user" : "agent"}`}>{m.role === "user" ? "ME" : "AI"}</div>
                      <div className={`bubble ${m.role === "user" ? "user" : ""}`}>
                        {m.role === "agent" && <div className="bubble-lbl">{m.label}</div>}
                        {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                      </div>
                    </div>
                  ))}
                  {agentPending && (
                    <div className="msg">
                      <div className="av agent">AI</div>
                      <div className="bubble bubble-thinking">
                        <div className="bubble-lbl">Maintenance Supervisor AI</div>
                        <div className="thinking-row">
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span>Thinking...</span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <div className="agent-inp">
                  <input className="ainput" value={agentInput} onChange={(e) => setAgentInput(e.target.value)} onKeyDown={(e) => e.key === "Enter" && sendMessage()} placeholder={agentPending ? "Processing your request..." : "Ask about risk, RUL, and next action..."} disabled={agentPending} />
                  <button className="sbtn" onClick={sendMessage} disabled={agentPending}>{agentPending ? "Processing..." : "Send"}</button>
                </div>
              </div>
            </div>

            <div className="alert-bar">
              <div className="alert-hdr">Recent alerts</div>
              <div className="arows">
                {(overview.alerts || []).map((a, i) => (
                  <div key={`${a.text}-${i}`} className={`arow ${a.severity}`}>
                    <div className={`apip ${a.severity}`} />
                    <span className="atext">{a.text}</span>
                    <span className="atime">{a.time}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className={`view-panel ${view === "executive" ? "active" : ""}`}>
            <div className="exec-wrap">
              <div className="exec-hero-card">
                <div className="exec-hero-eyebrow">Executive briefing value statement</div>
                <div className="exec-hero-title">{executive.value_statement || EMPTY_EXECUTIVE.value_statement}</div>
                <div className="exec-hero-sub">
                  Finance lens for {industry} skin ({executive.window || "last_30_days_simulated"}) to support EBC storytelling.
                </div>
              </div>

              <div className="exec-finance-row">
                <div className="exec-fin-card">
                  <div className="exec-fin-label">EBIT Saved</div>
                  <div className="exec-fin-val" title={execTips.ebit_saved || ""}>{executive.ebit_saved_fmt || "—"}</div>
                  <div className="exec-fin-sub">Net impact from prescriptive maintenance</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">ROI</div>
                  <div className="exec-fin-val" title={execTips.roi_pct || ""}>{Number(executive.roi_pct || 0).toFixed(1)}%</div>
                  <div className="exec-fin-sub">Savings versus intervention + platform cost</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">Payback</div>
                  <div className="exec-fin-val" title={execTips.payback_days || ""}>{Number(executive.payback_days || 0).toFixed(1)} days</div>
                  <div className="exec-fin-sub">Estimated time to recover investment</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">EBIT Margin Lift</div>
                  <div className="exec-fin-val" title={execTips.ebit_margin_bps || ""}>{Number(executive.ebit_margin_bps || 0).toFixed(1)} bps</div>
                  <div className="exec-fin-sub" title={execTips.baseline_monthly_ebit || ""}>Versus baseline monthly EBIT ({executive.baseline_monthly_ebit_fmt || "n/a"})</div>
                </div>
              </div>

              <div className="exec-finance-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">Value bridge to EBIT</div>
                  {(executive.value_bridge || []).map((b) => (
                    <div key={b.label} className="exec-bridge-row">
                      <span className="exec-bridge-label">{b.label}</span>
                      <span className={`exec-bridge-val ${b.kind === "negative" ? "neg" : "pos"}`} title={execTips.ebit_saved || ""}>{b.amount_fmt}</span>
                    </div>
                  ))}
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">ERP and work-order context</div>
                  <div className="exec-erp-grid">
                    <div><span className="exec-erp-k">Plant</span><span className="exec-erp-v">{executive.erp?.plant_code || "—"}</span></div>
                    <div><span className="exec-erp-k">Fiscal period</span><span className="exec-erp-v">{executive.erp?.fiscal_period || "—"}</span></div>
                    <div><span className="exec-erp-k">Planner group</span><span className="exec-erp-v">{executive.erp?.planner_group || "—"}</span></div>
                    <div><span className="exec-erp-k">Account</span><span className="exec-erp-v">{executive.erp?.reference_account || "—"}</span></div>
                  </div>
                  <div className="exec-chip-row">
                    {(executive.erp?.cost_centers || []).map((c) => <span key={c} className="exec-chip">{c}</span>)}
                  </div>
                </div>
              </div>

              <div className="exec-summary-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">EBIT impact trend (simulated)</div>
                  <TrendLine values={(executive.ebit_trend || []).map((p) => Number(p.value || 0))} color="#0F766E" height={120} />
                  <div className="exec-chip-row">
                    {(executive.ebit_trend || []).map((p) => (
                      <span key={`trend-${p.label}`} className="exec-chip" title={execTips.ebit_saved || ""}>{p.label}: {p.value_fmt}</span>
                    ))}
                  </div>
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">Financial impact by work order</div>
                  {(executive.work_orders || []).slice(0, 6).map((w) => (
                    <div key={w.wo_id} className="exec-wo-row">
                      <div>
                        <div className="exec-wo-id">{w.wo_id}</div>
                        <div className="exec-wo-meta">{w.equipment_id} · {w.priority} · {w.work_center}</div>
                      </div>
                      <div className="exec-wo-impact" title={execTips.ebit_saved || ""}>{w.net_ebit_impact_fmt}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className={`page ${page === "p2" ? "active" : ""}`} id="p2">
          {assetDetail && (
            <div className="p2-wrap">
              <div className="p2-hdr">
                <button className="back-btn" onClick={() => setPage("p1")}>Fleet</button>
                <div className="p2-meta">
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <div className="p2-id">{assetDetail.id}</div>
                    <span className={`sbadge ${assetDetail.status}`}>{assetDetail.status}</span>
                  </div>
                  <div className="p2-type">{assetDetail.type}</div>
                  <div className="p2-crumb">{assetDetail.crumb}</div>
                </div>
              </div>

              <div className="stat-cards">
                <div className="stat-card"><div className="sc-label">Health score</div><div className="sc-val" style={{ color: statusColor(assetDetail.status) }}>{assetDetail.health_score_pct}%</div><div className="sc-sub">{assetDetail.status}</div></div>
                <div className="stat-card"><div className="sc-label">RUL remaining</div><div className="sc-val">{assetDetail.rul_hours}h</div><div className="sc-sub">Estimated hours to failure</div></div>
                <div className="stat-card"><div className="sc-label">Anomaly score</div><div className="sc-val">{assetDetail.anomaly_score}</div><div className="sc-sub">Isolation forest</div></div>
                <div className="stat-card"><div className="sc-label">Protocol</div><div className="sc-val" style={{ fontSize: 18 }}>{model?.model_meta?.protocol || "OPC-UA"}</div><div className="sc-sub">Zerobus ingestion</div></div>
              </div>

              <div>
                <div className="sect-title">Sensor readings — live</div>
                <div className="sensor-grid">
                  {(assetDetail.sensors || []).map((s) => (
                    <div key={s.name} className={`sensor-tile ${s.status === "critical" ? "anomalous" : s.status === "warning" ? "warning" : ""}`}>
                      <div className="st-top"><div><div className="st-name">{s.label}</div><div className="st-unit">{s.unit}</div></div></div>
                      <div className="st-val" style={{ color: statusColor(s.status) }}>{s.value}<span style={{ fontSize: 12, color: "var(--muted)" }}> {s.unit}</span></div>
                      <div className="st-trend">{s.trend}</div>
                      <TrendLine values={(s.history || []).slice(-18)} color={s.status === "critical" ? "#DC2626" : s.status === "warning" ? "#D97706" : "#16A34A"} height={60} />
                    </div>
                  ))}
                </div>
              </div>
              <div className="anomaly-timeline">
                <div className="sect-title">Anomaly score — 72h history</div>
                <TrendLine values={(assetDetail.anomaly_history || []).map((v) => Number(v) * 100)} color="#FF3621" height={100} showXLabels />
              </div>
            </div>
          )}
        </div>

        <div className={`page ${page === "p3" ? "active" : ""}`} id="p3">
          <div className="p3-wrap">
            <div className="tree-panel">
              <div className="tree-hdr">Asset hierarchy</div>
              {hierarchy && <TreeNode node={hierarchy} onSelect={setHierSelection} />}
            </div>
            <div className="detail-panel">
              {hierSelection ? (
                <>
                  <div className="node-detail-hdr">
                    <div className="nd-icon-wrap">{hierSelection.icon || "📦"}</div>
                    <div>
                      <div className="nd-name">{hierSelection.label}</div>
                      <div className="nd-level">{hierSelection.level}</div>
                    </div>
                    <div style={{ marginLeft: "auto", textAlign: "right" }}>
                      <div style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 28, fontWeight: 500 }}>{hierSelection.health}%</div>
                      <div style={{ fontSize: 11, color: "var(--muted)" }}>health score</div>
                    </div>
                  </div>
                  {!!hierSelection.children?.length && (
                    <div className="health-breakdown">
                      <div className="sect-title" style={{ marginBottom: 8 }}>Health rollup</div>
                      {hierSelection.children.map((c, i) => (
                        <div key={`hb-${i}`} className="hb-row">
                          <span className="hb-label">{c.icon || "📦"} {c.label}</span>
                          <div className="hb-bar-wrap"><div className="hb-bar" style={{ width: `${c.health}%`, background: c.health >= 80 ? "#16A34A" : c.health >= 60 ? "#D97706" : "#DC2626" }} /></div>
                          <span className="hb-pct">{c.health}%</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {!!hierSelection.children?.length && (
                    <div>
                      <div className="sect-title" style={{ marginBottom: 8 }}>Child nodes ({hierSelection.children.length})</div>
                      <div className="children-grid">
                        {hierSelection.children.map((c, i) => (
                          <div key={`cc-${i}`} className="child-card" onClick={() => setHierSelection(c)}>
                            <div className="cc-id">{c.label}</div>
                            <div className="cc-type">{c.level}</div>
                            <div className="cc-health">{c.health}%</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {!!hierSelection.asset_id && (
                    <button className="back-btn" style={{ marginTop: 4 }} onClick={() => { setSelectedAssetId(hierSelection.asset_id); setPage("p2"); }}>
                      View asset drilldown →
                    </button>
                  )}
                </>
              ) : (
                <div style={{ color: "var(--muted)", marginTop: 32 }}>Select a node to view details</div>
              )}
            </div>
          </div>
        </div>

        <div className={`page ${page === "p4" ? "active" : ""}`} id="p4">
          <div className="p4-wrap">
            <div className="stream-controls">
              <div className="sc-live"><div className="live-dot" />Live stream</div>
              <select className="sc-select" value={streamFilters.asset} onChange={(e) => setStreamFilters((p) => ({ ...p, asset: e.target.value }))}>
                <option value="">All assets</option>
                {overview.assets.map((a) => <option key={`af-${a.id}`} value={a.id}>{a.id}</option>)}
              </select>
              <select className="sc-select" value={streamFilters.quality} onChange={(e) => setStreamFilters((p) => ({ ...p, quality: e.target.value }))}>
                <option value="">All quality</option><option value="good">Good only</option><option value="bad">Bad only</option><option value="uncertain">Uncertain only</option>
              </select>
              <select className="sc-select" value={streamFilters.proto} onChange={(e) => setStreamFilters((p) => ({ ...p, proto: e.target.value }))}>
                <option value="">All protocols</option><option value="OPC-UA">OPC-UA</option><option value="MQTT">MQTT</option><option value="Modbus">Modbus</option>
              </select>
              <span className="sc-count">{streamCount.toLocaleString()} readings</span>
            </div>
            <div className="stream-table-wrap">
              <table className="stream-table">
                <thead><tr><th>Timestamp</th><th>Asset</th><th>Tag name</th><th>Value</th><th>Unit</th><th>Quality</th><th>Protocol</th></tr></thead>
                <tbody>
                  {filteredStream.map((r, i) => (
                    <tr key={`${r.equipment_id}-${r.tag_name}-${i}`} className={r.quality !== "good" ? r.quality : ""}>
                      <td className="ts-cell">{r.timestamp}</td>
                      <td className="mono">{r.equipment_id}</td>
                      <td className="tag-name">{r.tag_name}</td>
                      <td className="val-cell">{r.value}</td>
                      <td>{r.unit}</td>
                      <td><span className={`q-badge ${r.quality}`}>{r.quality}</span></td>
                      <td><span className={`proto-badge ${(r.source_protocol || "").toLowerCase().replace(/[^a-z]/g, "")}`}>{r.source_protocol}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div className={`page ${page === "p5" ? "active" : ""}`} id="p5">
          <div className="p5-wrap">
            <div className="p5-asset-sel">
              <span className="p5-label">Asset</span>
              <select className="p5-select" value={selectedAssetId} onChange={(e) => setSelectedAssetId(e.target.value)}>
                {overview.assets.map((a) => <option key={`p5-${a.id}`} value={a.id}>{a.id} — {a.type}</option>)}
              </select>
            </div>

            {model && (
              <>
                <div className="model-meta">
                  <div className="mm-item"><div className="mm-label">Model trained</div><div className="mm-val">{model.model_meta.trained}</div></div>
                  <div className="mm-item"><div className="mm-label">RUL accuracy (R²)</div><div className="mm-val">{model.model_meta.r2}</div></div>
                  <div className="mm-item"><div className="mm-label">RMSE</div><div className="mm-val">{model.model_meta.rmse}</div></div>
                </div>
                <div className="p5-grid">
                  <div className="chart-card full">
                    <div className="cc-title">RUL degradation curve</div>
                    <div className="rul-stats">
                      <div className="rul-stat"><span className="rul-stat-l">Current RUL</span><span className="rul-stat-v">{model.rul_hours}h</span></div>
                    </div>
                    <TrendLine values={model.rul_curve.values || []} color="#1B2431" height={180} showXLabels />
                  </div>
                  <div className="chart-card">
                    <div className="cc-title">Feature importance — anomaly model</div>
                    <div className="anomaly-decomp">
                      {model.feature_importance.map((f) => (
                        <div key={f.name} className="ad-row">
                          <div className="ad-name">{f.name}</div>
                          <div className="ad-bar-wrap"><div className="ad-bar" style={{ width: `${Math.round(f.score * 100)}%` }} /></div>
                          <div className="ad-score">{f.score}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="chart-card">
                    <div className="cc-title">Anomaly score decomposition</div>
                    <div className="anomaly-decomp">
                      {model.anomaly_decomposition.map((f) => (
                        <div key={`d-${f.name}`} className="ad-row">
                          <div className="ad-name">{f.name}</div>
                          <div className="ad-bar-wrap"><div className="ad-bar" style={{ width: `${Math.round(f.score * 100)}%`, background: "#D97706" }} /></div>
                          <div className="ad-score">{f.score}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>

        <div className={`page ${page === "p6" ? "active" : ""}`} id="p6">
          <div className="p6-inner-tabs">
            <button className={`p6itab ${simTab === "sim" ? "active" : ""}`} onClick={() => setSimTab("sim")}>Simulator</button>
            <button className={`p6itab ${simTab === "config" ? "active" : ""}`} onClick={() => setSimTab("config")}>Industry Configuration</button>
            <button className={`p6itab ${simTab === "connector" ? "active" : ""}`} onClick={() => setSimTab("connector")}>Connector Setup</button>
            <button className={`p6itab ${simTab === "sdt" ? "active" : ""}`} onClick={() => setSimTab("sdt")}>SDT Benchmark</button>
          </div>

          <div className={`p6-panel ${simTab === "sim" ? "active" : ""}`}>
            <div className="p6-wrap">
              <div className="sim-controls">
                <div className="sim-ctrl-group"><span className="sim-ctrl-label">Tick interval</span><input className="sim-range" type="range" min="200" max="2000" step="100" value={simState.tick_interval_ms || 800} onChange={(e) => setSimState((p) => ({ ...p, tick_interval_ms: Number(e.target.value) }))} /><span className="sim-ctrl-val">{simState.tick_interval_ms || 800}ms</span></div>
                <div className="sim-ctrl-group"><span className="sim-ctrl-label">Noise factor</span><input className="sim-range" type="range" min="1" max="20" step="1" value={Math.round((simState.noise_factor || 0.02) * 100)} onChange={(e) => setSimState((p) => ({ ...p, noise_factor: Number(e.target.value) / 100 }))} /><span className="sim-ctrl-val">{(simState.noise_factor || 0.02).toFixed(2)}</span></div>
                <button className="sim-start" style={{ display: simState.running ? "none" : "inline-block" }} onClick={() => setSimRunning(true)}>▶ Start simulator</button>
                <button className="sim-stop" style={{ display: simState.running ? "inline-block" : "none" }} onClick={() => setSimRunning(false)}>▮▮ Stop</button>
                <div className="sim-status"><div className={`sim-dot ${simState.running ? "running" : ""}`} /><span>{simState.running ? "Running" : "Stopped"}</span></div>
                <span className="sim-reading-count">{(simState.reading_count || 0).toLocaleString()} readings emitted</span>
              </div>
              <div className="p6-main">
                <div className="asset-config-panel">
                  {(simState.assets || []).map((a) => (
                    <div key={`sa-${a.id}`} className="sim-asset-card">
                      <div className="sac-top">
                        <div><div className="sac-id">{a.id}</div><div className="sac-type">{(a.type || "").replaceAll("_", " ")}</div></div>
                        <div className="fault-toggle-wrap">
                          <span className="fault-toggle-label">Fault inject</span>
                          <label className="ftoggle">
                            <input type="checkbox" checked={!!simState.faults?.[a.id]?.enabled} onChange={(e) => updateFault(a.id, { enabled: e.target.checked })} />
                            <span className="fslider" />
                          </label>
                        </div>
                      </div>
                      <div className={`fault-controls ${simState.faults?.[a.id]?.enabled ? "visible" : ""}`}>
                        <div className="fc-row">
                          <span className="fc-label">Mode</span>
                          <select className="fc-select" value={simState.faults?.[a.id]?.mode || a.inject_fault || "degradation"} onChange={(e) => updateFault(a.id, { mode: e.target.value })}>
                            <option value="degradation">degradation</option>
                            {Array.from(new Set((simState.asset_sensors?.[a.id] || []).map((s) => s.failure_mode).filter(Boolean))).map((m) => (
                              <option key={`${a.id}-${m}`} value={m}>{m}</option>
                            ))}
                          </select>
                        </div>
                        <div className="fc-row">
                          <span className="fc-label">Severity</span>
                          <input className="fc-severity" type="range" min="1" max="100" value={simState.faults?.[a.id]?.severity || 1} onChange={(e) => updateFault(a.id, { severity: Number(e.target.value) })} />
                          <span className="fc-sev-val">{simState.faults?.[a.id]?.severity || 1}%</span>
                        </div>
                      </div>
                      <div className="affected-sensors">
                        <div className="as-label">Sensors</div>
                        <div className="sensor-pills">
                          {(simState.asset_sensors?.[a.id] || []).slice(0, 8).map((s) => {
                            const active = simState.faults?.[a.id]?.enabled && (simState.faults?.[a.id]?.mode === s.failure_mode || simState.faults?.[a.id]?.mode === "degradation");
                            return <span key={`${a.id}-${s.name}`} className={`spill ${active ? "affected" : ""}`}>{s.name}</span>;
                          })}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
                <div className="bronze-panel">
                  <div className="bronze-hdr">
                    <div>
                      <div className="bronze-title">Live ingestion flow</div>
                      <div className="bronze-subtitle">3 stages: Bronze → Silver → Gold (10 recent rows each)</div>
                    </div>
                  </div>
                  <div className="flow-viewport">
                    <div className="flow-chain">
                      {(simFlow.stages || [
                        simFlow.bronze,
                        simFlow.silver,
                        simFlow.gold
                      ]).map((data, idx) => (
                        <div key={`${data.stage || "stage"}-${idx}`} className="flow-stage-wrap">
                          <div className={`flow-stage ${String(data.tier || "bronze")}`}>
                            <div className="flow-stage-hdr">
                              <div className="flow-stage-title">
                                {idx + 1}) {String(data.stage || "").replaceAll("_", " ")}
                              </div>
                              <div className={`flow-tier-badge ${String(data.tier || "bronze")}`}>{String(data.tier || "bronze").toUpperCase()}</div>
                              <div className="flow-stage-meta">{data.table || "table unavailable"}</div>
                              <div className="flow-stage-transform">
                                Transformation: {{
                                  bronze_curated: "Read Zerobus landing + validate + normalize in one Bronze table",
                                  silver_features: "Compute rolling feature statistics per asset/tag",
                                  gold_predictions: "Generate anomaly/RUL prediction outputs"
                                }[String(data.stage || "")] || "Pipeline stage processing"}
                              </div>
                              <div className="flow-stage-kpi">
                                <span>{Number(data.rows_30m || 0).toLocaleString()} / 30m</span>
                                <span>{Number(data.rows_5m || 0).toLocaleString()} / 5m</span>
                                <span className={`flow-rate ${(Number(data.rate_change_pct || 0) >= 0) ? "up" : "down"}`}>
                                  {Number(data.rate_change_pct || 0) >= 0 ? "+" : ""}{Number(data.rate_change_pct || 0).toFixed(1)}%
                                </span>
                                <span>{data.latest_ts ? new Date(data.latest_ts).toLocaleTimeString() : "no recent rows"}</span>
                              </div>
                            </div>
                            <div className="bronze-table-wrap">
                              <table className="bronze-table flow-table">
                                <thead><tr><th>Timestamp</th><th>Equipment</th><th>Tag</th><th>Value</th><th>Q</th><th>Protocol</th></tr></thead>
                                <tbody>
                                  {(data.rows || []).slice(0, 10).map((r, i) => (
                                    <tr key={`${data.stage}-row-${i}`} className={r.quality !== "good" ? r.quality : ""}>
                                      <td className="mono">{r.timestamp}</td>
                                      <td className="mono">{r.equipment_id}</td>
                                      <td className="mono">{r.tag_name}</td>
                                      <td className="mono">{r.value}</td>
                                      <td><span className={`q-badge ${r.quality}`}>{r.quality}</span></td>
                                      <td><span className={`proto-badge ${(r.source_protocol || "").toLowerCase().replace(/[^a-z]/g, "")}`}>{r.source_protocol}</span></td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        {idx < 2 && <div className="flow-arrow">↓</div>}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className={`p6-panel ${simTab === "config" ? "active" : ""}`}>
            <div className="cfg-wrap">
              <div className="cfg-left">
                <div className="cfg-section">
                  <div className="cfg-sec-title">Industry basics</div>
                  <div className="cfg-row"><label className="cfg-lbl">Industry key</label><input className="cfg-inp" value={cfg.industry_key || ""} onChange={(e) => updateCfgField("industry_key", e.target.value)} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Display name</label><input className="cfg-inp" value={cfg.display_name || ""} onChange={(e) => updateCfgField("display_name", e.target.value)} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Catalog</label><input className="cfg-inp" value={cfg.catalog || ""} onChange={(e) => updateCfgField("catalog", e.target.value)} /></div>
                  <div className="cfg-row">
                    <label className="cfg-lbl">Protocol</label>
                    <select className="cfg-sel" value={cfg.protocol || "OPC-UA"} onChange={(e) => updateCfgField("protocol", e.target.value)}>
                      <option>OPC-UA</option><option>MQTT</option><option>Modbus</option><option>CAN bus</option>
                    </select>
                  </div>
                  <div className="cfg-row"><label className="cfg-lbl">Cost unit</label><input className="cfg-inp" value={cfg.cost_unit || ""} onChange={(e) => updateCfgField("cost_unit", e.target.value)} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Timezone</label><input className="cfg-inp" value={cfg.timezone || ""} onChange={(e) => updateCfgField("timezone", e.target.value)} /></div>
                </div>

                <div className="cfg-section">
                  <div className="cfg-sec-title">ISA-95 hierarchy levels</div>
                  {(cfg.isa_levels || []).map((lvl, i) => (
                    <div key={`isa-${i}`} className="cfg-row">
                      <label className="cfg-lbl">Level {i + 1}</label>
                      <input className="cfg-inp" value={lvl || ""} onChange={(e) => updateCfgIsa(i, e.target.value)} />
                    </div>
                  ))}
                </div>

                <div className="cfg-section">
                  <div className="cfg-sec-title" style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    Assets
                    <button className="cfg-add-btn" onClick={addCfgAsset}>+ Add asset</button>
                  </div>
                  {(cfg.assets || []).map((a, ai) => (
                    <div key={`ca-${ai}`} className="cfg-asset-row">
                      <div className="car-top">
                        <input className="cfg-inp" style={{ width: 80, flex: "none" }} value={a.id || ""} onChange={(e) => updateCfgAsset(ai, "id", e.target.value)} />
                        <input className="cfg-inp" style={{ flex: 1 }} value={a.type || ""} onChange={(e) => updateCfgAsset(ai, "type", e.target.value)} />
                        <button className="car-del" onClick={() => removeCfgAsset(ai)}>✕</button>
                      </div>
                      <div className="cfg-row" style={{ marginBottom: 4 }}>
                        <label className="cfg-lbl" style={{ width: 64, fontSize: 10 }}>ISA-95 path</label>
                        <input className="cfg-inp" value={a.path || ""} onChange={(e) => updateCfgAsset(ai, "path", e.target.value)} />
                      </div>
                      <div style={{ fontSize: 10, color: "var(--muted)", textTransform: "uppercase", letterSpacing: ".4px", margin: "6px 0 4px" }}>
                        Sensors <span style={{ fontSize: 9, color: "#94A3B8", marginLeft: 4 }}>name · unit · warn · crit</span>
                      </div>
                      <div className="sensor-list">
                        {(a.sensors || []).map((s, si) => (
                          <div key={`cs-${ai}-${si}`} className="sensor-row">
                            <input className="sr-inp" value={s.name || ""} onChange={(e) => updateCfgSensor(ai, si, "name", e.target.value)} />
                            <input className="sr-inp" value={s.unit || ""} onChange={(e) => updateCfgSensor(ai, si, "unit", e.target.value)} />
                            <input className="sr-inp" value={s.warn || ""} onChange={(e) => updateCfgSensor(ai, si, "warn", e.target.value)} />
                            <input className="sr-inp" value={s.crit || ""} onChange={(e) => updateCfgSensor(ai, si, "crit", e.target.value)} />
                            <button className="sr-del" onClick={() => removeCfgSensor(ai, si)}>✕</button>
                          </div>
                        ))}
                      </div>
                      <button className="add-sensor-btn" onClick={() => addCfgSensor(ai)}>+ Add sensor</button>
                    </div>
                  ))}
                </div>

                <div className="cfg-section">
                  <div className="cfg-sec-title">Agent persona</div>
                  <div className="cfg-row"><label className="cfg-lbl">Persona name</label><input className="cfg-inp" value={cfg.persona || ""} onChange={(e) => updateCfgField("persona", e.target.value)} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Asset noun</label><input className="cfg-inp" value={cfg.asset_noun || ""} onChange={(e) => updateCfgField("asset_noun", e.target.value)} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Downtime event</label><input className="cfg-inp" value={cfg.downtime_event || ""} onChange={(e) => updateCfgField("downtime_event", e.target.value)} /></div>
                </div>
              </div>

              <div className="cfg-right">
                <div className="cfg-preview-hdr">
                  <span className="cfg-preview-title">Generated YAML preview</span>
                  <button className="cfg-copy-btn" onClick={() => navigator.clipboard.writeText(cfgYaml)}>Copy</button>
                </div>
                <pre className="cfg-yaml-pre">{cfgYaml}</pre>
              </div>
            </div>
          </div>
          <div className={`p6-panel ${simTab === "connector" ? "active" : ""}`}>
            <div className="cfg-wrap">
              <div className="cfg-left">
                <div className="cfg-section">
                  <div className="cfg-sec-title" style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    ZeroBus
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button className="cfg-add-btn" onClick={refreshZerobusStatus} disabled={connStatus.loading || connStatus.processing}>Refresh status</button>
                      <button className="cfg-add-btn" onClick={loadSavedZerobusConfig} disabled={connStatus.loading || connStatus.processing}>{connStatus.loading ? "Loading..." : "Load saved"}</button>
                      <button className="cfg-add-btn" onClick={saveZerobusConfig} disabled={connStatus.processing}>Save</button>
                      <button className="cfg-add-btn" onClick={testZerobus} disabled={connStatus.processing}>Test</button>
                      <button className="cfg-add-btn" onClick={startZerobus} disabled={connStatus.processing || !connStatus.status?.[conn.protocol]?.has_config}>Start streaming</button>
                      <button className="cfg-add-btn" onClick={stopZerobus} disabled={connStatus.processing || !connStatus.status?.[conn.protocol]?.active}>Stop streaming</button>
                    </div>
                  </div>
                  <div className="cfg-row">
                    <label className="cfg-lbl">Protocol</label>
                    <select className="cfg-sel" value={conn.protocol} onChange={(e) => resetConnEndpoint(e.target.value)}>
                      <option value="opcua">OPC-UA</option>
                      <option value="mqtt">MQTT</option>
                      <option value="modbus">Modbus</option>
                    </select>
                  </div>
                  <div className="cfg-row"><label className="cfg-lbl">Workspace host</label><input className="cfg-inp" value={conn.workspace_url} onChange={(e) => setConn((p) => ({ ...p, workspace_url: e.target.value }))} placeholder="https://adb-..." /></div>
                  <div className="cfg-row"><label className="cfg-lbl">ZeroBus endpoint</label><input className="cfg-inp" value={conn.zerobus_endpoint} onChange={(e) => setConn((p) => ({ ...p, zerobus_endpoint: e.target.value }))} placeholder=".zerobus..cloud.databricks.com" /></div>
                  <div className="cfg-row"><label className="cfg-lbl">OT endpoint</label><input className="cfg-inp" value={conn.endpoint} onChange={(e) => setConn((p) => ({ ...p, endpoint: e.target.value }))} /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Target table (catalog.schema.table)</label><input className="cfg-inp" value={conn.target_fqn} onChange={(e) => setConn((p) => ({ ...p, target_fqn: e.target.value }))} placeholder="main.iot_data.sensor_readings" /></div>
                  <div className="cfg-row"><label className="cfg-lbl">Client ID</label><input className="cfg-inp" value={conn.oauth_client_id} onChange={(e) => setConn((p) => ({ ...p, oauth_client_id: e.target.value }))} /></div>
                  <div className="cfg-row">
                    <label className="cfg-lbl">Client secret</label>
                    <input
                      type="password"
                      className="cfg-inp"
                      value={conn.oauth_client_secret}
                      onChange={(e) => setConn((p) => ({ ...p, oauth_client_secret: e.target.value }))}
                      placeholder={conn.has_saved_secret ? "Stored securely (leave blank to keep)" : ""}
                    />
                  </div>
                  <div style={{ color: "var(--muted)", fontSize: 12 }}>
                    Status: {connStatus.status?.[conn.protocol]?.active ? "streaming active" : "inactive"}; saved config: {connStatus.status?.[conn.protocol]?.has_config ? "yes" : "no"}
                  </div>
                </div>
              </div>
              <div className="cfg-right">
                <div className="cfg-section" style={{ height: "100%", display: "flex", flexDirection: "column" }}>
                  <div className="cfg-sec-title">Last result</div>
                  <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 8 }}>Raw response</div>
                  <pre className="cfg-yaml-pre" style={{ margin: 0, whiteSpace: "pre-wrap" }}>{connResult || "—"}</pre>
                </div>
              </div>
            </div>
          </div>
          <div className={`p6-panel ${simTab === "sdt" ? "active" : ""}`}>
            <div className="cfg-wrap">
              <div className="cfg-left">
                <div className="cfg-section">
                  <div className="cfg-sec-title" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    SDT Compression by Industry
                    <div style={{ display: "flex", gap: 8 }}>
                      <select className="cfg-sel" value={sdtTicks} onChange={(e) => setSdtTicks(Number(e.target.value))}>
                        {(sdtReport.available_ticks || [300]).map((t) => (
                          <option key={`ticks-${t}`} value={t}>{t} ticks</option>
                        ))}
                      </select>
                      <select className="cfg-sel" value={sdtMetric} onChange={(e) => setSdtMetric(e.target.value)}>
                        <option value="drop">Drop %</option>
                        <option value="kept">Kept %</option>
                      </select>
                      <button className="cfg-add-btn" onClick={loadSdtReport} disabled={sdtReport.loading}>
                        {sdtReport.loading ? "Refreshing..." : "Refresh"}
                      </button>
                    </div>
                  </div>
                  <div className="sdt-summary-grid">
                    {(sdtReport.summary || []).map((r) => {
                      const metricValue = Number(sdtMetric === "drop" ? (r.drop_pct || 0) : (r.kept_pct || 0));
                      const trendPoints = (sdtReport.trend_by_industry || {})[r.industry] || [];
                      const trendSeries = trendPoints.map((p) => Number(sdtMetric === "drop" ? (p.drop_pct || 0) : (p.kept_pct || 0)));
                      const trendTickLegend = trendPoints.map((p) => `${p.ticks}`).join(" · ");
                      return (
                        <div key={`sdt-${r.industry}`} className="sdt-card">
                          <div className="sdt-card-top">
                            <div className="sdt-card-industry">{r.industry}</div>
                            <div className="sdt-card-metric">{metricValue.toFixed(2)}%</div>
                          </div>
                          <div className="sdt-card-sub">{sdtMetric === "drop" ? "drop ratio" : "kept ratio"} ({sdtReport.ticks || sdtTicks} ticks)</div>
                          {trendSeries.length > 1 && (
                            <div className="sdt-trend-mini">
                              <TrendLine values={trendSeries} color={sdtMetric === "drop" ? "#FF6B57" : "#16A34A"} height={44} />
                              <div className="sdt-trend-legend">windows: {trendTickLegend} ticks</div>
                            </div>
                          )}
                          <div className="sdt-split-rail">
                            <div className="sdt-split-keep" style={{ width: `${Math.max(0, Math.min(100, Number(r.kept_pct || 0)))}%` }} />
                            <div className="sdt-split-drop" style={{ width: `${Math.max(0, Math.min(100, Number(r.drop_pct || 0)))}%` }} />
                          </div>
                          <div className="sdt-card-foot">
                            <span>Raw {Number(r.raw_total || 0).toLocaleString()}</span>
                            <span>SDT {Number(r.sdt_total || 0).toLocaleString()}</span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                  <div style={{ marginTop: 12, padding: 10, border: "1px solid var(--border)", borderRadius: 6, background: "#fff" }}>
                    <div style={{ fontSize: 11, color: "var(--muted)", textTransform: "uppercase", letterSpacing: 0.4, marginBottom: 6 }}>
                      Time-series window analysis ({sdtReport.ticks || sdtTicks} ticks)
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 8, fontSize: 12 }}>
                      <div><strong>Avg drop:</strong> {Number(sdtWindowInsights.avgDropPct || 0).toFixed(2)}%</div>
                      <div><strong>Avg kept:</strong> {Number(sdtWindowInsights.avgKeptPct || 0).toFixed(2)}%</div>
                      <div><strong>Best industry:</strong> {sdtWindowInsights.bestIndustry ? `${sdtWindowInsights.bestIndustry.industry} (${Number(sdtWindowInsights.bestIndustry.drop_pct || 0).toFixed(2)}% drop)` : "n/a"}</div>
                      <div><strong>Lowest drop:</strong> {sdtWindowInsights.worstIndustry ? `${sdtWindowInsights.worstIndustry.industry} (${Number(sdtWindowInsights.worstIndustry.drop_pct || 0).toFixed(2)}% drop)` : "n/a"}</div>
                      <div style={{ gridColumn: "1 / -1" }}>
                        <strong>Top compressed tag:</strong> {sdtWindowInsights.topDropTag ? `${sdtWindowInsights.topDropTag.tag_name} (${Number(sdtWindowInsights.topDropTag.drop_pct || 0).toFixed(2)}% drop)` : "n/a"}
                      </div>
                    </div>
                  </div>
                  {!!(sdtReport.industry_window_snapshots || []).length && (
                    <div className="sdt-window-snapshots">
                      <div className="sdt-window-title">{industry} multi-window snapshots</div>
                      <div className="sdt-window-list">
                        {(sdtReport.industry_window_snapshots || []).map((w) => (
                          <div key={`snap-${w.ticks}`} className="sdt-window-pill">
                            <span>{w.ticks} ticks</span>
                            <strong>{Number(sdtMetric === "drop" ? (w.drop_pct || 0) : (w.kept_pct || 0)).toFixed(2)}%</strong>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  <div style={{ marginTop: 10, color: "var(--muted)", fontSize: 11 }}>
                    Generated: {sdtReport.generated_at ? new Date(sdtReport.generated_at).toLocaleString() : "n/a"} · window: {sdtReport.ticks || sdtTicks} ticks
                  </div>
                </div>
              </div>
              <div className="cfg-right">
                <div className="cfg-section" style={{ height: "100%", display: "flex", flexDirection: "column" }}>
                  <div className="cfg-sec-title">Tag-level {sdtMetric === "drop" ? "Drop" : "Kept"} ({industry})</div>
                  <div className="mapping-hdr">
                    <span style={{ flex: 2 }}>Tag</span><span style={{ flex: 1 }}>Raw</span><span style={{ flex: 1 }}>SDT</span><span style={{ flex: 1 }}>{sdtMetric === "drop" ? "Drop %" : "Kept %"}</span><span style={{ flex: 2 }}>Visual</span>
                  </div>
                  <div className="mapping-list">
                    {!(sdtReport.tags || []).length ? (
                      <div style={{ color: "var(--muted)", fontSize: 12, padding: 12, textAlign: "center" }}>
                        No SDT benchmark data found. Run `python3 tools/sdt_compression_report.py`.
                      </div>
                    ) : (
                      sdtSortedTags.map((t, idx) => (
                        <div key={`sdt-tag-${t.tag_name}`} className="map-row">
                          <span className="map-rank">#{idx + 1}</span>
                          <span className="map-tag">{t.tag_name}</span>
                          <span className="map-val">{t.raw_count}</span>
                          <span className="map-val">{t.sdt_count}</span>
                          <span className="map-val">{(sdtMetric === "drop" ? (t.drop_pct || 0) : (t.kept_pct || 0)).toFixed(2)}%</span>
                          <span style={{ flex: 2 }}>
                            <span className="sdt-rail">
                              <span
                                className="sdt-rail-fill"
                                style={{
                                  width: `${Math.max(0, Math.min(100, sdtMetric === "drop" ? (t.drop_pct || 0) : (t.kept_pct || 0)))}%`,
                                  background: sdtMetric === "drop" ? "#FF6B57" : "#16A34A"
                                }}
                              />
                            </span>
                          </span>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className={`page ${page === "p7" ? "active" : ""}`} id="p7">
          <div className="p7-wrap">
            <div className="p7-left">
              <div className="exec-hero-card">
                <div className="exec-hero-eyebrow">Executive command center</div>
                <div className="exec-hero-title">{executive.value_statement || EMPTY_EXECUTIVE.value_statement}</div>
                <div className="exec-hero-sub">One-pane financial view for predictive maintenance value realization.</div>
              </div>

              <div className="exec-finance-row">
                <div className="exec-fin-card">
                  <div className="exec-fin-label">EBIT Saved</div>
                  <div className="exec-fin-val" title={execTips.ebit_saved || ""}>{executive.ebit_saved_fmt || "—"}</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">ROI</div>
                  <div className="exec-fin-val" title={execTips.roi_pct || ""}>{Number(executive.roi_pct || 0).toFixed(1)}%</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">Payback</div>
                  <div className="exec-fin-val" title={execTips.payback_days || ""}>{Number(executive.payback_days || 0).toFixed(1)} days</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">Margin Lift</div>
                  <div className="exec-fin-val" title={execTips.ebit_margin_bps || ""}>{Number(executive.ebit_margin_bps || 0).toFixed(1)} bps</div>
                </div>
              </div>

              <div className="p7-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">EBIT trend</div>
                  <TrendLine values={(executive.ebit_trend || []).map((p) => Number(p.value || 0))} color="#0F766E" height={140} />
                  <div className="exec-chip-row">
                    {(executive.ebit_trend || []).map((p) => (
                      <span key={`p7-trend-${p.label}`} className="exec-chip" title={execTips.ebit_saved || ""}>{p.label}: {p.value_fmt}</span>
                    ))}
                  </div>
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">Value bridge</div>
                  {(executive.value_bridge || []).map((b) => (
                    <div key={`p7-bridge-${b.label}`} className="exec-bridge-row">
                      <span className="exec-bridge-label">{b.label}</span>
                      <span className={`exec-bridge-val ${b.kind === "negative" ? "neg" : "pos"}`} title={execTips.ebit_saved || ""}>{b.amount_fmt}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="p7-right">
              <div className="agent-panel finance-panel">
                <div className="agent-hdr">
                  <div className="adot" />
                  <div>
                    <div className="atitle">Finance Scenario Genie</div>
                    <div className="asubt">Ask financial what-if scenarios in predictive maintenance context</div>
                  </div>
                </div>
                <div className="msgs">
                  {financeMsgs.map((m, i) => (
                    <div className="msg" key={`f-${m.role}-${i}`}>
                      <div className={`av ${m.role === "user" ? "user" : "agent"}`}>{m.role === "user" ? "ME" : "AI"}</div>
                      <div className={`bubble ${m.role === "user" ? "user" : ""}`}>
                        {m.role === "agent" && <div className="bubble-lbl">{m.label}</div>}
                        {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                      </div>
                    </div>
                  ))}
                  {financePending && (
                    <div className="msg">
                      <div className="av agent">AI</div>
                      <div className="bubble bubble-thinking">
                        <div className="bubble-lbl">Finance Command AI</div>
                        <div className="thinking-row">
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span>Thinking...</span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <div className="agent-inp">
                  <input
                    className="ainput"
                    value={financeInput}
                    onChange={(e) => setFinanceInput(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && sendFinanceMessage()}
                    placeholder={financePending ? "Processing scenario..." : "Ask: If we defer maintenance by 2 weeks, what is EBIT impact?"}
                    disabled={financePending}
                  />
                  <button className="sbtn" onClick={sendFinanceMessage} disabled={financePending}>{financePending ? "Processing..." : "Ask"}</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
