import { useEffect, useMemo, useRef, useState } from "react";
import GeoMap from "./components/GeoMap";
import PIDSchematic from "./components/PIDSchematic";
import WindSchematic from "./components/WindSchematic";
import AssetSidebar from "./components/AssetSidebar";
import GeoStatusBar from "./components/GeoStatusBar";
import GeoGeniePanel from "./components/GeoGeniePanel";
import { useGeoData, useAssetData } from "./hooks/useGeoData";

const INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"];
const CURRENCIES = ["AUTO", "USD", "AUD", "JPY"];
const EMPTY_EXECUTIVE = {
  audience: "finance_executive",
  window: "last_30_days",
  currency: "USD",
  value_statement: "Prescriptive maintenance unlocked EBIT upside of USD 0.",
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
  work_orders: [],
  executive_summary: {},
  forward_outlook: {},
  decision_cockpit: [],
  portfolio_insights: {}
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
  ["p7", "Finance", "¥"],
  ["p8", "Geo", "◌"]
];
const JA_INDUSTRY_LABELS = {
  mining: "鉱業",
  energy: "エネルギー",
  water: "水道",
  automotive: "自動車",
  semiconductor: "半導体"
};
const JA_UI = {
  Fleet: "フリート",
  Asset: "資産",
  "Hier.": "階層",
  Stream: "ストリーム",
  Model: "モデル",
  Sim: "シミュレーター",
  Finance: "財務",
  Geo: "地理",
  Currency: "通貨",
  Operator: "オペレーター",
  Executive: "経営",
  "Fleet Health": "フリート健全性",
  "Critical Assets": "重要資産",
  "Asset Count": "資産数",
  "Average health score": "平均健全性スコア",
  "Need immediate action": "即時対応が必要",
  "In monitored fleet": "監視対象フリート",
  "Live asset risk matrix": "ライブ資産リスクマトリクス",
  All: "全て",
  Critical: "重大",
  Warning: "警告",
  "Maintenance Supervisor AI": "保全スーパーバイザーAI",
  "Operational diagnosis and actions": "運用診断と推奨アクション",
  "Force critical": "重大を強制",
  "Forcing...": "強制中...",
  "Open Genie room": "Genieルームを開く",
  "Genie room unavailable": "Genieルーム未設定",
  "Genie rooms configured": "設定済みGenieルーム",
  "Approve": "承認",
  "Reject": "却下",
  "Defer": "延期",
  "Actioned": "対応済み",
  "Factory map": "工場マップ",
  "Hierarchy tree": "階層ツリー",
  "Physical layout map": "物理レイアウトマップ",
  "Investigate in Genie": "Genieで調査",
  "Map reset": "マップをリセット",
  "Map zoom in": "マップ拡大",
  "Map zoom out": "マップ縮小",
  "Ask AI": "AIに質問",
  "Ask about this location and incident context...": "この場所とインシデント状況について質問...",
  "Sending...": "送信中...",
  "Processing your request...": "リクエストを処理中...",
  "Ask about risk, RUL, and next action...": "リスク、RUL、次アクションを質問...",
  "Processing...": "処理中...",
  Send: "送信",
  "Recent alerts": "最新アラート",
  now: "現在",
  recent: "最近",
  "Industry Configuration": "業界設定",
  "Connector Setup": "コネクター設定",
  "SDT Benchmark": "SDTベンチマーク",
  "Tick interval": "ティック間隔",
  "Noise factor": "ノイズ係数",
  "Start simulator": "シミュレーター開始",
  Stop: "停止",
  Running: "実行中",
  Stopped: "停止中",
  "readings emitted": "件の読み取りを送信",
  "Live ingestion flow": "ライブ取り込みフロー",
  "3 stages: Bronze → Silver → Gold (5 recent rows each)": "3段階: Bronze → Silver → Gold（各5行）",
  Anomaly: "異常度",
  RUL: "残存寿命",
  Exposure: "影響額",
  "Executive command center": "経営コマンドセンター",
  "One-pane financial view for predictive maintenance value realization.": "予知保全の価値実現を1画面で確認できます。",
  "Finance Scenario Genie": "財務シナリオGenie",
  "Ask financial what-if scenarios in predictive maintenance context": "予知保全に関する財務のWhat-ifを質問",
  "Processing scenario...": "シナリオを処理中...",
  "Ask: If we defer maintenance by 2 weeks, what is EBIT impact?": "質問例: 保全を2週間延期した場合のEBIT影響は？",
  Ask: "質問",
  "EBIT trend": "EBIT推移",
  "Value bridge": "価値ブリッジ",
  "Forward outlook (30/90 days)": "将来見通し（30日/90日）",
  "30d protected EBIT (with actions)": "30日間の保護EBIT（推奨アクション実行時）",
  "30d protected EBIT (if deferred)": "30日間の保護EBIT（延期時）",
  "90d EBIT at risk if deferred": "延期時の90日EBITリスク",
  Confidence: "信頼度",
  "Decision cockpit": "意思決定コックピット",
  "Defer top actions by weeks": "上位アクションの延期週数",
  "Scenario at-risk EBIT": "シナリオ上のリスクEBIT",
  Disruption: "業務影響",
  "Portfolio concentration": "ポートフォリオ集中度",
  "Top 5 assets share of risk exposure": "上位5資産のリスク露出シェア",
  "Run-rate to annual target": "年次目標に対するランレート",
  "Back to Executive View": "経営ビューへ戻る",
  "EBIT Saved": "EBIT改善額",
  ROI: "ROI",
  Payback: "投資回収",
  "Margin Lift": "利益率改善",
  "EBIT Margin Lift": "EBITマージン改善",
  "Savings versus intervention + platform cost": "介入コスト＋プラットフォーム費用に対する効果",
  "Estimated time to recover investment": "投資回収までの推定期間",
  "Versus baseline monthly EBIT": "基準月次EBIT比",
  "Source table:": "参照テーブル:",
  "Thinking...": "考え中...",
  Sources: "参照ソース",
  ME: "私",
  AI: "AI",
  healthy: "正常",
  warning: "警告",
  critical: "重大",
  "Executive scenario outlook": "経営シナリオ見通し",
  "30d protected with actions": "30日保護EBIT（実行時）",
  "30d protected if deferred": "30日保護EBIT（延期時）",
  "Top decision actions": "優先意思決定アクション",
  "No decision actions available in current window.": "現在の期間で意思決定アクションはありません。",
  "Executive briefing metadata": "経営ブリーフィング情報",
  Industry: "業界",
  "Prepared at": "作成日時",
  "Source table": "参照テーブル",
  "Model trained": "モデル学習日時",
  "RUL accuracy (R²)": "RUL精度 (R²)",
  RMSE: "RMSE",
  "RUL degradation curve": "RUL劣化曲線",
  "Current RUL": "現在のRUL",
  "Feature importance — anomaly model": "特徴量重要度（異常モデル）",
  "Anomaly score decomposition": "異常スコア分解",
  "Executive briefing value statement": "経営向け価値サマリー",
  "Open Finance Command Center": "財務コマンドセンターを開く",
  "Export Board Briefing (PDF)": "経営ブリーフィングをPDF出力",
  "Board-ready financial operating view": "経営会議向けの財務オペレーティングビュー",
  "EBIT Protected (Quarter)": "保護されたEBIT（四半期）",
  "Variance (MoM / YoY)": "変化率（前月比 / 前年比）",
  "Annual run-rate": "年換算ランレート",
  "Annual target": "年次目標",
  "Financial impact by work order": "作業指示別の財務影響",
  "Value bridge to EBIT": "EBITへの価値ブリッジ",
  "ERP and work-order context": "ERP・作業指示コンテキスト",
  "EBIT impact trend (simulated)": "EBIT影響推移（シミュレーション）"
};

function localizeAlertText(text, isJapanese) {
  if (!isJapanese) return text;
  const immediate = String(text || "").match(/^(.+?) requires immediate intervention \((.+)\)$/);
  if (immediate) return `${immediate[1]} は即時対応が必要です（${immediate[2]}）`;
  const schedule = String(text || "").match(/^(.+?) should be scheduled this week \((.+)\)$/);
  if (schedule) return `${schedule[1]} は今週中の対応推奨です（${schedule[2]}）`;
  return text;
}

function localizeStatusText(status, isJapanese) {
  const s = String(status || "");
  if (!isJapanese) return s;
  return JA_UI[s] || s;
}

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
    const contentType = String(res.headers.get("content-type") || "");
    const isJson = contentType.includes("application/json");
    const payload = isJson ? await res.json() : await res.text();
    if (!res.ok) {
      const detail =
        (payload && typeof payload === "object" && (payload.detail || payload.message || payload.error)) ||
        (typeof payload === "string" ? payload : `HTTP ${res.status}`);
      return { ...(fallback || {}), ok: false, status: res.status, detail: String(detail || "Request failed") };
    }
    return payload;
  } catch {
    return fallback;
  }
}

function toBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const out = String(reader.result || "");
      const idx = out.indexOf(",");
      resolve(idx >= 0 ? out.slice(idx + 1) : out);
    };
    reader.onerror = () => reject(new Error("Failed to read file."));
    reader.readAsDataURL(file);
  });
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

function hashSeed(text) {
  const s = String(text || "");
  let out = 0;
  for (let i = 0; i < s.length; i += 1) out = ((out << 5) - out + s.charCodeAt(i)) | 0;
  return Math.abs(out);
}

function zoneRects(industry) {
  if (industry === "water") return [{ x: 16, y: 20, w: 30, h: 20 }, { x: 56, y: 20, w: 30, h: 20 }, { x: 16, y: 56, w: 30, h: 24 }, { x: 56, y: 56, w: 30, h: 24 }];
  if (industry === "mining") return [{ x: 12, y: 16, w: 36, h: 24 }, { x: 52, y: 16, w: 34, h: 24 }, { x: 12, y: 52, w: 36, h: 30 }, { x: 52, y: 52, w: 34, h: 30 }];
  if (industry === "automotive") return [{ x: 10, y: 18, w: 38, h: 26 }, { x: 52, y: 18, w: 36, h: 26 }, { x: 10, y: 52, w: 38, h: 28 }, { x: 52, y: 52, w: 36, h: 28 }];
  if (industry === "semiconductor") return [{ x: 12, y: 20, w: 34, h: 24 }, { x: 54, y: 20, w: 34, h: 24 }, { x: 12, y: 54, w: 34, h: 24 }, { x: 54, y: 54, w: 34, h: 24 }];
  return [{ x: 14, y: 18, w: 34, h: 24 }, { x: 52, y: 18, w: 34, h: 24 }, { x: 14, y: 54, w: 34, h: 24 }, { x: 52, y: 54, w: 34, h: 24 }];
}

function buildMapPinLayout(assets, industry) {
  const list = Array.isArray(assets) ? assets : [];
  if (!list.length) return { pins: [], zoneChips: [] };
  const byZone = new Map();
  list.forEach((a) => {
    const key = `${a.site || "site"} · ${a.area || "area"} · ${a.unit || "unit"}`;
    if (!byZone.has(key)) byZone.set(key, []);
    byZone.get(key).push(a);
  });
  const zones = Array.from(byZone.entries()).sort((a, b) => b[1].length - a[1].length);
  const rects = zoneRects(industry);
  const pins = [];
  const zoneChips = zones.slice(0, 4).map(([name, rows], i) => ({ name, count: rows.length, slot: i }));
  zones.forEach(([name, rows], zoneIdx) => {
    const rect = rects[zoneIdx % rects.length];
    const cx = rect.x + rect.w / 2;
    const cy = rect.y + rect.h / 2;
    rows.forEach((a, idx) => {
      const angle = (idx * 137.5 * Math.PI) / 180;
      const norm = Math.sqrt((idx + 0.5) / Math.max(1, rows.length));
      const seed = hashSeed(`${a.id}:${name}`);
      const jitter = ((seed % 7) - 3) * 0.25;
      const rx = rect.w * 0.38;
      const ry = rect.h * 0.4;
      const x = Math.max(6, Math.min(94, cx + Math.cos(angle) * norm * rx + jitter));
      const y = Math.max(8, Math.min(92, cy + Math.sin(angle) * norm * ry + jitter));
      pins.push({
        id: a.id,
        status: a.status,
        health: a.health_score_pct,
        anomaly: a.anomaly_score,
        type: a.type,
        zone: name,
        minX: rect.x + 2,
        maxX: rect.x + rect.w - 2,
        minY: rect.y + 2,
        maxY: rect.y + rect.h - 2,
        x,
        y
      });
    });
  });
  // Relax pin positions to avoid overlaps while staying inside zone bounds.
  const minDist = 5.2;
  for (let iter = 0; iter < 60; iter += 1) {
    for (let i = 0; i < pins.length; i += 1) {
      for (let j = i + 1; j < pins.length; j += 1) {
        const a = pins[i];
        const b = pins[j];
        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const d2 = (dx * dx) + (dy * dy);
        if (d2 <= 0.0001) {
          const nudge = ((hashSeed(`${a.id}:${b.id}:${iter}`) % 5) - 2) * 0.14;
          a.x = Math.max(a.minX, Math.min(a.maxX, a.x - nudge));
          b.x = Math.max(b.minX, Math.min(b.maxX, b.x + nudge));
          continue;
        }
        const d = Math.sqrt(d2);
        if (d >= minDist) continue;
        const push = (minDist - d) * 0.46;
        const ux = dx / d;
        const uy = dy / d;
        a.x = Math.max(a.minX, Math.min(a.maxX, a.x - (ux * push)));
        a.y = Math.max(a.minY, Math.min(a.maxY, a.y - (uy * push)));
        b.x = Math.max(b.minX, Math.min(b.maxX, b.x + (ux * push)));
        b.y = Math.max(b.minY, Math.min(b.maxY, b.y + (uy * push)));
      }
    }
  }
  const normalizedPins = pins.map((p) => ({
    id: p.id,
    status: p.status,
    health: p.health,
    anomaly: p.anomaly,
    type: p.type,
    zone: p.zone,
    x: p.x,
    y: p.y
  }));
  return { pins: normalizedPins, zoneChips };
}

function toEpochMs(ts) {
  if (!ts) return 0;
  const ms = Date.parse(String(ts));
  return Number.isFinite(ms) ? ms : 0;
}

function prioritizeFlowRows(rows, injectedAssets, startedAt) {
  const input = Array.isArray(rows) ? rows : [];
  if (!input.length) return [];
  const startedAtMs = toEpochMs(startedAt);
  const injectedSet = new Set((injectedAssets || []).map((x) => String(x || "")));
  const ranked = input.map((r, idx) => {
    const equipmentId = String(r?.equipment_id || "");
    const isInjectedAsset = injectedSet.has(equipmentId);
    const tsMs = toEpochMs(r?.timestamp);
    const isFresh = startedAtMs > 0 ? tsMs >= (startedAtMs - 120000) : false;
    return {
      ...r,
      _isInjectedTop: isInjectedAsset && (isFresh || startedAtMs <= 0),
      _tsMs: tsMs,
      _idx: idx
    };
  });
  ranked.sort((a, b) => {
    if (a._isInjectedTop !== b._isInjectedTop) return a._isInjectedTop ? -1 : 1;
    if (a._tsMs !== b._tsMs) return b._tsMs - a._tsMs;
    return a._idx - b._idx;
  });
  return ranked;
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
  const mainScrollRef = useRef(null);
  const mapDragRef = useRef(null);
  const [industry, setIndustry] = useState("mining");
  const [demoCurrency, setDemoCurrency] = useState("AUTO");
  const [page, setPage] = useState("p1");
  const [view, setView] = useState("operator");
  const [simTab, setSimTab] = useState("sim");
  const [assetSeverityFilter, setAssetSeverityFilter] = useState("all");

  const [overview, setOverview] = useState(EMPTY_OVERVIEW);
  const [recActionPending, setRecActionPending] = useState({});
  const [recCommentByAsset, setRecCommentByAsset] = useState({});
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [assetDetail, setAssetDetail] = useState(null);
  const [hierarchy, setHierarchy] = useState(null);
  const [hierSelection, setHierSelection] = useState(null);
  const [hierViewMode, setHierViewMode] = useState("map");
  const [hierPanelWidth, setHierPanelWidth] = useState(520);
  const [hierResizing, setHierResizing] = useState(false);
  const [mapView, setMapView] = useState({ scale: 1, x: 0, y: 0 });
  const [model, setModel] = useState(null);
  const [advancedPdm, setAdvancedPdm] = useState(null);
  const [manualInput, setManualInput] = useState("");
  const [manualParse, setManualParse] = useState(null);
  const [manualParsePending, setManualParsePending] = useState(false);
  const [manualFile, setManualFile] = useState(null);
  const [manualUploadPending, setManualUploadPending] = useState(false);
  const [simScoringPending, setSimScoringPending] = useState(false);
  const [forceCriticalPendingByAsset, setForceCriticalPendingByAsset] = useState({});
  const [simScoringRunId, setSimScoringRunId] = useState("");
  const [simPipeline, setSimPipeline] = useState({
    active: false,
    phase: "",
    runId: "",
    runUrl: "",
    runStatus: "",
    runResult: "",
    rowsEmitted: 0,
    enabledAssets: [],
    baseline: null,
    startedAt: "",
    completedAt: "",
    error: ""
  });

  const [streamRows, setStreamRows] = useState([]);
  const [streamCount, setStreamCount] = useState(0);
  const [streamFilters, setStreamFilters] = useState({ asset: "", quality: "", proto: "" });

  const [agentInput, setAgentInput] = useState("");
  const [agentMsgs, setAgentMsgs] = useState([]);
  const [genieConversationByIndustry, setGenieConversationByIndustry] = useState({});
  const [mapCopilotInput, setMapCopilotInput] = useState("");
  const [mapCopilotPending, setMapCopilotPending] = useState(false);
  const [mapCopilotMsgsByKey, setMapCopilotMsgsByKey] = useState({});
  const [mapCopilotConversationByKey, setMapCopilotConversationByKey] = useState({});
  const [hierGenieOpen, setHierGenieOpen] = useState(false);
  const [hierGenieInput, setHierGenieInput] = useState("");
  const [hierGeniePending, setHierGeniePending] = useState(false);
  const [hierGenieMsgsByKey, setHierGenieMsgsByKey] = useState({});
  const [hierGenieConversationByKey, setHierGenieConversationByKey] = useState({});
  const [hierGenieTargetAssetId, setHierGenieTargetAssetId] = useState("");
  const [genieRooms, setGenieRooms] = useState({ industry: "", workspace_url: "", configured_count: 0, total_count: 5, missing: [], rooms: {} });
  const [agentPending, setAgentPending] = useState(false);
  const [financeInput, setFinanceInput] = useState("");
  const [financeMsgs, setFinanceMsgs] = useState([]);
  const [financeConversationByIndustry, setFinanceConversationByIndustry] = useState({});
  const [financePending, setFinancePending] = useState(false);
  const [execDelayWeeks, setExecDelayWeeks] = useState(0);
  const [liveClock, setLiveClock] = useState(() => new Date().toLocaleString());
  const [geoView, setGeoView] = useState("geo");
  const [activeSiteId, setActiveSiteId] = useState(null);
  const [activeAssetId, setActiveAssetId] = useState(null);
  const [mapLayer, setMapLayer] = useState("terrain");
  const [visibleIndustries, setVisibleIndustries] = useState(new Set(INDUSTRIES));

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
  const { sites: geoSites, loading: geoLoading, error: geoError, refetch: refetchGeoSites } = useGeoData(visibleIndustries, demoCurrency);
  const { assets: geoAssets, schematic: geoSchematic, loading: geoAssetLoading } = useAssetData(activeSiteId, demoCurrency);

  useEffect(() => {
    // Prevent stale cross-industry asset IDs from triggering 404 API calls
    // while the next industry's overview payload is loading.
    setSelectedAssetId("");
    setAssetDetail(null);
    setModel(null);
    setAdvancedPdm(null);
    setMapView({ scale: 1, x: 0, y: 0 });
    setHierPanelWidth(520);
    setGeoView("geo");
    setActiveSiteId(null);
    setActiveAssetId(null);
  }, [industry]);

  useEffect(() => {
    if (!hierResizing) return undefined;
    const onMove = (e) => {
      const minW = 360;
      const maxW = Math.min(window.innerWidth - 420, 1200);
      setHierPanelWidth(Math.max(minW, Math.min(maxW, e.clientX - 92)));
    };
    const onUp = () => setHierResizing(false);
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, [hierResizing]);

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
      setSimState((prev) => {
        const next = sim || {};
        // Avoid a late initial fetch flipping the simulator to stopped
        // if the user already started it.
        return {
          ...prev,
          ...next,
          running: Boolean(prev?.running || next?.running)
        };
      });
      setAgentMsgs((ov.messages || []).map((m) => ({ role: m.role, text: m.text, label: m.label || "AI" })));
      setFinanceMsgs([{
        role: "agent",
        label: isJapanese ? "財務コマンドAI" : "Finance Command AI",
        text: isJapanese
          ? `${industryLabel(industry)}向けに、${(ov.executive || {}).currency || demoCurrency}で予知保全の財務シナリオに回答できます。`
          : `I can answer financial predictive maintenance scenarios for ${industry} in ${(ov.executive || {}).currency || demoCurrency}.`
      }]);
      const defaultAsset = ov.assets?.[0]?.id || "";
      setSelectedAssetId(defaultAsset);
      const template = await getJson(`/api/ui/config/template?industry=${industry}`, null);
      const roomCfg = await getJson(`/api/ui/genie/rooms?industry=${industry}`, null);
      if (!cancelled && template) {
        setCfg(template);
        const tc = template.connector || {};
        const targetFqn = [tc.target_catalog || template.catalog || `pdm_${industry}`, tc.target_schema || "bronze", tc.target_table || "pravin_zerobus"].join(".");
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
      if (!cancelled && roomCfg) setGenieRooms(roomCfg);
    })();
    return () => {
      cancelled = true;
    };
  }, [industry, currencyParam]);

  useEffect(() => {
    if (!selectedAssetId) return;
    let cancelled = false;
    (async () => {
      const [detail, modelData, advData] = await Promise.all([
        getJson(`/api/ui/asset/${selectedAssetId}?industry=${industry}${currencyParam}`, null),
        getJson(`/api/ui/model/${selectedAssetId}?industry=${industry}${currencyParam}`, null),
        getJson(`/api/ui/advanced_pdm?asset_id=${encodeURIComponent(selectedAssetId)}&industry=${industry}${currencyParam}`, null)
      ]);
      if (cancelled) return;
      setAssetDetail(detail);
      setModel(modelData);
      setAdvancedPdm(advData);
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
        target_table: parsedTarget.table || "pravin_zerobus",
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

  useEffect(() => {
    const rid = simPipeline.runId;
    if (!rid || !simPipeline.active) return undefined;
    let alive = true;
    const poll = async () => {
      const st = await getJson(`/api/ui/scoring/status?run_id=${encodeURIComponent(rid)}`, { ok: false });
      if (!alive || !st?.ok) return;
      const life = String(st.life_cycle_state || "");
      const result = String(st.result_state || "");
      setSimPipeline((prev) => ({
        ...prev,
        runStatus: life || prev.runStatus,
        runResult: result || prev.runResult,
        runUrl: st.run_page_url || prev.runUrl,
        phase:
          life === "TERMINATED"
            ? (result === "SUCCESS" ? "Scoring completed successfully." : "Scoring completed with failure.")
            : "Scoring job running...",
        active: life !== "TERMINATED",
        completedAt: life === "TERMINATED" ? new Date().toISOString() : prev.completedAt
      }));
    };
    poll();
    const timer = setInterval(poll, 3000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [simPipeline.runId, simPipeline.active]);

  useEffect(() => {
    const timer = setInterval(() => setLiveClock(new Date().toLocaleString()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!activeSiteId) return;
    const exists = (geoSites || []).some((s) => s.site_id === activeSiteId);
    if (!exists) {
      setActiveSiteId(null);
      setActiveAssetId(null);
      setGeoView("geo");
    }
  }, [geoSites, activeSiteId]);

  const selectedAsset = useMemo(
    () => overview.assets.find((a) => a.id === selectedAssetId) || overview.assets[0] || null,
    [overview.assets, selectedAssetId]
  );

  const filteredAssets = useMemo(() => {
    const assets = overview.assets || [];
    if (assetSeverityFilter === "critical") return assets.filter((a) => a.status === "critical");
    if (assetSeverityFilter === "warning") return assets.filter((a) => a.status === "warning");
    return assets;
  }, [overview.assets, assetSeverityFilter]);
  const mapLayout = useMemo(() => buildMapPinLayout(overview.assets || [], industry), [overview.assets, industry]);
  const mapPins = mapLayout.pins;
  const mapZoneChips = mapLayout.zoneChips;
  const mapCopilotKey = `${industry}::${selectedAssetId || ""}`;
  const mapCopilotMsgs = mapCopilotMsgsByKey[mapCopilotKey] || [];
  const hierGenieAssetId = String(hierGenieTargetAssetId || hierSelection?.asset_id || selectedAssetId || "");
  const hierGenieKey = `${industry}::${hierGenieAssetId}`;
  const hierGenieMsgs = hierGenieMsgsByKey[hierGenieKey] || [];
  const hierGenieAssetOptions = useMemo(() => {
    const pinIds = (mapPins || []).map((p) => String(p.id || "")).filter(Boolean);
    const overviewIds = (overview.assets || []).map((a) => String(a.id || "")).filter(Boolean);
    return Array.from(new Set([...pinIds, ...overviewIds]));
  }, [mapPins, overview.assets]);
  const activeGenieRoom = genieRooms?.rooms?.[industry] || {};
  const activeGenieUrl = String(activeGenieRoom?.url || "");
  const activeGeoSite = useMemo(
    () => (geoSites || []).find((s) => s.site_id === activeSiteId) || null,
    [geoSites, activeSiteId]
  );
  const activeGeoIndustry = String(activeGeoSite?.industry || industry || "").toLowerCase();
  const activeGeoGenieUrl = String((genieRooms?.rooms?.[activeGeoIndustry] || {}).url || "");
  const activeGeoAsset = useMemo(
    () => (geoAssets || []).find((a) => a.asset_id === activeAssetId) || null,
    [geoAssets, activeAssetId]
  );
  useEffect(() => {
    if (!activeSiteId) return;
    if (activeAssetId) return;
    if (!geoAssets.length) return;
    setActiveAssetId(String(geoAssets[0].asset_id || ""));
  }, [activeSiteId, activeAssetId, geoAssets]);

  const simFlowDelta = useMemo(() => {
    const b = simPipeline.baseline;
    if (!b) return { bronze: 0, silver: 0, gold: 0 };
    return {
      bronze: Number(simFlow?.bronze?.rows_5m || 0) - Number(b.bronze || 0),
      silver: Number(simFlow?.silver?.rows_5m || 0) - Number(b.silver || 0),
      gold: Number(simFlow?.gold?.rows_5m || 0) - Number(b.gold || 0)
    };
  }, [simPipeline.baseline, simFlow?.bronze?.rows_5m, simFlow?.silver?.rows_5m, simFlow?.gold?.rows_5m]);

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
  const effectiveUiCurrency = demoCurrency === "AUTO" ? (executive.currency || "USD") : demoCurrency;
  const isJapanese = effectiveUiCurrency === "JPY";
  const t = (text) => (isJapanese ? (JA_UI[text] || text) : text);
  const dayUnit = isJapanese ? "日" : "days";
  const shortDayUnit = isJapanese ? "日" : "d";
  const industryLabel = (ind) => (isJapanese ? (JA_INDUSTRY_LABELS[ind] || ind) : (ind.charAt(0).toUpperCase() + ind.slice(1)));
  const execScenario = useMemo(() => {
    const decisions = executive.decision_cockpit || [];
    const baseRisk = decisions.reduce((acc, d) => acc + Number(d.value_uplift || 0), 0);
    const delayMultiplier = 1 + (Number(execDelayWeeks || 0) * 0.12);
    const atRisk = baseRisk * delayMultiplier;
    const atRiskFmt = new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: effectiveUiCurrency || "USD",
      maximumFractionDigits: 0
    }).format(atRisk || 0);
    return {
      atRisk,
      atRiskFmt,
      withAction30Fmt: executive.forward_outlook?.horizon_30_days?.protected_with_actions_fmt || executive.ebit_saved_fmt || "—"
    };
  }, [executive, execDelayWeeks, effectiveUiCurrency]);
  const briefingStamp = useMemo(
    () => new Date().toLocaleString(),
    [industry, effectiveUiCurrency, executive.ebit_saved_fmt, executive.roi_pct, executive.payback_days]
  );

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

  async function sendMessage(overrideText = "", overrideAssetId = "") {
    const userText = String(overrideText || agentInput || "").trim();
    if (!userText || agentPending) return;
    setAgentMsgs((prev) => [...prev, { role: "user", text: userText, label: "ME" }]);
    if (!overrideText) setAgentInput("");
    setAgentPending(true);
    try {
      const reply = await postJson(
        "/api/agent/chat",
        {
          industry,
          currency: demoCurrency === "AUTO" ? executive.currency : demoCurrency,
          asset_id: String(overrideAssetId || selectedAssetId || ""),
          conversation_id: genieConversationByIndustry[industry] || "",
          messages: [{ role: "user", content: userText }]
        },
        { choices: [{ message: { content: "Unable to get response." } }] }
      );
      if (reply?.conversation_id) {
        setGenieConversationByIndustry((prev) => ({ ...prev, [industry]: reply.conversation_id }));
      }
      const answer = reply?.choices?.[0]?.message?.content || "No response.";
      const refs = Array.isArray(reply?.references) ? reply.references : [];
      setAgentMsgs((prev) => [...prev, { role: "agent", text: answer, label: t("Maintenance Supervisor AI"), references: refs }]);
    } finally {
      setAgentPending(false);
    }
  }

  async function sendHierarchyGenieMessage(overrideText = "") {
    const assetId = String(hierSelection?.asset_id || selectedAssetId || "").trim();
    const userText = String(overrideText || hierGenieInput || "").trim();
    if (!assetId || !userText || hierGeniePending) return;
    const k = `${industry}::${assetId}`;
    if (!overrideText) setHierGenieInput("");
    setHierGeniePending(true);
    setHierGenieMsgsByKey((prev) => ({
      ...prev,
      [k]: [...(prev[k] || []), { role: "user", text: userText, label: "ME" }]
    }));
    try {
      const context = `Hierarchy context: industry=${industry}, asset_id=${assetId}. Focus root cause, risk, and action sequence for this equipment.`;
      const reply = await postJson(
        "/api/agent/chat",
        {
          industry,
          currency: demoCurrency === "AUTO" ? executive.currency : demoCurrency,
          asset_id: assetId,
          conversation_id: hierGenieConversationByKey[k] || "",
          messages: [{ role: "user", content: `${userText}\n\n${context}` }]
        },
        { choices: [{ message: { content: "Unable to get response." } }] }
      );
      if (reply?.conversation_id) {
        setHierGenieConversationByKey((prev) => ({ ...prev, [k]: reply.conversation_id }));
      }
      const answer = reply?.choices?.[0]?.message?.content || "No response.";
      const refs = Array.isArray(reply?.references) ? reply.references : [];
      setHierGenieMsgsByKey((prev) => ({
        ...prev,
        [k]: [...(prev[k] || []), { role: "agent", text: answer, label: t("Maintenance Supervisor AI"), references: refs }]
      }));
    } finally {
      setHierGeniePending(false);
    }
  }

  async function investigateHierarchyInGenie(assetId) {
    const targetAsset = String(assetId || "").trim();
    if (!targetAsset) return;
    setSelectedAssetId(targetAsset);
    setHierGenieTargetAssetId(targetAsset);
    setHierGenieOpen(true);
    const question = `Investigate asset ${targetAsset} for root cause, current risk, and recommended action sequence.`;
    await sendHierarchyGenieMessage(question);
  }

  async function sendMapCopilotMessage() {
    const assetId = String(selectedAssetId || "").trim();
    if (!assetId || !mapCopilotInput.trim() || mapCopilotPending) return;
    const k = `${industry}::${assetId}`;
    const userText = mapCopilotInput.trim();
    setMapCopilotInput("");
    setMapCopilotPending(true);
    setMapCopilotMsgsByKey((prev) => ({
      ...prev,
      [k]: [...(prev[k] || []), { role: "user", text: userText, label: "ME" }]
    }));
    try {
      const context = `Map context: industry=${industry}, asset_id=${assetId}. Please focus this equipment and nearby zone impact.`;
      const reply = await postJson(
        "/api/agent/chat",
        {
          industry,
          conversation_id: mapCopilotConversationByKey[k] || "",
          messages: [{ role: "user", content: `${userText}\n\n${context}` }]
        },
        { choices: [{ message: { content: "Unable to get response." } }] }
      );
      if (reply?.conversation_id) {
        setMapCopilotConversationByKey((prev) => ({ ...prev, [k]: reply.conversation_id }));
      }
      const answer = reply?.choices?.[0]?.message?.content || "No response.";
      const refs = Array.isArray(reply?.references) ? reply.references : [];
      setMapCopilotMsgsByKey((prev) => ({
        ...prev,
        [k]: [...(prev[k] || []), { role: "agent", text: answer, label: t("Maintenance Supervisor AI"), references: refs }]
      }));
    } finally {
      setMapCopilotPending(false);
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

  async function triggerSimScoring() {
    if (simScoringPending) return;
    const baseline = {
      bronze: Number(simFlow?.bronze?.rows_5m || 0),
      silver: Number(simFlow?.silver?.rows_5m || 0),
      gold: Number(simFlow?.gold?.rows_5m || 0)
    };
    setSimPipeline({
      active: true,
      phase: "Injecting faults and emitting simulator ticks...",
      runId: "",
      runUrl: "",
      runStatus: "PENDING",
      runResult: "",
      rowsEmitted: 0,
      enabledAssets: [],
      baseline,
      startedAt: new Date().toISOString(),
      completedAt: "",
      error: ""
    });
    setSimScoringPending(true);
    try {
      const res = await postJson(
        "/api/ui/simulator/inject_and_score",
        { industry, ticks: 12, wait_seconds: 0, keep_running: true, non_blocking: true },
        { ok: false }
      );
      const rid = String(res?.run_id || res?.score?.run_id || "");
      if (res?.ok && rid) {
        setSimScoringRunId(rid);
        setSimPipeline((prev) => ({
          ...prev,
          phase: "Scoring run submitted (non-blocking). Updates will stream in.",
          runId: rid,
          runStatus: "RUNNING",
          rowsEmitted: Number(res?.rows_emitted || 0),
          enabledAssets: Array.isArray(res?.enabled_fault_assets) ? res.enabled_fault_assets : []
        }));
      } else if (res?.ok && Number(res?.rows_emitted || 0) > 0) {
        setSimPipeline((prev) => ({
          ...prev,
          active: false,
          phase: "Rows emitted. Processing continues asynchronously (non-blocking).",
          runStatus: "ACCEPTED",
          rowsEmitted: Number(res?.rows_emitted || 0),
          enabledAssets: Array.isArray(res?.enabled_fault_assets) ? res.enabled_fault_assets : []
        }));
      } else {
        const scoreError = String(res?.score?.error || res?.detail || "").trim();
        const flowHint = Number(res?.rows_emitted || 0) <= 0 ? " No simulator rows were emitted; check asset metadata/sensor mappings." : "";
        setSimPipeline((prev) => ({
          ...prev,
          active: false,
          runStatus: "FAILED",
          rowsEmitted: Number(res?.rows_emitted || 0),
          enabledAssets: Array.isArray(res?.enabled_fault_assets) ? res.enabled_fault_assets : [],
          error: scoreError ? `${scoreError}${flowHint}` : `Unable to trigger end-to-end scoring run.${flowHint}`,
          completedAt: new Date().toISOString()
        }));
      }
    } catch {
      setSimPipeline((prev) => ({
        ...prev,
        active: false,
        runStatus: "FAILED",
        error: "Unable to trigger end-to-end scoring run.",
        completedAt: new Date().toISOString()
      }));
    } finally {
      setSimScoringPending(false);
    }
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
      const refs = Array.isArray(reply?.references) ? reply.references : [];
      setFinanceMsgs((prev) => [...prev, { role: "agent", text: answer, label: isJapanese ? "財務コマンドAI" : "Finance Command AI", references: refs }]);
    } finally {
      setFinancePending(false);
    }
  }

  async function parseManualText() {
    if (!manualInput.trim() || manualParsePending) return;
    setManualParsePending(true);
    try {
      const res = await postJson(
        "/api/ui/manuals/parse",
        { industry, text: manualInput, asset_id: selectedAssetId },
        { ok: false, message: "Unable to parse manual text." }
      );
      setManualParse(res);
    } finally {
      setManualParsePending(false);
    }
  }

  async function uploadManualFile() {
    if (!manualFile || manualUploadPending) return;
    setManualUploadPending(true);
    try {
      const contentBase64 = await toBase64(manualFile);
      const res = await postJson(
        "/api/ui/manuals/upload",
        { industry, filename: manualFile.name, content_base64: contentBase64 },
        { ok: false, message: "Manual upload failed." }
      );
      if (res?.ok) {
        setManualParse(res);
        setManualInput("");
        setManualFile(null);
      } else {
        setManualParse(res);
      }
    } finally {
      setManualUploadPending(false);
    }
  }

  function exportExecutiveBriefing() {
    setPage("p1");
    setView("executive");
    setTimeout(() => {
      if (typeof window !== "undefined" && typeof window.print === "function") {
        window.print();
      }
    }, 120);
  }

  async function updateFault(assetId, patch) {
    const result = await postJson("/api/ui/simulator/fault", { industry, asset_id: assetId, ...patch }, { faults: simState.faults || {} });
    setSimState((prev) => ({ ...prev, faults: result.faults || prev.faults }));
  }

  function mapZoom(delta) {
    setMapView((prev) => {
      const nextScale = Math.max(0.7, Math.min(2.4, prev.scale + delta));
      return { ...prev, scale: nextScale };
    });
  }

  function mapReset() {
    setMapView({ scale: 1, x: 0, y: 0 });
  }

  function startMapDrag(e) {
    if (hierViewMode !== "map") return;
    mapDragRef.current = { sx: e.clientX, sy: e.clientY, x: mapView.x, y: mapView.y };
  }

  function moveMapDrag(e) {
    if (!mapDragRef.current) return;
    const d = mapDragRef.current;
    setMapView((prev) => ({ ...prev, x: d.x + (e.clientX - d.sx), y: d.y + (e.clientY - d.sy) }));
  }

  function endMapDrag() {
    mapDragRef.current = null;
  }

  function onMapWheel(e) {
    e.preventDefault();
    mapZoom(e.deltaY < 0 ? 0.08 : -0.08);
  }

  async function forceCritical(assetId) {
    if (!assetId || forceCriticalPendingByAsset[assetId]) return;
    setForceCriticalPendingByAsset((prev) => ({ ...prev, [assetId]: true }));
    try {
      const res = await postJson(
        "/api/ui/simulator/force_critical",
        { industry, asset_id: assetId, anomaly_score: 0.95, rul_hours: 6 },
        { ok: false }
      );
      if (!res?.ok) {
        const detail = String(res?.detail || "Unable to force critical prediction.");
        setSimPipeline((prev) => ({
          ...prev,
          active: false,
          runStatus: "FAILED",
          error: detail,
          completedAt: new Date().toISOString()
        }));
        return;
      }

      if (res?.faults) {
        setSimState((prev) => ({ ...prev, faults: res.faults || prev.faults }));
      }
      setSelectedAssetId(assetId);
      const [ov, flow, detail, modelData, advData] = await Promise.all([
        getJson(`/api/ui/overview?industry=${industry}${currencyParam}`, EMPTY_OVERVIEW),
        getJson(`/api/ui/simulator/flow?industry=${industry}&limit=24`, simFlow),
        getJson(`/api/ui/asset/${encodeURIComponent(assetId)}?industry=${industry}${currencyParam}`, assetDetail),
        getJson(`/api/ui/model/${encodeURIComponent(assetId)}?industry=${industry}${currencyParam}`, model),
        getJson(`/api/ui/advanced_pdm?asset_id=${encodeURIComponent(assetId)}&industry=${industry}${currencyParam}`, advancedPdm)
      ]);
      setOverview(ov || EMPTY_OVERVIEW);
      setSimFlow(flow || simFlow);
      if (detail) setAssetDetail(detail);
      if (modelData) setModel(modelData);
      if (advData) setAdvancedPdm(advData);
      setSimPipeline((prev) => ({
        ...prev,
        active: false,
        phase: `Forced critical alert for ${assetId}.`,
        runStatus: "FORCED",
        runResult: "SUCCESS",
        rowsEmitted: Math.max(1, Number(prev?.rowsEmitted || 0)),
        enabledAssets: Array.from(new Set([...(prev?.enabledAssets || []), assetId])),
        error: "",
        completedAt: new Date().toISOString()
      }));
    } finally {
      setForceCriticalPendingByAsset((prev) => ({ ...prev, [assetId]: false }));
    }
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
      table: "pravin_zerobus"
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
      const fqn = [c?.target?.catalog || cfg.catalog || `pdm_${industry}`, c?.target?.schema || "bronze", c?.target?.table || "pravin_zerobus"].join(".");
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

  function toggleGeoIndustry(ind) {
    setVisibleIndustries((prev) => {
      const next = new Set(prev);
      if (next.has(ind)) next.delete(ind);
      else next.add(ind);
      if (!next.size) INDUSTRIES.forEach((v) => next.add(v));
      return next;
    });
  }

  function onGeoSiteClick(siteId) {
    setActiveSiteId(siteId);
    setActiveAssetId(null);
    setGeoView("facility");
  }

  return (
    <>
      <header className="topbar">
        <div className="logo">
          <svg className="dbx-mark" viewBox="0 0 64 64" fill="none" aria-hidden="true">
            <polygon points="8,18 32,6 56,18 48,22 32,14 16,22" fill="#FF3621" />
            <polygon points="8,32 32,20 56,32 48,36 32,28 16,36" fill="#FF3621" />
            <polygon points="8,46 32,34 56,46 32,58" fill="#FF3621" />
          </svg>
          <span className="logo-text">Databricks</span>
        </div>
        <div className="topbar-div" />
        <span className="app-name">{isJapanese ? "予知保全 オペレーション＆ビジネス価値ハブ" : "Predictive & Prescriptive Maintenance Hub"}</span>
        <div className="ind-tabs">
          {INDUSTRIES.map((ind) => (
            <button key={ind} className={`itab ${industry === ind ? "active" : ""}`} onClick={() => setIndustry(ind)}>
              {industryLabel(ind)}
            </button>
          ))}
        </div>
        <div className="currency-wrap">
          <span className="currency-lbl">{t("Currency")}</span>
          <select className="currency-sel" value={demoCurrency} onChange={(e) => setDemoCurrency(e.target.value)}>
            {CURRENCIES.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <span className="isa-badge">{isJapanese ? `ライブ ・ ${liveClock}` : `Live · ${liveClock}`}</span>
      </header>

      <div className="body-wrap">
        <nav className="sidebar">
          {PAGE_META.map(([pid, label, icon]) => (
            <button key={pid} className={`nav-btn ${page === pid ? "active" : ""}`} onClick={() => setPage(pid)}>
              <span className="nav-icon">{icon}</span>
              <span>{t(label)}</span>
            </button>
          ))}
        </nav>
        <div className="main-scroll" ref={mainScrollRef}>

        <div className={`page ${page === "p1" ? "active" : ""}`} id="p1">
          <div className="p1-topbar">
            <div className="view-toggle-wrap">
              <button className={`view-btn ${view === "operator" ? "active" : ""}`} onClick={() => setView("operator")}>{t("Operator")}</button>
              <button className={`view-btn ${view === "executive" ? "active" : ""}`} onClick={() => setView("executive")}>{t("Executive")}</button>
            </div>
            <div className="kpi-strip" style={{ flex: 1, borderBottom: "none" }}>
              <div className="kpi">
                <div className="kpi-l">{t("Fleet Health")}</div>
                <div className="kpi-v g">{overview.kpis.fleet_health_score}%</div>
                <div className="kpi-d">{t("Average health score")}</div>
              </div>
              <div className="kpi">
                <div className="kpi-l">{t("Critical Assets")}</div>
                <div className="kpi-v r">{overview.kpis.critical_assets}</div>
                <div className="kpi-d">{t("Need immediate action")}</div>
              </div>
              <div className="kpi">
                <div className="kpi-l">{t("Asset Count")}</div>
                <div className="kpi-v a">{overview.kpis.asset_count}</div>
                <div className="kpi-d">{t("In monitored fleet")}</div>
              </div>
            </div>
          </div>

          <div className={`view-panel ${view === "operator" ? "active" : ""}`} id="view-operator">
            <div className="p1-main">
              <div className="asset-panel">
                <div className="panel-hdr">
                  <span className="panel-title">{t("Live asset risk matrix")}</span>
                  <div className="chips">
                    <button className={`chip ${assetSeverityFilter === "all" ? "active" : ""}`} onClick={() => setAssetSeverityFilter("all")}>{t("All")}</button>
                    <button className={`chip ${assetSeverityFilter === "critical" ? "active" : ""}`} onClick={() => setAssetSeverityFilter("critical")}>{t("Critical")}</button>
                    <button className={`chip ${assetSeverityFilter === "warning" ? "active" : ""}`} onClick={() => setAssetSeverityFilter("warning")}>{t("Warning")}</button>
                  </div>
                </div>
                <div className="asset-grid">
                  {filteredAssets.map((a) => (
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
                        <span className={`sbadge ${a.status}`}>{localizeStatusText(a.status, isJapanese)}</span>
                      </div>
                      <div className="card-metrics">
                        <div className="cm"><div className="cml">{t("Anomaly")}</div><div className="cmv">{a.anomaly_score}</div></div>
                        <div className="cm"><div className="cml">{t("RUL")}</div><div className="cmv">{a.rul_hours}h</div></div>
                        <div className="cm"><div className="cml">{t("Exposure")}</div><div className="cmv">{a.cost_exposure}</div></div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="agent-panel">
                <div className="agent-hdr">
                  <div className="adot" />
                  <div>
                    <div className="atitle">{t("Maintenance Supervisor AI")}</div>
                    <div className="asubt-row">
                      <div className="asubt">{t("Operational diagnosis and actions")}</div>
                      {activeGenieUrl ? (
                        <a className="agenie-link" href={activeGenieUrl} target="_blank" rel="noreferrer">
                          {t("Open Genie room")}
                        </a>
                      ) : (
                        <span className="agenie-link disabled">{t("Genie room unavailable")}</span>
                      )}
                    </div>
                    <div className="asubt-meta">
                      {t("Genie rooms configured")}: {Number(genieRooms?.configured_count || 0)}/{Number(genieRooms?.total_count || 5)}
                    </div>
                  </div>
                </div>
                <div className="msgs">
                  {agentMsgs.map((m, i) => (
                    <div className="msg" key={`${m.role}-${i}`}>
                      <div className={`av ${m.role === "user" ? "user" : "agent"}`}>{m.role === "user" ? t("ME") : t("AI")}</div>
                      <div className={`bubble ${m.role === "user" ? "user" : ""}`}>
                        {m.role === "agent" && <div className="bubble-lbl">{m.label}</div>}
                        {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                        {m.role === "agent" && Array.isArray(m.references) && m.references.length > 0 && (
                          <div style={{ marginTop: 8, borderTop: "1px solid var(--border)", paddingTop: 6, fontSize: 11, color: "var(--muted)" }}>
                            <div style={{ marginBottom: 4 }}>{t("Sources")}</div>
                            {m.references.slice(0, 4).map((r, idx) => (
                              <div key={`agent-ref-${i}-${idx}`}>[{r.source}] {(Number(r.score || 0) * 100).toFixed(0)}%</div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                  {agentPending && (
                    <div className="msg">
                      <div className="av agent">{t("AI")}</div>
                      <div className="bubble bubble-thinking">
                        <div className="bubble-lbl">{t("Maintenance Supervisor AI")}</div>
                        <div className="thinking-row">
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span>{t("Thinking...")}</span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <div className="agent-inp">
                  <input className="ainput" value={agentInput} onChange={(e) => setAgentInput(e.target.value)} onKeyDown={(e) => e.key === "Enter" && sendMessage()} placeholder={agentPending ? t("Processing your request...") : t("Ask about risk, RUL, and next action...")} disabled={agentPending} />
                  <button className="sbtn" onClick={sendMessage} disabled={agentPending}>{agentPending ? t("Processing...") : t("Send")}</button>
                </div>
              </div>
            </div>

            <div className="alert-bar">
              <div className="alert-hdr">{t("Recent alerts")}</div>
              <div className="arows">
                {(overview.alerts || []).map((a, i) => (
                  <div key={`${a.text}-${i}`} className={`arow ${a.severity}`} title={a.tooltip || ""}>
                    <div className={`apip ${a.severity}`} />
                    <span className="alert-text-wrap">
                      <span className="atext">{localizeAlertText(a.text, isJapanese)}</span>
                      <span
                        className="alert-tip"
                        title={a.tooltip || ""}
                        data-tip={a.tooltip || ""}
                        aria-label={a.tooltip || ""}
                        tabIndex={0}
                      >
                        i
                      </span>
                    </span>
                    <span className="atime">{t(a.time)}</span>
                    <div className="alert-actions">
                      {overview.actioned_assets?.includes(a.equipment_id) ? (
                        <span className="alert-actioned">{t("Actioned")}</span>
                      ) : (
                        <>
                          <button
                            className="alert-act-btn approve"
                            disabled={!a.equipment_id || !!recActionPending[`${a.equipment_id}:approve`]}
                            onClick={() => actOnRecommendation(a.equipment_id, "approve")}
                          >
                            {t("Approve")}
                          </button>
                          <button
                            className="alert-act-btn reject"
                            disabled={!a.equipment_id || !!recActionPending[`${a.equipment_id}:reject`]}
                            onClick={() => actOnRecommendation(a.equipment_id, "reject")}
                          >
                            {t("Reject")}
                          </button>
                          <button
                            className="alert-act-btn defer"
                            disabled={!a.equipment_id || !!recActionPending[`${a.equipment_id}:defer`]}
                            onClick={() => actOnRecommendation(a.equipment_id, "defer")}
                          >
                            {t("Defer")}
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className={`view-panel ${view === "executive" ? "active" : ""}`}>
            <div className="exec-wrap">
              <div className="exec-hero-card">
                <div className="exec-hero-eyebrow">{t("Executive briefing value statement")}</div>
                <div className="exec-board-hero">
                  <div className="exec-board-left">
                    <div className="exec-hero-title">{executive.value_statement || EMPTY_EXECUTIVE.value_statement}</div>
                    <div className="exec-hero-sub">
                      {t("Board-ready financial operating view")} - {industryLabel(industry)}.
                    </div>
                    <div className="exec-board-kpi-row">
                      <div className="exec-board-kpi">
                        <div className="exec-fin-label">{t("EBIT Protected (Quarter)")}</div>
                        <div className="exec-board-big">{executive.ebit_saved_fmt || "—"}</div>
                      </div>
                      <div className="exec-board-kpi">
                        <div className="exec-fin-label">{t("Variance (MoM / YoY)")}</div>
                        <div className="exec-board-mini">
                          MoM {Number(executive.mom_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.mom_ebit_pct || 0).toFixed(1)}% · YoY {Number(executive.yoy_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.yoy_ebit_pct || 0).toFixed(1)}%
                        </div>
                      </div>
                      <div className="exec-board-kpi">
                        <div className="exec-fin-label">{t("Annual run-rate")}</div>
                        <div className="exec-board-mini">{executive.executive_summary?.annualized_ebit_saved_fmt || "—"}</div>
                      </div>
                      <div className="exec-board-kpi">
                        <div className="exec-fin-label">{t("Annual target")}</div>
                        <div className="exec-board-mini">{executive.executive_summary?.annual_ebit_target_fmt || "—"}</div>
                      </div>
                    </div>
                  </div>
                  <div className="exec-board-right">
                    <TrendLine values={(executive.ebit_trend || []).map((p) => Number(p.value || 0))} color="#22c55e" height={90} />
                    <div className="exec-fin-sub" style={{ justifyContent: "flex-end" }}>
                      {t("Run-rate to annual target")}: {Number(executive.executive_summary?.run_rate_to_target_pct || 0).toFixed(1)}%
                    </div>
                  </div>
                </div>
                <div className="exec-hero-actions">
                  <button className="exec-jump-btn" onClick={() => setPage("p7")}>{t("Open Finance Command Center")}</button>
                  <button className="exec-jump-btn" onClick={exportExecutiveBriefing}>{t("Export Board Briefing (PDF)")}</button>
                </div>
              </div>

              <div className="exec-wow-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Forward outlook (30/90 days)")}</div>
                  <div className="exec-wow-row">
                    <span>{t("30d protected EBIT (with actions)")}</span>
                    <strong>{executive.forward_outlook?.horizon_30_days?.protected_with_actions_fmt || executive.ebit_saved_fmt || "—"}</strong>
                  </div>
                  <div className="exec-wow-row">
                    <span>{t("30d protected EBIT (if deferred)")}</span>
                    <strong>{executive.forward_outlook?.horizon_30_days?.protected_without_actions_fmt || "—"}</strong>
                  </div>
                  <div className="exec-wow-row">
                    <span>{t("90d EBIT at risk if deferred")}</span>
                    <strong>{executive.forward_outlook?.horizon_90_days?.at_risk_if_deferred_fmt || "—"}</strong>
                  </div>
                  <div className="exec-fin-sub">
                    {t("Confidence")} {Number(executive.executive_summary?.confidence_pct || 0).toFixed(1)}%
                    <span className="exec-tip" data-tip={execTips.confidence_pct || ""} aria-label={execTips.confidence_pct || ""} tabIndex={0}>i</span>
                  </div>
                </div>

                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Decision cockpit")}</div>
                  <div className="exec-slider-wrap">
                    <label className="exec-fin-sub" htmlFor="exec-delay-slider">{t("Defer top actions by weeks")}: {execDelayWeeks}</label>
                    <input
                      id="exec-delay-slider"
                      type="range"
                      min="0"
                      max="4"
                      step="1"
                      value={execDelayWeeks}
                      onChange={(e) => setExecDelayWeeks(Number(e.target.value || 0))}
                    />
                    <div className="exec-wow-risk">{t("Scenario at-risk EBIT")}: {execScenario.atRiskFmt}</div>
                  </div>
                  {(executive.decision_cockpit || []).slice(0, 3).map((d, i) => (
                    <div key={`dec-${i}`} className="exec-decision-row">
                      <div>
                        <div className="exec-wo-id">{d.title || d.equipment_id}</div>
                        <div className="exec-wo-meta">{d.equipment_id} · {t("Payback")} {Number(d.payback_days || 0).toFixed(1)}{shortDayUnit} · {t("Disruption")} {d.disruption_score}/10</div>
                      </div>
                      <div className="exec-wo-impact">{d.value_uplift_fmt || "—"}</div>
                    </div>
                  ))}
                </div>

                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Portfolio concentration")}</div>
                  <div className="exec-fin-val" style={{ fontSize: 30 }}>
                    {Number(executive.portfolio_insights?.concentration_top5_pct || 0).toFixed(1)}%
                  </div>
                  <div className="exec-fin-sub">
                    {t("Top 5 assets share of risk exposure")}
                    <span className="exec-tip" data-tip={execTips.concentration_top5_pct || ""} aria-label={execTips.concentration_top5_pct || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-chip-row">
                    {(executive.portfolio_insights?.top_risk_assets || []).slice(0, 5).map((a) => (
                      <span key={`risk-${a.asset_id}`} className="exec-chip">
                        {a.asset_id} {a.exposure_fmt}
                      </span>
                    ))}
                  </div>
                  <div className="exec-fin-sub">
                    {t("Run-rate to annual target")}: {Number(executive.executive_summary?.run_rate_to_target_pct || 0).toFixed(1)}%
                    <span className="exec-tip" data-tip={execTips.run_rate_to_target_pct || ""} aria-label={execTips.run_rate_to_target_pct || ""} tabIndex={0}>i</span>
                  </div>
                </div>
              </div>

              <div className="exec-finance-row">
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("EBIT Saved")}
                    <span className="exec-tip" data-tip={execTips.ebit_saved || ""} aria-label={execTips.ebit_saved || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.ebit_saved || ""}>{executive.ebit_saved_fmt || "—"}</div>
                  <div className="exec-fin-sub" title={execTips.source_table || ""}>
                    MoM {Number(executive.mom_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.mom_ebit_pct || 0).toFixed(1)}% ·
                    YoY {Number(executive.yoy_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.yoy_ebit_pct || 0).toFixed(1)}%
                    <span className="exec-tip" data-tip={`${execTips.mom_ebit_pct || ""} ${execTips.yoy_ebit_pct || ""}`.trim()} aria-label={`${execTips.mom_ebit_pct || ""} ${execTips.yoy_ebit_pct || ""}`.trim()} tabIndex={0}>i</span>
                  </div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("ROI")}
                    <span className="exec-tip" data-tip={execTips.roi_pct || ""} aria-label={execTips.roi_pct || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.roi_pct || ""}>{Number(executive.roi_pct || 0).toFixed(1)}%</div>
                  <div className="exec-fin-sub">
                    {t("Savings versus intervention + platform cost")}
                    <span className="exec-tip" data-tip={execTips.roi_pct || ""} aria-label={execTips.roi_pct || ""} tabIndex={0}>i</span>
                  </div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("Payback")}
                    <span className="exec-tip" data-tip={execTips.payback_days || ""} aria-label={execTips.payback_days || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.payback_days || ""}>{Number(executive.payback_days || 0).toFixed(1)} {dayUnit}</div>
                  <div className="exec-fin-sub">
                    {t("Estimated time to recover investment")}
                    <span className="exec-tip" data-tip={execTips.payback_days || ""} aria-label={execTips.payback_days || ""} tabIndex={0}>i</span>
                  </div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("EBIT Margin Lift")}
                    <span className="exec-tip" data-tip={execTips.ebit_margin_bps || ""} aria-label={execTips.ebit_margin_bps || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.ebit_margin_bps || ""}>{Number(executive.ebit_margin_bps || 0).toFixed(1)} bps</div>
                  <div className="exec-fin-sub" title={execTips.baseline_monthly_ebit || ""}>
                    Versus baseline monthly EBIT ({executive.baseline_monthly_ebit_fmt || "n/a"})
                    <span className="exec-tip" data-tip={execTips.baseline_monthly_ebit || ""} aria-label={execTips.baseline_monthly_ebit || ""} tabIndex={0}>i</span>
                  </div>
                </div>
              </div>

              <div className="exec-finance-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Value bridge to EBIT")}</div>
                  {(executive.value_bridge || []).map((b) => (
                    <div key={b.label} className="exec-bridge-row">
                      <span className="exec-bridge-label">
                        {b.label}
                        <span
                          className="exec-tip"
                          title={b.tooltip || execTips.source_table || ""}
                          data-tip={b.tooltip || execTips.source_table || ""}
                          aria-label={b.tooltip || execTips.source_table || ""}
                          tabIndex={0}
                        >
                          i
                        </span>
                      </span>
                      <span className={`exec-bridge-val ${b.kind === "negative" ? "neg" : "pos"}`} title={b.tooltip || execTips.source_table || ""}>{b.amount_fmt}</span>
                    </div>
                  ))}
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("ERP and work-order context")}</div>
                  <div className="exec-erp-grid">
                    <div><span className="exec-erp-k">Plant</span><span className="exec-erp-v">{executive.erp?.plant_code || "—"}</span></div>
                    <div><span className="exec-erp-k">Fiscal period</span><span className="exec-erp-v">{executive.erp?.fiscal_period || "—"}</span></div>
                    <div><span className="exec-erp-k">Planner group</span><span className="exec-erp-v">{executive.erp?.planner_group || "—"}</span></div>
                    <div><span className="exec-erp-k">Account</span><span className="exec-erp-v">{executive.erp?.reference_account || "—"}</span></div>
                  </div>
                  <div className="exec-erp-source" title={execTips.source_table || ""}>
                    {t("Source table:")} {executive.source_table || "simulated model"}
                    <span
                      className="exec-tip"
                      title={execTips.source_table || ""}
                      data-tip={execTips.source_table || ""}
                      aria-label={execTips.source_table || ""}
                      tabIndex={0}
                    >
                      i
                    </span>
                  </div>
                  <div className="exec-chip-row">
                    {(executive.erp?.cost_centers || []).map((c) => <span key={c} className="exec-chip">{c}</span>)}
                  </div>
                </div>
              </div>

              <div className="exec-summary-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("EBIT impact trend (simulated)")}</div>
                  <TrendLine values={(executive.ebit_trend || []).map((p) => Number(p.value || 0))} color="#0F766E" height={120} />
                  <div className="exec-chip-row">
                    {(executive.ebit_trend || []).map((p) => (
                      <span key={`trend-${p.label}`} className="exec-chip" title={execTips.ebit_saved || ""}>{p.label}: {p.value_fmt}</span>
                    ))}
                  </div>
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">
                    {t("Financial impact by work order")}
                    <span className="exec-tip" data-tip={execTips.work_orders || ""} aria-label={execTips.work_orders || ""} tabIndex={0}>i</span>
                  </div>
                  {(executive.work_orders || []).slice(0, 6).map((w) => (
                    <div key={w.wo_id} className="exec-wo-row">
                      <div>
                        <div className="exec-wo-id">{w.wo_id}</div>
                        <div className="exec-wo-meta">
                          {w.equipment_id} · {w.priority} · {w.work_center}
                          <span className="exec-tip" data-tip={execTips.work_orders || ""} aria-label={execTips.work_orders || ""} tabIndex={0}>i</span>
                        </div>
                      </div>
                      <div className="exec-wo-impact" title={execTips.work_order_net_ebit_impact || execTips.ebit_saved || ""}>
                        {w.net_ebit_impact_fmt}
                        <span className="exec-tip" data-tip={execTips.work_order_net_ebit_impact || execTips.ebit_saved || ""} aria-label={execTips.work_order_net_ebit_impact || execTips.ebit_saved || ""} tabIndex={0}>i</span>
                      </div>
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
                    <span className={`sbadge ${assetDetail.status}`}>{localizeStatusText(assetDetail.status, isJapanese)}</span>
                  </div>
                  <div className="p2-type">{assetDetail.type}</div>
                  <div className="p2-crumb">{assetDetail.crumb}</div>
                </div>
              </div>

              <div className="stat-cards">
                <div className="stat-card"><div className="sc-label">Health score</div><div className="sc-val" style={{ color: statusColor(assetDetail.status) }}>{assetDetail.health_score_pct}%</div><div className="sc-sub">{assetDetail.status}</div></div>
                <div className="stat-card"><div className="sc-label">RUL remaining</div><div className="sc-val">{assetDetail.rul_hours}h</div><div className="sc-sub">Estimated hours to failure</div></div>
                <div className="stat-card"><div className="sc-label">Anomaly score</div><div className="sc-val">{assetDetail.anomaly_score}</div><div className="sc-sub">Isolation forest</div></div>
                <div className="stat-card"><div className="sc-label">Protocol</div><div className="sc-val" style={{ fontSize: 18 }}>{model?.model_meta?.protocol || "OPC-UA"}</div><div className="sc-sub">Source: {model?.model_meta?.data_source || "UNKNOWN"}</div></div>
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
          <div className="p3-wrap" style={{ gridTemplateColumns: `${hierPanelWidth}px 8px 1fr` }}>
            <div className="tree-panel">
              <div className="tree-hdr-row">
                <div className="tree-hdr">Asset hierarchy</div>
                <div className="tree-mode-toggle">
                  <button className={`tree-mode-btn ${hierViewMode === "map" ? "active" : ""}`} onClick={() => setHierViewMode("map")}>{t("Factory map")}</button>
                  <button className={`tree-mode-btn ${hierViewMode === "tree" ? "active" : ""}`} onClick={() => setHierViewMode("tree")}>{t("Hierarchy tree")}</button>
                </div>
              </div>
              {hierViewMode === "tree" ? (
                hierarchy && <TreeNode node={hierarchy} onSelect={setHierSelection} />
              ) : (
                <div className={`factory-map industry-${industry}`} onMouseMove={moveMapDrag} onMouseUp={endMapDrag} onMouseLeave={endMapDrag} onWheel={onMapWheel}>
                  <div className="factory-map-controls">
                    <button className="map-ctrl-btn" onClick={() => mapZoom(-0.1)}>{t("Map zoom out")}</button>
                    <button className="map-ctrl-btn" onClick={() => mapZoom(0.1)}>{t("Map zoom in")}</button>
                    <button className="map-ctrl-btn" onClick={mapReset}>{t("Map reset")}</button>
                    <span className="map-zoom-readout">{Math.round(mapView.scale * 100)}%</span>
                  </div>
                  <div
                    className={`factory-map-canvas industry-${industry}`}
                    style={{ transform: `translate(${mapView.x}px, ${mapView.y}px) scale(${mapView.scale})` }}
                    onMouseDown={startMapDrag}
                  >
                    <div className="factory-map-overlay" />
                    <div className="factory-map-title">{t("Physical layout map")} · {industryLabel(industry)}</div>
                    <div className="factory-map-zones">
                      {mapZoneChips.slice(0, 4).map((z) => <span key={`mz-${z.name}`}>{z.name} ({z.count})</span>)}
                    </div>
                    {mapPins.map((p) => (
                      <button
                        key={`pin-${p.id}`}
                        className={`asset-pin ${p.status}`}
                        style={{ left: `${p.x}%`, top: `${p.y}%` }}
                        title={`${p.id} · ${p.type} · ${p.zone} · anomaly ${p.anomaly}`}
                      onMouseDown={(e) => e.stopPropagation()}
                        onClick={() => {
                          setSelectedAssetId(p.id);
                          const targetNode = (hierarchy?.children || [])
                            .flatMap((s) => (s.children || []).flatMap((a) => (a.children || [])))
                            .flatMap((u) => u.children || [])
                            .find((e) => e.asset_id === p.id);
                          if (targetNode) setHierSelection(targetNode);
                        }}
                      >
                        <span className="asset-pin-dot" />
                        <span className="asset-pin-label">{p.id}</span>
                      </button>
                    ))}
                  </div>
                  <div className="factory-map-legend">
                    <span className="lg healthy">Healthy</span>
                    <span className="lg warning">Warning</span>
                    <span className="lg critical">Critical</span>
                  </div>
                </div>
              )}
            </div>
            <div className={`p3-splitter ${hierResizing ? "active" : ""}`} onMouseDown={() => setHierResizing(true)} />
            <div className="detail-panel">
              {hierViewMode === "map" && (
                <div className="map-copilot-panel">
                  <div className="map-copilot-hdr">
                    <strong>{t("Ask AI")}</strong>
                    <span className="map-copilot-asset">{selectedAssetId || "Select an asset pin"}</span>
                  </div>
                  <div className="map-copilot-msgs">
                    {mapCopilotMsgs.slice(-6).map((m, i) => (
                      <div key={`mcp-${i}`} className={`map-copilot-msg ${m.role}`}>
                        <div className="map-copilot-msg-lbl">{m.role === "user" ? t("ME") : t("AI")}</div>
                        <div className="map-copilot-msg-body">
                          {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                        </div>
                      </div>
                    ))}
                    {mapCopilotPending && <div className="map-copilot-pending">{t("Sending...")}</div>}
                  </div>
                  <div className="map-copilot-input-row">
                    <input
                      className="map-copilot-input"
                      value={mapCopilotInput}
                      onChange={(e) => setMapCopilotInput(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && sendMapCopilotMessage()}
                      placeholder={t("Ask about this location and incident context...")}
                      disabled={!selectedAssetId || mapCopilotPending}
                    />
                    <button className="map-copilot-send" onClick={sendMapCopilotMessage} disabled={!selectedAssetId || mapCopilotPending}>
                      {mapCopilotPending ? t("Sending...") : t("Ask AI")}
                    </button>
                  </div>
                </div>
              )}
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
                    <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                      <button className="back-btn" onClick={() => { setSelectedAssetId(hierSelection.asset_id); setPage("p2"); }}>
                        View asset drilldown →
                      </button>
                      <button
                        className="back-btn"
                        onClick={() => investigateHierarchyInGenie(hierSelection.asset_id)}
                      >
                        {t("Investigate in Genie")}
                      </button>
                    </div>
                  )}
                  {hierViewMode === "tree" && hierGenieOpen && (hierGenieAssetOptions.length > 0 || !!hierSelection?.asset_id) && (
                    <div className="map-copilot-panel" style={{ marginTop: 10 }}>
                      <div className="map-copilot-hdr">
                        <strong>{t("Ask AI")}</strong>
                        <span className="map-copilot-asset">{hierGenieAssetId || "Select asset"}</span>
                        <button className="map-copilot-send" onClick={() => setHierGenieOpen(false)} style={{ marginLeft: "auto" }}>
                          Close
                        </button>
                      </div>
                      <div className="map-copilot-input-row" style={{ marginBottom: 8 }}>
                        <select
                          className="map-copilot-input"
                          value={hierGenieAssetId}
                          onChange={(e) => {
                            setHierGenieTargetAssetId(e.target.value);
                            setSelectedAssetId(e.target.value);
                          }}
                        >
                          <option value="">Select asset from picture...</option>
                          {hierGenieAssetOptions.map((id) => (
                            <option key={`hgo-${id}`} value={id}>{id}</option>
                          ))}
                        </select>
                      </div>
                      <div className="map-copilot-msgs">
                        {hierGenieMsgs.slice(-8).map((m, i) => (
                          <div key={`hgp-${i}`} className={`map-copilot-msg ${m.role}`}>
                            <div className="map-copilot-msg-lbl">{m.role === "user" ? t("ME") : t("AI")}</div>
                            <div className="map-copilot-msg-body">
                              {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                            </div>
                          </div>
                        ))}
                        {hierGeniePending && <div className="map-copilot-pending">{t("Sending...")}</div>}
                      </div>
                      <div className="map-copilot-input-row">
                        <input
                          className="map-copilot-input"
                          value={hierGenieInput}
                          onChange={(e) => setHierGenieInput(e.target.value)}
                          onKeyDown={(e) => e.key === "Enter" && sendHierarchyGenieMessage()}
                          placeholder={t("Ask about this equipment, risk, and action plan...")}
                          disabled={!hierGenieAssetId || hierGeniePending}
                        />
                        <button
                          className="map-copilot-send"
                          onClick={() => sendHierarchyGenieMessage()}
                          disabled={!hierGenieAssetId || hierGeniePending}
                        >
                          {hierGeniePending ? t("Sending...") : t("Ask AI")}
                        </button>
                      </div>
                    </div>
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
              <span className="p5-label">{t("Asset")}</span>
              <select className="p5-select" value={selectedAssetId} onChange={(e) => setSelectedAssetId(e.target.value)}>
                {overview.assets.map((a) => <option key={`p5-${a.id}`} value={a.id}>{a.id} — {a.type}</option>)}
              </select>
            </div>

            {model && (
              <>
                <div className="model-meta">
                  <div className="mm-item"><div className="mm-label">{t("Model trained")}</div><div className="mm-val">{model.model_meta.trained}</div></div>
                  <div className="mm-item"><div className="mm-label">{t("RUL accuracy (R²)")}</div><div className="mm-val">{model.model_meta.r2}</div></div>
                  <div className="mm-item"><div className="mm-label">{t("RMSE")}</div><div className="mm-val">{model.model_meta.rmse}</div></div>
                </div>
                {!model?.model_meta?.is_model_driven && (
                  <div className="notice warning" style={{ marginBottom: 12 }}>
                    {model?.model_meta?.status_message || "No scored prediction found yet. Train and score this asset first."}
                  </div>
                )}
                <div className="p5-grid">
                  <div className="chart-card full">
                    <div className="cc-title">{t("RUL degradation curve")}</div>
                    <div className="rul-stats">
                      <div className="rul-stat"><span className="rul-stat-l">{t("Current RUL")}</span><span className="rul-stat-v">{model.rul_hours == null ? "n/a" : `${model.rul_hours}h`}</span></div>
                    </div>
                    <TrendLine values={model.rul_curve.values || []} color="#1B2431" height={180} showXLabels />
                  </div>
                  <div className="chart-card">
                    <div className="cc-title">{t("Feature importance — anomaly model")}</div>
                    <div className="anomaly-decomp">
                      {!model.feature_importance?.length && <div className="cc-sub">No model-driven feature attribution available yet.</div>}
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
                    <div className="cc-title">{t("Anomaly score decomposition")}</div>
                    <div className="anomaly-decomp">
                      {!model.anomaly_decomposition?.length && <div className="cc-sub">No model-driven decomposition available yet.</div>}
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

                {advancedPdm && (
                  <div className="chart-card full">
                    <div className="cc-title">Advanced PdM Command Layer</div>
                    <div className="exec-summary-grid">
                      <div className="exec-trend-card">
                        <div className="exec-card-title">Failure mode-centric PdM</div>
                        {(advancedPdm.failure_mode_centric || []).slice(0, 6).map((m) => (
                          <div key={`fm-${m.mode}`} className="exec-wo-row">
                            <div>
                              <div className="exec-wo-id">{m.mode}</div>
                              <div className="exec-wo-meta">Sensors: {m.sensor_count} · Confidence: {(Number(m.confidence || 0) * 100).toFixed(0)}%</div>
                            </div>
                            <div className="exec-wo-impact">{(Number(m.likelihood || 0) * 100).toFixed(0)}% risk · {m.priority}</div>
                          </div>
                        ))}
                      </div>
                      <div className="exec-trend-card">
                        <div className="exec-card-title">Prescriptive optimizer</div>
                        <div className="exec-fin-sub">Window: {(advancedPdm.prescriptive_optimizer || {}).recommended_window || "n/a"}</div>
                        <div className="exec-fin-sub">Mode: {(advancedPdm.prescriptive_optimizer || {}).event_type || "n/a"} · Source: {(advancedPdm.prescriptive_optimizer || {}).data_source || "UNKNOWN"}</div>
                        <div className="exec-fin-val" style={{ fontSize: 24 }}>
                          {(advancedPdm.prescriptive_optimizer || {}).expected_avoided_loss_fmt || "—"}
                        </div>
                        <div className="exec-fin-sub">
                          Planned intervention: {(advancedPdm.prescriptive_optimizer || {}).planned_intervention_cost_fmt || "—"}
                        </div>
                        <div className="exec-fin-sub">
                          Expected failure cost: {(advancedPdm.prescriptive_optimizer || {}).expected_failure_cost_fmt || "—"}
                        </div>
                        <div className="exec-chip-row">
                          {((advancedPdm.prescriptive_optimizer || {}).actions || []).map((a, i) => (
                            <span key={`act-${i}`} className="exec-chip">{a}</span>
                          ))}
                        </div>
                      </div>
                    </div>

                    <div className="exec-summary-grid" style={{ marginTop: 12 }}>
                      <div className="exec-trend-card">
                        <div className="exec-card-title">Spare parts risk planning</div>
                        {(advancedPdm.spare_parts_risk_planning || []).slice(0, 5).map((p) => (
                          <div key={`sp-${p.part_number}`} className="exec-wo-row">
                            <div>
                              <div className="exec-wo-id">{p.part_number}</div>
                              <div className="exec-wo-meta">{p.description} · lead {p.lead_time_days}d</div>
                            </div>
                            <div className="exec-wo-impact">
                              on-hand {p.quantity_on_hand} / need {p.required_qty_risk_adjusted} · reorder {p.recommended_reorder_qty}
                            </div>
                          </div>
                        ))}
                      </div>
                      <div className="exec-trend-card">
                        <div className="exec-card-title">MLOps for industrial AI</div>
                        <div className="exec-fin-sub">Anomaly model: {(advancedPdm.mlops_industrial_ai || {}).anomaly_model_version || "n/a"}</div>
                        <div className="exec-fin-sub">RUL model: {(advancedPdm.mlops_industrial_ai || {}).rul_model_version || "n/a"}</div>
                        <div className="exec-fin-sub">Data drift index: {(advancedPdm.mlops_industrial_ai || {}).data_drift_index}</div>
                        <div className="exec-fin-sub">Feedback coverage: {(advancedPdm.mlops_industrial_ai || {}).label_feedback_coverage_pct}%</div>
                        <div className="exec-fin-sub">
                          Retrain recommended: {(advancedPdm.mlops_industrial_ai || {}).retrain_recommended ? "yes" : "no"}
                        </div>
                      </div>
                    </div>

                    <div className="exec-trend-card" style={{ marginTop: 12 }}>
                      <div className="exec-card-title">Manual-aware recommendations (PDF/text references)</div>
                      {(advancedPdm.manual_references || []).length ? (
                        (advancedPdm.manual_references || []).map((r, i) => (
                          <div key={`ref-${i}`} className="exec-exp-row">
                            <div className="exec-exp-asset">[{r.source}]</div>
                            <div className="exec-exp-bar-wrap" />
                            <div className="exec-exp-val">{(Number(r.score || 0) * 100).toFixed(0)}%</div>
                          </div>
                        ))
                      ) : (
                        <div className="exec-fin-sub">No manual snippets found for this query context.</div>
                      )}

                      <div style={{ marginTop: 10, display: "flex", gap: 8 }}>
                        <textarea
                          value={manualInput}
                          onChange={(e) => setManualInput(e.target.value)}
                          placeholder="Paste manual excerpt here to demo parsing (supports PDF-extracted text)."
                          style={{ flex: 1, minHeight: 90, border: "1px solid var(--border)", borderRadius: 6, padding: 8, fontSize: 12 }}
                        />
                        <button className="sbtn" onClick={parseManualText} disabled={manualParsePending}>
                          {manualParsePending ? "Parsing..." : "Parse manual"}
                        </button>
                      </div>
                      <div style={{ marginTop: 8, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                        <input
                          type="file"
                          accept=".pdf,.md,.txt"
                          onChange={(e) => setManualFile((e.target.files && e.target.files[0]) || null)}
                        />
                        <button className="sbtn" onClick={uploadManualFile} disabled={!manualFile || manualUploadPending}>
                          {manualUploadPending ? "Uploading..." : "Upload and index manual"}
                        </button>
                        {manualFile && <span style={{ fontSize: 12, color: "var(--muted)" }}>{manualFile.name}</span>}
                      </div>
                      {manualParse && (
                        <div style={{ marginTop: 8, fontSize: 12, color: "var(--muted)" }}>
                          {(manualParse.summary || manualParse.message || "").toString()}
                          {!!Object.keys(manualParse.fields || {}).length && (
                            <div style={{ marginTop: 6 }}>
                              {Object.entries(manualParse.fields || {}).map(([k, v]) => (
                                <div key={`fld-${k}`}><strong>{k}:</strong> {String(v)}</div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>

        <div className={`page ${page === "p6" ? "active" : ""}`} id="p6">
          <div className="p6-inner-tabs">
            <button className={`p6itab ${simTab === "sim" ? "active" : ""}`} onClick={() => setSimTab("sim")}>{t("Sim")}</button>
            <button className={`p6itab ${simTab === "config" ? "active" : ""}`} onClick={() => setSimTab("config")}>{t("Industry Configuration")}</button>
            <button className={`p6itab ${simTab === "connector" ? "active" : ""}`} onClick={() => setSimTab("connector")}>{t("Connector Setup")}</button>
            <button className={`p6itab ${simTab === "sdt" ? "active" : ""}`} onClick={() => setSimTab("sdt")}>{t("SDT Benchmark")}</button>
          </div>

          <div className={`p6-panel ${simTab === "sim" ? "active" : ""}`}>
            <div className="p6-wrap">
              <div className="sim-controls">
                <div className="sim-ctrl-group"><span className="sim-ctrl-label">{t("Tick interval")}</span><input className="sim-range" type="range" min="200" max="2000" step="100" value={simState.tick_interval_ms || 800} onChange={(e) => setSimState((p) => ({ ...p, tick_interval_ms: Number(e.target.value) }))} /><span className="sim-ctrl-val">{simState.tick_interval_ms || 800}ms</span></div>
                <div className="sim-ctrl-group"><span className="sim-ctrl-label">{t("Noise factor")}</span><input className="sim-range" type="range" min="1" max="20" step="1" value={Math.round((simState.noise_factor || 0.02) * 100)} onChange={(e) => setSimState((p) => ({ ...p, noise_factor: Number(e.target.value) / 100 }))} /><span className="sim-ctrl-val">{(simState.noise_factor || 0.02).toFixed(2)}</span></div>
                <button className="sim-start" style={{ display: simState.running ? "none" : "inline-block" }} onClick={() => setSimRunning(true)}>▶ {t("Start simulator")}</button>
                <button className="sim-stop" style={{ display: simState.running ? "inline-block" : "none" }} onClick={() => setSimRunning(false)}>▮▮ {t("Stop")}</button>
                <button className="sim-score" onClick={triggerSimScoring} disabled={simScoringPending}>
                  {simScoringPending ? t("Injecting + scoring...") : t("Inject faults + score")}
                </button>
                <div className="sim-status"><div className={`sim-dot ${simState.running ? "running" : ""}`} /><span>{simState.running ? t("Running") : t("Stopped")}</span></div>
                {!!simScoringRunId && <span className="sim-run-badge">{t("Scoring run")} #{simScoringRunId}</span>}
                <span className="sim-reading-count">{(simState.reading_count || 0).toLocaleString()} {t("readings emitted")}</span>
                {(simPipeline.active || simPipeline.completedAt) && (
                  <div className="sim-pipeline-visual">
                    <div className="sim-pipeline-head">
                      <strong>{t("Fault -> Bronze -> Silver -> Gold -> Scoring")}</strong>
                      <span>{simPipeline.phase || "Ready"}</span>
                    </div>
                    <div className="sim-pipeline-steps">
                      <span className={`sim-step ${simPipeline.rowsEmitted > 0 ? "done" : simPipeline.active ? "live" : ""}`}>Inject {simPipeline.rowsEmitted || 0} rows</span>
                      <span className={`sim-step ${simFlowDelta.bronze > 0 ? "done" : simPipeline.rowsEmitted > 0 && simPipeline.runStatus !== "TERMINATED" ? "live" : ""}`}>Bronze +{Math.max(0, simFlowDelta.bronze)}{simFlowDelta.bronze <= 0 && simPipeline.rowsEmitted > 0 ? " (pending)" : ""}</span>
                      <span className={`sim-step ${simFlowDelta.silver > 0 ? "done" : simPipeline.rowsEmitted > 0 && simPipeline.runStatus !== "TERMINATED" ? "live" : ""}`}>Silver +{Math.max(0, simFlowDelta.silver)}{simFlowDelta.silver <= 0 && simPipeline.rowsEmitted > 0 ? " (pending)" : ""}</span>
                      <span className={`sim-step ${simFlowDelta.gold > 0 ? "done" : simPipeline.rowsEmitted > 0 && simPipeline.runStatus !== "TERMINATED" ? "live" : ""}`}>Gold +{Math.max(0, simFlowDelta.gold)}{simFlowDelta.gold <= 0 && simPipeline.rowsEmitted > 0 ? " (pending)" : ""}</span>
                      <span className={`sim-step ${simPipeline.runStatus === "TERMINATED" && simPipeline.runResult === "SUCCESS" ? "done" : simPipeline.runStatus === "RUNNING" ? "live" : ""}`}>
                        Score {simPipeline.runStatus || "pending"}{simPipeline.runResult ? `/${simPipeline.runResult}` : ""}
                      </span>
                    </div>
                    {!!simPipeline.error && <div className="sim-pipeline-err">{simPipeline.error}</div>}
                  </div>
                )}
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
                        <div className="fc-row">
                          <span className="fc-label">Alert</span>
                          <button className="fc-force-critical" onClick={() => forceCritical(a.id)} disabled={!!forceCriticalPendingByAsset[a.id]}>
                            {forceCriticalPendingByAsset[a.id] ? t("Forcing...") : t("Force critical")}
                          </button>
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
                      <div className="bronze-title">{t("Live ingestion flow")}</div>
                      <div className="bronze-subtitle">{t("3 stages: Bronze → Silver → Gold (5 recent rows each)")}</div>
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
                                  {prioritizeFlowRows(data.rows || [], simPipeline.enabledAssets || [], simPipeline.startedAt).slice(0, 5).map((r, i) => (
                                    <tr key={`${data.stage}-row-${i}`} className={`${r.quality !== "good" ? r.quality : ""} ${r._isInjectedTop ? "injected-row" : ""}`.trim()}>
                                      <td className="mono">{r.timestamp}</td>
                                      <td className="mono">{r.equipment_id} {r._isInjectedTop ? <span className="injected-pill">INJECTED</span> : null}</td>
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
                <div className="exec-hero-eyebrow">{t("Executive command center")}</div>
                <div className="exec-hero-title">{executive.value_statement || EMPTY_EXECUTIVE.value_statement}</div>
                <div className="exec-hero-sub">{t("One-pane financial view for predictive maintenance value realization.")}</div>
                <div className="exec-hero-actions">
                  <button
                    className="exec-jump-btn"
                    onClick={() => {
                      setPage("p1");
                      setView("executive");
                    }}
                  >
                    {t("Back to Executive View")}
                  </button>
                </div>
              </div>

              <div className="exec-finance-row">
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("EBIT Saved")}
                    <span className="exec-tip" data-tip={execTips.ebit_saved || ""} aria-label={execTips.ebit_saved || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.ebit_saved || ""}>{executive.ebit_saved_fmt || "—"}</div>
                  <div className="exec-fin-sub" title={execTips.source_table || ""}>
                    MoM {Number(executive.mom_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.mom_ebit_pct || 0).toFixed(1)}% ·
                    YoY {Number(executive.yoy_ebit_pct || 0) >= 0 ? "+" : ""}{Number(executive.yoy_ebit_pct || 0).toFixed(1)}%
                    <span className="exec-tip" data-tip={`${execTips.mom_ebit_pct || ""} ${execTips.yoy_ebit_pct || ""}`.trim()} aria-label={`${execTips.mom_ebit_pct || ""} ${execTips.yoy_ebit_pct || ""}`.trim()} tabIndex={0}>i</span>
                  </div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("ROI")}
                    <span className="exec-tip" data-tip={execTips.roi_pct || ""} aria-label={execTips.roi_pct || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.roi_pct || ""}>{Number(executive.roi_pct || 0).toFixed(1)}%</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("Payback")}
                    <span className="exec-tip" data-tip={execTips.payback_days || ""} aria-label={execTips.payback_days || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.payback_days || ""}>{Number(executive.payback_days || 0).toFixed(1)} {dayUnit}</div>
                </div>
                <div className="exec-fin-card">
                  <div className="exec-fin-label">
                    {t("Margin Lift")}
                    <span className="exec-tip" data-tip={execTips.ebit_margin_bps || ""} aria-label={execTips.ebit_margin_bps || ""} tabIndex={0}>i</span>
                  </div>
                  <div className="exec-fin-val" title={execTips.ebit_margin_bps || ""}>{Number(executive.ebit_margin_bps || 0).toFixed(1)} bps</div>
                </div>
              </div>

              <div className="p7-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("EBIT trend")}</div>
                  <TrendLine values={(executive.ebit_trend || []).map((p) => Number(p.value || 0))} color="#0F766E" height={140} />
                  <div className="exec-chip-row">
                    {(executive.ebit_trend || []).map((p) => (
                      <span key={`p7-trend-${p.label}`} className="exec-chip" title={execTips.ebit_saved || ""}>{p.label}: {p.value_fmt}</span>
                    ))}
                  </div>
                </div>
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Value bridge")}</div>
                  {(executive.value_bridge || []).map((b) => (
                    <div key={`p7-bridge-${b.label}`} className="exec-bridge-row">
                      <span className="exec-bridge-label">
                        {b.label}
                        <span
                          className="exec-tip"
                          title={b.tooltip || execTips.source_table || ""}
                          data-tip={b.tooltip || execTips.source_table || ""}
                          aria-label={b.tooltip || execTips.source_table || ""}
                          tabIndex={0}
                        >
                          i
                        </span>
                      </span>
                      <span className={`exec-bridge-val ${b.kind === "negative" ? "neg" : "pos"}`} title={b.tooltip || execTips.source_table || ""}>{b.amount_fmt}</span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="p7-insights-grid">
                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Executive scenario outlook")}</div>
                  <div className="exec-wow-row">
                    <span>{t("30d protected with actions")}</span>
                    <strong>{executive.forward_outlook?.horizon_30_days?.protected_with_actions_fmt || executive.ebit_saved_fmt || "—"}</strong>
                  </div>
                  <div className="exec-wow-row">
                    <span>{t("30d protected if deferred")}</span>
                    <strong>{executive.forward_outlook?.horizon_30_days?.protected_without_actions_fmt || "—"}</strong>
                  </div>
                  <div className="exec-wow-row">
                    <span>{t("90d EBIT at risk if deferred")}</span>
                    <strong>{executive.forward_outlook?.horizon_90_days?.at_risk_if_deferred_fmt || execScenario.atRiskFmt}</strong>
                  </div>
                  <div className="exec-fin-sub">
                    {t("Confidence")} {Number(executive.executive_summary?.confidence_pct || 0).toFixed(1)}%
                    <span className="exec-tip" data-tip={execTips.confidence_pct || ""} aria-label={execTips.confidence_pct || ""} tabIndex={0}>i</span>
                  </div>
                </div>

                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Top decision actions")}</div>
                  {(executive.decision_cockpit || []).slice(0, 3).map((d, i) => (
                    <div key={`p7-decision-${i}`} className="exec-decision-row">
                      <div>
                        <div className="exec-wo-id">{d.title || d.equipment_id || "Action"}</div>
                        <div className="exec-wo-meta">
                          {d.equipment_id || "—"} · {t("Payback")} {Number(d.payback_days || 0).toFixed(1)}{shortDayUnit} · {t("Disruption")} {d.disruption_score || 0}/10
                        </div>
                      </div>
                      <div className="exec-wo-impact">{d.value_uplift_fmt || "—"}</div>
                    </div>
                  ))}
                  {!(executive.decision_cockpit || []).length && (
                    <div className="exec-fin-sub">{t("No decision actions available in current window.")}</div>
                  )}
                </div>

                <div className="exec-trend-card">
                  <div className="exec-card-title">{t("Executive briefing metadata")}</div>
                  <div className="exec-erp-grid">
                    <div><span className="exec-erp-k">{t("Industry")}</span><span className="exec-erp-v">{industryLabel(industry)}</span></div>
                    <div><span className="exec-erp-k">{t("Currency")}</span><span className="exec-erp-v">{effectiveUiCurrency}</span></div>
                    <div><span className="exec-erp-k">{t("Annual run-rate")}</span><span className="exec-erp-v">{executive.executive_summary?.annualized_ebit_saved_fmt || "—"}</span></div>
                    <div><span className="exec-erp-k">{t("Annual target")}</span><span className="exec-erp-v">{executive.executive_summary?.annual_ebit_target_fmt || "—"}</span></div>
                    <div><span className="exec-erp-k">{t("Run-rate to annual target")}</span><span className="exec-erp-v">{Number(executive.executive_summary?.run_rate_to_target_pct || 0).toFixed(1)}%</span></div>
                    <div><span className="exec-erp-k">{t("Prepared at")}</span><span className="exec-erp-v">{briefingStamp}</span></div>
                  </div>
                  <div className="exec-erp-source">
                    {t("Source table")}: {executive.source_table || "—"}
                    <span className="exec-tip" data-tip={execTips.source_table || ""} aria-label={execTips.source_table || ""} tabIndex={0}>i</span>
                  </div>
                </div>
              </div>
            </div>

            <div className="p7-right">
              <div className="agent-panel finance-panel">
                <div className="agent-hdr">
                  <div className="adot" />
                  <div>
                    <div className="atitle">{t("Finance Scenario Genie")}</div>
                    <div className="asubt">{t("Ask financial what-if scenarios in predictive maintenance context")}</div>
                  </div>
                </div>
                <div className="msgs">
                  {financeMsgs.map((m, i) => (
                    <div className="msg" key={`f-${m.role}-${i}`}>
                      <div className={`av ${m.role === "user" ? "user" : "agent"}`}>{m.role === "user" ? "ME" : "AI"}</div>
                      <div className={`bubble ${m.role === "user" ? "user" : ""}`}>
                        {m.role === "agent" && <div className="bubble-lbl">{m.label}</div>}
                        {m.role === "agent" ? renderSimpleMarkdown(m.text) : m.text}
                        {m.role === "agent" && Array.isArray(m.references) && m.references.length > 0 && (
                          <div style={{ marginTop: 8, borderTop: "1px solid var(--border)", paddingTop: 6, fontSize: 11, color: "var(--muted)" }}>
                            <div style={{ marginBottom: 4 }}>{isJapanese ? "参照ソース" : "Sources"}</div>
                            {m.references.slice(0, 4).map((r, idx) => (
                              <div key={`fin-ref-${i}-${idx}`}>[{r.source}] {(Number(r.score || 0) * 100).toFixed(0)}%</div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                  {financePending && (
                    <div className="msg">
                      <div className="av agent">AI</div>
                      <div className="bubble bubble-thinking">
                        <div className="bubble-lbl">{isJapanese ? "財務コマンドAI" : "Finance Command AI"}</div>
                        <div className="thinking-row">
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span className="thinking-dot" />
                          <span>{t("Thinking...")}</span>
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
                    placeholder={financePending ? t("Processing scenario...") : t("Ask: If we defer maintenance by 2 weeks, what is EBIT impact?")}
                    disabled={financePending}
                  />
                  <button className="sbtn" onClick={sendFinanceMessage} disabled={financePending}>{financePending ? t("Processing...") : t("Ask")}</button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className={`page ${page === "p8" ? "active" : ""}`} id="p8">
          <div className="geo-page">
            <div className="geo-topbar">
              <div className="geo-top-title">Geo Intelligence</div>
              <div className="geo-top-meta">
                {geoLoading ? "Loading sites..." : `${geoSites.length} sites`}
                {geoError ? <span className="geo-err">{geoError}</span> : null}
              </div>
              <button className="sbtn" onClick={refetchGeoSites}>Refresh</button>
            </div>
            <div className="geo-stack">
              <div className="geo-map-top">
                <GeoMap
                  sites={geoSites}
                  onSiteClick={onGeoSiteClick}
                  activeSiteId={activeSiteId}
                  activeIndustries={visibleIndustries}
                  onToggleIndustry={toggleGeoIndustry}
                  mapLayer={mapLayer}
                  onMapLayerChange={setMapLayer}
                />
              </div>
              <div className="geo-drill-wrap">
                {activeGeoSite ? (
                  <div className="geo-facility-wrap">
                    <div className="geo-facility-hdr">
                      <button className="chip" onClick={() => { setGeoView("geo"); setActiveSiteId(null); setActiveAssetId(null); }}>Clear selection</button>
                      <div className="geo-facility-title">{activeGeoSite.customer} · {activeGeoSite.name}</div>
                      <div className="geo-facility-sub">{activeGeoSite.industry} · {geoSchematic?.subtitle || ""} · Assets {geoAssets.length}</div>
                    </div>
                    <div className="geo-facility-main">
                      <div className="geo-schematic-pane">
                        {activeSiteId === "alinta-hsdale" ? (
                          <WindSchematic assets={geoAssets} activeAssetId={activeAssetId} onAssetClick={setActiveAssetId} />
                        ) : (
                          <PIDSchematic
                            schematic={geoSchematic}
                            assets={geoAssets}
                            industry={activeGeoSite.industry}
                            activeAssetId={activeAssetId}
                            onAssetClick={setActiveAssetId}
                          />
                        )}
                      </div>
                      <AssetSidebar assets={geoAssets} activeAssetId={activeAssetId} onAssetClick={setActiveAssetId} />
                      {activeGeoAsset ? (
                        <GeoGeniePanel
                          asset={activeGeoAsset}
                          assets={geoAssets}
                          site={activeGeoSite}
                          industry={activeGeoSite.industry || industry}
                          currency={demoCurrency}
                          genieUrl={activeGeoGenieUrl}
                          onSelectAsset={setActiveAssetId}
                          onClose={() => setActiveAssetId(null)}
                        />
                      ) : null}
                    </div>
                    <GeoStatusBar
                      assets={geoAssets}
                      kpi={{
                        oee: geoAssets.length ? Math.round((geoAssets.filter((a) => a.status === "running").length / geoAssets.length) * 100) : 0,
                        mtbf: geoAssets.length ? Math.round(geoAssets.reduce((sum, a) => sum + Number(a.rul_hours || 0), 0) / geoAssets.length) : 0,
                        location: activeGeoSite.name
                      }}
                    />
                    {geoAssetLoading ? <div className="geo-loading-strip">Refreshing site assets...</div> : null}
                  </div>
                ) : (
                  <div className="geo-empty">Select a site from map above to open PID drill-down + Genie.</div>
                )}
              </div>
            </div>
          </div>
        </div>
        </div>
      </div>
    </>
  );
}
