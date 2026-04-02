function statusClass(status) {
  const s = String(status || "running").toLowerCase();
  return s === "critical" ? "critical" : s === "warning" ? "warning" : "running";
}

function formatStatus(status) {
  return String(status || "running").replace(/^\w/, (m) => m.toUpperCase());
}

function buildLayoutNodes(schematic, assets) {
  const inputNodes = Array.isArray(schematic?.nodes) ? schematic.nodes : [];
  const safeAssets = Array.isArray(assets) ? assets : [];
  if (!safeAssets.length) return inputNodes;

  const cols = 5;
  const startX = 60;
  const startY = 96;
  const xStep = 220;
  const yStep = 160;
  const w = 170;
  const h = 56;

  return safeAssets.map((asset, idx) => {
    const row = Math.floor(idx / cols);
    const col = idx % cols;
    const fallback = inputNodes[idx] || {};
    return {
      id: `asset-${asset.asset_id || idx}`,
      label: String(asset.asset_id || fallback.label || `A-${idx + 1}`),
      equip_id: String(asset.equip_id || asset.asset_id || fallback.equip_id || ""),
      x: Number(fallback.x ?? (startX + col * xStep)),
      y: Number(fallback.y ?? (startY + row * yStep)),
      w: Number(fallback.w ?? w),
      h: Number(fallback.h ?? h),
    };
  });
}

function buildLayoutPipes(nodes, schematic) {
  const template = Array.isArray(schematic?.pipes) ? schematic.pipes : [];
  if (nodes.length <= 1) return template;
  const generated = [];
  for (let i = 0; i < nodes.length - 1; i += 1) {
    generated.push({ from: nodes[i].id, to: nodes[i + 1].id });
  }
  return generated;
}

function nodeBox(node) {
  const w = Number(node?.w || 150) + 26;
  const h = 76;
  const x = Number(node?.x || 0) - 10;
  const y = Number(node?.y || 0) - 10;
  return { x, y, w, h };
}

export default function PIDSchematic({ schematic, assets, activeAssetId, onAssetClick }) {
  const assetByEquip = new Map((assets || []).map((a) => [String(a.equip_id || a.asset_id), a]));
  const nodes = buildLayoutNodes(schematic, assets);
  const pipes = buildLayoutPipes(nodes, schematic);
  const nodesById = new Map(nodes.map((n) => [n.id, n]));
  return (
    <div className="geo-schematic-wrap">
      <svg className="geo-schematic" viewBox="0 0 1200 460" preserveAspectRatio="xMinYMin meet">
        <defs>
          <pattern id="geoGrid" width="20" height="20" patternUnits="userSpaceOnUse">
            <path d="M20 0 L0 0 0 20" fill="none" stroke="rgba(148,163,184,0.2)" strokeWidth="1" />
          </pattern>
          <marker id="geoArrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <path d="M0,0 L8,3 L0,6 Z" fill="#d97706" />
          </marker>
          <filter id="geoNodeShadow" x="-20%" y="-20%" width="140%" height="160%">
            <feDropShadow dx="0" dy="2" stdDeviation="2" floodColor="#0f172a" floodOpacity="0.12" />
          </filter>
        </defs>
        <rect x="0" y="0" width="1200" height="460" fill="#fcfdff" />
        <rect x="0" y="0" width="1200" height="460" fill="url(#geoGrid)" />
        <text x="20" y="24" className="geo-pid-header">{String(schematic?.subtitle || "Process flow")}</text>
        {pipes.map((p, idx) => {
          const from = nodesById.get(p.from);
          const to = nodesById.get(p.to);
          if (!from || !to) return null;
          const fromBox = nodeBox(from);
          const toBox = nodeBox(to);
          const x1 = fromBox.x + fromBox.w;
          const y1 = fromBox.y + fromBox.h / 2;
          const x2 = toBox.x;
          const y2 = toBox.y + toBox.h / 2;
          const mid = x1 + (x2 - x1) / 2;
          const d = `M ${x1} ${y1} L ${mid} ${y1} L ${mid} ${y2} L ${x2} ${y2}`;
          return (
            <g key={`pipe-${idx}`}>
              <path d={d} className="geo-pipe-back" />
              <path d={d} className="geo-pipe" markerEnd="url(#geoArrow)" />
            </g>
          );
        })}
        {nodes.map((node) => {
          const asset = assetByEquip.get(String(node.equip_id || ""));
          const clickable = Boolean(asset);
          const status = statusClass(asset?.status);
          const isActive = activeAssetId === asset?.asset_id;
          const statusLabel = formatStatus(asset?.status);
          const box = nodeBox(node);
          return (
            <g
              key={node.id}
              className={`geo-node ${clickable ? "live" : "ghost"} ${status} ${isActive ? "active" : ""}`}
              onClick={() => clickable && onAssetClick?.(asset.asset_id)}
            >
              <rect x={box.x} y={box.y} width={box.w} height={box.h} rx="12" filter="url(#geoNodeShadow)" />
              <rect className={`geo-node-accent ${status}`} x={box.x} y={box.y} width="7" height={box.h} rx="3" />
              <text className="geo-node-title" x={box.x + 14} y={box.y + 22}>{node.label}</text>
              {clickable ? (
                <>
                  <text className="geo-node-meta" x={box.x + 14} y={box.y + 39}>
                    {asset.asset_id}  {String(asset.type || "").slice(0, 12)}
                  </text>
                  <text className="geo-node-kpi" x={box.x + 14} y={box.y + box.h - 10}>
                    A {Number(asset.anomaly_score || 0).toFixed(2)}  •  RUL {Number(asset.rul_hours || 0).toFixed(1)}h
                  </text>
                  <rect className={`geo-node-status-pill ${status}`} x={box.x + box.w - 86} y={box.y + 8} width="74" height="16" rx="8" />
                  <text className="geo-node-status-text" x={box.x + box.w - 49} y={box.y + 20}>
                    {statusLabel}
                  </text>
                </>
              ) : null}
              {clickable ? <circle className="geo-node-dot" cx={box.x + box.w - 16} cy={box.y + box.h - 12} r="4.5" /> : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
