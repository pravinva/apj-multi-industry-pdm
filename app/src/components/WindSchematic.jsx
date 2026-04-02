function statusClass(status) {
  const s = String(status || "running").toLowerCase();
  return s === "critical" ? "critical" : s === "warning" ? "warning" : "running";
}

function byType(assets, needle) {
  return (assets || []).filter((a) =>
    String(a?.type || "").toLowerCase().includes(needle)
  );
}

export default function WindSchematic({ assets, activeAssetId, onAssetClick }) {
  const all = Array.isArray(assets) ? assets : [];
  const turbines = byType(all, "wind").slice(0, 8);
  const bessAssets = byType(all, "bess");
  const transformerAssets = byType(all, "transformer");
  const nonMapped = all.filter(
    (a) =>
      !turbines.includes(a) &&
      !bessAssets.includes(a) &&
      !transformerAssets.includes(a)
  );

  const turbineNode = (idx) => {
    const col = idx % 2;
    const row = Math.floor(idx / 2);
    return { x: 120 + col * 170, y: 85 + row * 84 };
  };

  const substationList = [...bessAssets, ...transformerAssets, ...nonMapped].slice(0, 6);

  return (
    <div className="geo-schematic-wrap">
      <svg className="geo-schematic" viewBox="0 0 1200 460" preserveAspectRatio="xMinYMin meet">
        <defs>
          <pattern id="windGrid" width="32" height="32" patternUnits="userSpaceOnUse">
            <path d="M32 0 L0 0 0 32" fill="none" stroke="rgba(148,163,184,0.16)" strokeWidth="1" />
          </pattern>
          <marker id="windArrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <path d="M0,0 L8,3 L0,6 Z" fill="#d97706" />
          </marker>
        </defs>
        <rect x="0" y="0" width="1200" height="460" fill="#f8fafc" />
        <rect x="0" y="0" width="1200" height="460" fill="url(#windGrid)" />
        <text x="700" y="30" className="geo-wind-header">ENERGY SINGLE LINE  •  ALINTA ENERGY  •  LIVE ASSETS</text>
        {turbines.map((asset, idx) => {
          const t = turbineNode(idx);
          const status = statusClass(asset?.status);
          const isActive = activeAssetId === asset?.asset_id;
          const isSecondCol = idx % 2 === 1;
          return (
            <g
              key={asset.asset_id || idx}
              className={`geo-wind-node ${status} ${isActive ? "active" : ""}`}
              onClick={() => asset && onAssetClick?.(asset.asset_id)}
            >
              <circle className="geo-wind-node-ring" cx={t.x} cy={t.y} r="24" />
              <line x1={t.x} y1={t.y - 14} x2={t.x} y2={t.y + 11} className="geo-wind-blade" />
              <line x1={t.x - 12} y1={t.y + 7} x2={t.x + 12} y2={t.y - 7} className="geo-wind-blade" />
              <line x1={t.x - 12} y1={t.y - 7} x2={t.x + 12} y2={t.y + 7} className="geo-wind-blade" />
              <text x={t.x - 24} y={t.y + 39} className={`geo-wind-node-id ${isActive ? "active" : ""}`}>{asset.asset_id}</text>
              {isSecondCol ? <path d={`M ${t.x + 24} ${t.y} L 540 ${t.y}`} className="geo-wind-branch" /> : null}
              {isSecondCol ? <circle cx="540" cy={t.y} r="4.5" className="geo-wind-junction" /> : null}
            </g>
          );
        })}
        <path d="M540 85 L540 352" className="geo-wind-backbone" />
        <path d="M540 232 L618 232" className="geo-wind-backbone" markerEnd="url(#windArrow)" />

        {substationList.map((asset, idx) => {
          const x = 620 + Math.floor(idx / 3) * 240;
          const y = 92 + (idx % 3) * 98;
          const status = statusClass(asset?.status);
          const isActive = activeAssetId === asset?.asset_id;
          return (
            <g
              key={`station-${asset.asset_id || idx}`}
              className={`geo-wind-station ${status} ${isActive ? "active" : ""}`}
              onClick={() => onAssetClick?.(asset.asset_id)}
            >
              <rect x={x} y={y} width="210" height="72" rx="10" />
              <text x={x + 14} y={y + 30} className="geo-wind-station-title">{asset.asset_id}</text>
              <text x={x + 14} y={y + 52} className="geo-wind-station-sub">{String(asset.type || "").toUpperCase()}</text>
              <path d={`M 540 232 L ${x} ${y + 36}`} className="geo-wind-branch" />
            </g>
          );
        })}
        <text x="1090" y="236" className="geo-wind-grid-label">GRID</text>
      </svg>
    </div>
  );
}
