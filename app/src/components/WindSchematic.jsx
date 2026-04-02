function windNodePositions() {
  const nodes = [];
  let n = 1;
  for (let row = 0; row < 4; row += 1) {
    for (let col = 0; col < 3; col += 1) {
      nodes.push({ id: `T-${String(n).padStart(2, "0")}`, x: 130 + col * 150, y: 90 + row * 86 });
      n += 1;
    }
  }
  return nodes;
}

function statusClass(status) {
  const s = String(status || "running").toLowerCase();
  return s === "critical" ? "critical" : s === "warning" ? "warning" : "running";
}

function statusByName(assets, includes) {
  const hit = (assets || []).find((a) => String(a?.name || "").toLowerCase().includes(includes));
  return statusClass(hit?.status);
}

export default function WindSchematic({ assets, activeAssetId, onAssetClick }) {
  const byId = new Map((assets || []).map((a) => [String(a.asset_id || a.equip_id), a]));
  const turbines = windNodePositions();
  const collectorStatus = statusByName(assets, "collector");
  const bessStatus = statusByName(assets, "bess");
  const hvStatus = statusByName(assets, "substation");
  const feederStatus = statusByName(assets, "feeder");
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
        <text x="820" y="30" className="geo-wind-header">WIND FARM SINGLE LINE  •  ALINTA ENERGY  •  LIVE</text>
        {turbines.map((t, idx) => {
          const asset = byId.get(t.id);
          const status = statusClass(asset?.status);
          const isActive = activeAssetId === asset?.asset_id;
          const row = Math.floor(idx / 3);
          const isLast = idx % 3 === 2;
          const showJunction = isLast;
          return (
            <g
              key={t.id}
              className={`geo-wind-node ${status} ${isActive ? "active" : ""}`}
              onClick={() => asset && onAssetClick?.(asset.asset_id)}
            >
              <circle className="geo-wind-node-ring" cx={t.x} cy={t.y} r="24" />
              <line x1={t.x} y1={t.y - 14} x2={t.x} y2={t.y + 11} className="geo-wind-blade" />
              <line x1={t.x - 12} y1={t.y + 7} x2={t.x + 12} y2={t.y - 7} className="geo-wind-blade" />
              <line x1={t.x - 12} y1={t.y - 7} x2={t.x + 12} y2={t.y + 7} className="geo-wind-blade" />
              <text x={t.x - 15} y={t.y + 39} className={`geo-wind-node-id ${isActive ? "active" : ""}`}>{t.id}</text>
              {isLast ? <path d={`M ${t.x + 24} ${t.y} L 620 ${t.y}`} className="geo-wind-branch" /> : null}
              {showJunction ? <circle cx="620" cy={t.y} r="4.5" className={`geo-wind-junction ${row === 3 ? "hot" : ""}`} /> : null}
            </g>
          );
        })}
        <path d="M620 90 L620 358" className="geo-wind-backbone" />
        <path d="M620 232 L680 232" className="geo-wind-backbone" markerEnd="url(#windArrow)" />
        <path d="M850 232 L910 232" className="geo-wind-backbone" markerEnd="url(#windArrow)" />
        <path d="M1080 232 L1132 232" className="geo-wind-backbone" markerEnd="url(#windArrow)" />

        <g className={`geo-wind-station ${collectorStatus}`}>
          <rect x="680" y="196" width="170" height="72" rx="10" />
          <text x="695" y="224" className="geo-wind-station-title">BESS / HPR</text>
          <text x="695" y="246" className="geo-wind-station-sub">150MW / 194MWh</text>
        </g>
        <g className={`geo-wind-station ${hvStatus}`}>
          <rect x="910" y="196" width="170" height="72" rx="10" />
          <text x="924" y="224" className="geo-wind-station-title">MV COLLECTOR FEEDER</text>
          <text x="924" y="246" className="geo-wind-station-sub">33kV</text>
        </g>
        <text x="1140" y="236" className="geo-wind-grid-label">NATIONAL GRID</text>
        <g className={`geo-wind-substation ${bessStatus}`}>
          <rect x="510" y="196" width="130" height="72" rx="10" />
          <text x="524" y="224" className="geo-wind-station-title small">MV COLLECTOR</text>
          <text x="524" y="243" className="geo-wind-station-sub">SUBSTATION</text>
        </g>
        <g className={`geo-wind-substation ${feederStatus}`}>
          <rect x="860" y="196" width="40" height="72" rx="8" />
        </g>
      </svg>
    </div>
  );
}
