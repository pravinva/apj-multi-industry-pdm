function statusCount(assets, state) {
  return (assets || []).filter((a) => String(a?.status || "").toLowerCase() === state).length;
}

export default function GeoStatusBar({ assets, kpi }) {
  const running = statusCount(assets, "running");
  const warning = statusCount(assets, "warning");
  const critical = statusCount(assets, "critical");
  return (
    <div className="geo-statusbar">
      <span className="dot running" /> Running {running}
      <span className="dot warning" /> Warning {warning}
      <span className="dot critical" /> Critical {critical}
      <span className="geo-status-sep">|</span>
      <span className="geo-status-kpi">OEE {kpi?.oee ?? "--"}%</span>
      <span className="geo-status-kpi">Avg MTBF {kpi?.mtbf ?? "--"}h</span>
      <span className="geo-status-kpi">{kpi?.location || "—"}</span>
    </div>
  );
}
