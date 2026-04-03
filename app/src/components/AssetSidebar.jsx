function formatExposure(asset, fallbackCurrency = "") {
  const exposure = Number(asset?.exposure_value || 0);
  if (!Number.isFinite(exposure) || exposure <= 0) return "-";
  const currency = String(asset?.financial?.currency || fallbackCurrency || "").toUpperCase();
  if (!currency) return exposure.toLocaleString();
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency,
      maximumFractionDigits: 0,
    }).format(exposure);
  } catch {
    return `${currency} ${Math.round(exposure).toLocaleString()}`;
  }
}

export default function AssetSidebar({ assets, activeAssetId, onAssetClick, currency = "" }) {
  const total = Array.isArray(assets) ? assets.length : 0;
  const warningCount = (assets || []).filter((a) => String(a?.status || "").toLowerCase() === "warning").length;
  const criticalCount = (assets || []).filter((a) => String(a?.status || "").toLowerCase() === "critical").length;
  const sorted = [...(assets || [])].sort((a, b) => {
    const rank = { critical: 2, warning: 1, running: 0 };
    const ra = rank[String(a?.status || "running").toLowerCase()] || 0;
    const rb = rank[String(b?.status || "running").toLowerCase()] || 0;
    if (ra !== rb) return rb - ra;
    return Number(b?.anomaly_score || 0) - Number(a?.anomaly_score || 0);
  });
  return (
    <div className="geo-asset-sidebar">
      <div className="geo-asset-sidebar-hdr">
        <div className="geo-asset-sidebar-title">Monitored Assets</div>
        <div className="geo-asset-sidebar-meta">
          {total} total · {warningCount} warning · {criticalCount} critical
        </div>
      </div>
      {sorted.map((asset) => {
        const status = String(asset?.status || "running").toLowerCase();
        const badge = status === "critical" ? "CRITICAL" : status === "warning" ? "WARNING" : "";
        const pct = Math.max(1, Math.min(99, Math.round((1 - Number(asset?.confidence || 0)) * 100)));
        return (
          <button
            key={asset.asset_id}
            className={`geo-asset-card ${status} ${activeAssetId === asset.asset_id ? "active" : ""}`}
            onClick={() => onAssetClick?.(asset.asset_id)}
          >
            <div className={`geo-asset-ring ${status}`}>
              <span>{pct}%</span>
            </div>
            <div className="geo-asset-main">
              <div className="geo-asset-top">
                <span className="geo-asset-name">{asset.asset_id || asset.name}</span>
                {badge ? <span className={`geo-asset-badge ${status}`}>{badge}</span> : null}
              </div>
              <div className="geo-asset-type">{asset.type || "-"}</div>
              <div className="geo-asset-crumb">{asset.crumb || ""}</div>
              <div className="geo-asset-metrics">
                <span>ANOMALY {Number(asset?.anomaly_score || 0).toFixed(2)}</span>
                <span>RUL {Number(asset?.rul_hours || 0).toFixed(1)}h</span>
                <span>EXP {formatExposure(asset, currency)}</span>
              </div>
            </div>
          </button>
        );
      })}
    </div>
  );
}
