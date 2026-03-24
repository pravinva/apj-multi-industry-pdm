import { useEffect, useMemo, useState } from "react";

const INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"];
const PAGES = [
  ["p1", "Fleet"],
  ["p2", "Asset"],
  ["p3", "Hierarchy"],
  ["p4", "Stream"],
  ["p5", "Model"],
  ["p6", "Simulator"]
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

export default function App() {
  const [industry, setIndustry] = useState("mining");
  const [page, setPage] = useState("p1");
  const [assets, setAssets] = useState([]);
  const [kpis, setKpis] = useState({ fleet_health_score: 0, critical_assets: 0, asset_count: 0 });
  const [stream, setStream] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const [a, k] = await Promise.all([
        getJson(`/api/fleet/assets?industry=${industry}`, []),
        getJson(`/api/fleet/kpis?industry=${industry}`, {})
      ]);
      if (!cancelled) {
        setAssets(a);
        setKpis(k);
        if (!selectedAsset && a.length) setSelectedAsset(a[0].equipment_id);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [industry, selectedAsset]);

  useEffect(() => {
    if (page !== "p4") return;
    let alive = true;
    const run = async () => {
      const data = await getJson(`/api/stream/latest?industry=${industry}&limit=50`, { rows: [] });
      if (alive) setStream(data.rows || []);
    };
    run();
    const t = setInterval(run, 5000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [page, industry]);

  const selected = useMemo(
    () => assets.find((a) => a.equipment_id === selectedAsset) || null,
    [assets, selectedAsset]
  );

  return (
    <div className="app">
      <header className="topbar">
        <strong>OT PdM Intelligence</strong>
        {INDUSTRIES.map((ind) => (
          <button
            key={ind}
            className="pill"
            style={{ opacity: ind === industry ? 1 : 0.65 }}
            onClick={() => setIndustry(ind)}
          >
            {ind}
          </button>
        ))}
      </header>
      <div className="body">
        <aside className="sidebar">
          {PAGES.map(([id, label]) => (
            <button key={id} className={`nav-btn ${page === id ? "active" : ""}`} onClick={() => setPage(id)}>
              {label}
            </button>
          ))}
        </aside>
        <main className="content">
          {page === "p1" && (
            <div className="row">
              <div className="row three">
                <div className="card">Fleet health: <span className="mono">{kpis.fleet_health_score}%</span></div>
                <div className="card">Critical assets: <span className="mono">{kpis.critical_assets}</span></div>
                <div className="card">Asset count: <span className="mono">{kpis.asset_count}</span></div>
              </div>
              <div className="card">
                <h3>Fleet overview</h3>
                <table className="table">
                  <thead>
                    <tr><th>Asset</th><th>Anomaly</th><th>RUL (h)</th><th>Health</th></tr>
                  </thead>
                  <tbody>
                    {assets.map((a) => (
                      <tr key={a.equipment_id} onClick={() => { setSelectedAsset(a.equipment_id); setPage("p2"); }}>
                        <td className="mono">{a.equipment_id}</td>
                        <td>{a.anomaly_score}</td>
                        <td>{a.rul_hours}</td>
                        <td>{a.health_score_pct}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {page === "p2" && (
            <div className="card">
              <h3>Asset drilldown</h3>
              {selected ? (
                <p className="mono">
                  {selected.equipment_id} | anomaly={selected.anomaly_score} | rul={selected.rul_hours}
                </p>
              ) : (
                <p>No asset selected.</p>
              )}
            </div>
          )}

          {page === "p3" && <div className="card"><h3>ISA-95 Hierarchy</h3><p>Hierarchy view scaffold is in place.</p></div>}

          {page === "p4" && (
            <div className="card">
              <h3>Sensor stream</h3>
              <table className="table">
                <thead>
                  <tr>
                    <th>Timestamp</th><th>Site</th><th>Area</th><th>Unit</th><th>Equipment</th><th>Tag</th><th>Value</th><th>Quality</th><th>Protocol</th>
                  </tr>
                </thead>
                <tbody>
                  {stream.map((r, i) => (
                    <tr key={`${r.equipment_id}-${r.tag_name}-${i}`}>
                      <td className="mono">{r.timestamp}</td><td>{r.site_id}</td><td>{r.area_id}</td><td>{r.unit_id}</td><td className="mono">{r.equipment_id}</td><td>{r.tag_name}</td><td>{r.value}</td><td>{r.quality}</td><td>{r.source_protocol}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {page === "p5" && <div className="card"><h3>Model explainability</h3><p>Model summary scaffold is in place.</p></div>}
          {page === "p6" && <div className="card"><h3>OT simulator</h3><p>Simulator controls scaffold is in place.</p></div>}
        </main>
      </div>
    </div>
  );
}
