import { useEffect, useMemo, useRef, useState } from "react";
import Sparkline from "./Sparkline";

function formatNumber(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString();
}

export default function GeoGeniePanel({ asset, assets, site, industry, currency, genieUrl, onSelectAsset, onClose }) {
  const [messages, setMessages] = useState([]);
  const [inputValue, setInputValue] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [actionDone, setActionDone] = useState("");
  const inputRef = useRef(null);
  const suggestions = useMemo(() => (Array.isArray(asset?.suggestions) ? asset.suggestions.slice(0, 3) : []), [asset]);
  const drillAssets = useMemo(() => (Array.isArray(assets) ? assets : []), [assets]);
  const drillIdx = useMemo(
    () => drillAssets.findIndex((a) => a.asset_id === asset?.asset_id),
    [drillAssets, asset?.asset_id]
  );
  const prevAsset = drillIdx > 0 ? drillAssets[drillIdx - 1] : null;
  const nextAsset = drillIdx >= 0 && drillIdx < drillAssets.length - 1 ? drillAssets[drillIdx + 1] : null;
  const isJpy = String(currency || "").toUpperCase() === "JPY";

  useEffect(() => {
    if (!asset || !site) return;
    setMessages([
      {
        role: "assistant",
        text: isJpy
          ? `${site.customer} ${site.name} の ${asset.name} を調査できます。根本原因、リスク、対応順序を質問してください。金額は JPY で回答します。`
          : `I can investigate ${asset.name} at ${site.customer} ${site.name}. Ask for root cause, risk, and action sequencing.${currency && currency !== "AUTO" ? ` I will respond with costs in ${currency}.` : ""}`
      }
    ]);
    setInputValue("");
    setActionDone("");
  }, [asset, site, currency, isJpy]);

  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`;
  }, [inputValue]);

  function selectNeighbor(target) {
    if (!target?.asset_id) return;
    onSelectAsset?.(target.asset_id);
  }

  async function ask() {
    const q = inputValue.trim();
    if (!q || isLoading) return;
    setMessages((prev) => [...prev, { role: "user", text: q }]);
    setInputValue("");
    setIsLoading(true);
    try {
      const res = await fetch("/api/geo/genie/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          industry,
          currency,
          question: q,
          asset_context: { name: asset?.name, status: asset?.status, tags: asset?.tags || [] }
        })
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        const permissionHint = res.status === 401 || res.status === 403 ? " Check Genie room config in genie_rooms.json" : "";
        throw new Error(`${payload?.detail || "Request failed"}.${permissionHint}`.trim());
      }
      setMessages((prev) => [...prev, { role: "assistant", text: payload?.answer || "No answer returned." }]);
    } catch (e) {
      setMessages((prev) => [...prev, { role: "assistant", text: `Error: ${String(e?.message || "Unable to query Genie")}` }]);
    } finally {
      setIsLoading(false);
    }
  }

  async function act(action) {
    if (!asset?.active_alert || actionDone) return;
    setActionDone("pending");
    try {
      const res = await fetch("/api/geo/alert/action", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ asset_id: asset.asset_id, industry, action })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setActionDone(action);
    } catch {
      setActionDone("error");
    }
  }

  if (!asset || !site) return null;
  return (
    <aside className="geo-genie-panel">
      <div className="geo-genie-header">
        <div className="geo-genie-title-row">
          <span className="geo-industry-badge">{industry}</span>
          <button className="geo-close-btn" onClick={onClose}>Close</button>
        </div>
        <div className="geo-genie-asset">{asset.name}</div>
        <div className="geo-genie-sub">{site.customer} · {site.name}</div>
        {genieUrl ? (
          <a className="geo-genie-link" href={genieUrl} target="_blank" rel="noreferrer">
            Open Genie conversation
          </a>
        ) : null}
        <div className="geo-status-pills">
          <span className={`pill ${asset.status}`}>{String(asset.status || "").toUpperCase()}</span>
          {String(asset.data_source || "").toUpperCase() === "SIMULATOR" ? <span className="pill sim">Simulated fault active</span> : null}
        </div>
      </div>
      <div className="geo-drill-row">
        <button className="geo-drill-btn" onClick={() => selectNeighbor(prevAsset)} disabled={!prevAsset}>{isJpy ? "前へ" : "Prev"}</button>
        <select
          className="geo-drill-select"
          value={asset.asset_id || ""}
          onChange={(e) => onSelectAsset?.(e.target.value)}
        >
          {drillAssets.map((a) => (
            <option key={a.asset_id} value={a.asset_id}>
              {a.asset_id} · {a.name}
            </option>
          ))}
        </select>
        <button className="geo-drill-btn" onClick={() => selectNeighbor(nextAsset)} disabled={!nextAsset}>{isJpy ? "次へ" : "Next"}</button>
      </div>
      <div className="geo-drill-meta-grid">
        <div className="geo-drill-meta-cell"><span>{isJpy ? "資産ID" : "Asset ID"}</span><strong>{asset.asset_id || "-"}</strong></div>
        <div className="geo-drill-meta-cell"><span>{isJpy ? "タイプ" : "Type"}</span><strong>{asset.type || "-"}</strong></div>
        <div className="geo-drill-meta-cell"><span>{isJpy ? "RUL (時間)" : "RUL (hrs)"}</span><strong>{formatNumber(asset.rul_hours)}</strong></div>
        <div className="geo-drill-meta-cell"><span>{isJpy ? "信頼度" : "Confidence"}</span><strong>{formatNumber(Math.round(Number(asset.confidence || 0) * 100))}%</strong></div>
      </div>
      {asset.top_alert ? (
        <div className="geo-drill-alert">
          <div className="geo-drill-alert-title">{isJpy ? "最重要アラート" : "Top alert"}</div>
          <div>{asset.top_alert.message}</div>
        </div>
      ) : null}
      <div className="geo-tag-grid">
        {(asset.tags || []).slice(0, 6).map((tag) => (
          <div className="geo-tag-cell" key={tag.name}>
            <div className="geo-tag-name">{tag.name}</div>
            <div className="geo-tag-val">{tag.value}</div>
            <Sparkline history={tag.history_24h} trend={tag.trend} />
          </div>
        ))}
      </div>
      {asset.financial ? (
        <div className="geo-fin-row">
          Avoided {asset.financial.currency} {Math.round(asset.financial.avoided_cost || 0).toLocaleString()} vs Intervention {asset.financial.currency} {Math.round(asset.financial.intervention_cost || 0).toLocaleString()}
        </div>
      ) : null}
      <div className="geo-ai-top-block">
        <div className="geo-suggestions">
          {suggestions.map((s, idx) => (
            <button key={`${idx}`} onClick={() => setInputValue(s)}>{s}</button>
          ))}
        </div>
        <div className="geo-input-row">
          <textarea
            ref={inputRef}
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            placeholder={isJpy ? "この資産について AI に質問..." : "Ask AI about this asset..."}
            rows={1}
          />
          <button onClick={ask} disabled={isLoading}>{isJpy ? "質問" : "Ask"}</button>
        </div>
        {asset.active_alert ? (
          <div className="geo-action-row">
            <button disabled={!!actionDone} onClick={() => act("approve")}>{isJpy ? "承認" : "Approve"}</button>
            <button disabled={!!actionDone} onClick={() => act("defer")}>{isJpy ? "保留" : "Defer"}</button>
            <button disabled={!!actionDone} onClick={() => act("reject")}>{isJpy ? "却下" : "Reject"}</button>
            {actionDone && actionDone !== "pending" ? <span className="geo-action-confirm">{isJpy ? `保存済み: ${actionDone}` : `Action saved: ${actionDone}`}</span> : null}
          </div>
        ) : null}
      </div>
      <div className="geo-chat-area">
        {messages.map((m, idx) => (
          <div key={`${m.role}-${idx}`} className={`geo-chat-msg ${m.role}`}>{m.text}</div>
        ))}
        {isLoading ? <div className="geo-chat-msg assistant">Thinking...</div> : null}
      </div>
    </aside>
  );
}
