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
  const resolvedCurrency = String(
    currency && currency !== "AUTO" ? currency : asset?.financial?.currency || ""
  ).toUpperCase();
  const isJpy = resolvedCurrency === "JPY";
  const isKrw = resolvedCurrency === "KRW";
  const locale = isJpy ? "ja" : isKrw ? "ko" : "en";

  useEffect(() => {
    if (!asset || !site) return;
    setMessages([
      {
        role: "assistant",
        text: locale === "ja"
          ? `${site.customer} ${site.name} の ${asset.name} を調査できます。根本原因、リスク、対応順序を質問してください。金額は JPY で回答します。`
          : locale === "ko"
          ? `${site.customer} ${site.name}의 ${asset.name} 자산을 조사할 수 있습니다. 근본 원인, 위험도, 조치 순서를 질문해 주세요. 금액은 KRW로 답변합니다.`
          : `I can investigate ${asset.name} at ${site.customer} ${site.name}. Ask for root cause, risk, and action sequencing.${resolvedCurrency ? ` I will respond with costs in ${resolvedCurrency}.` : ""}`
      }
    ]);
    setInputValue("");
    setActionDone("");
  }, [asset, site, resolvedCurrency, locale]);

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
          site_id: site?.site_id || "",
          currency: resolvedCurrency || currency,
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
          <button className="geo-close-btn" onClick={onClose}>{locale === "ja" ? "閉じる" : locale === "ko" ? "닫기" : "Close"}</button>
        </div>
        <div className="geo-genie-asset">{asset.name}</div>
        <div className="geo-genie-sub">{site.customer} · {site.name}</div>
        {genieUrl ? (
          <a className="geo-genie-link" href={genieUrl} target="_blank" rel="noreferrer">
            {locale === "ja" ? "Genie 会話を開く" : locale === "ko" ? "Genie 대화 열기" : "Open Genie conversation"}
          </a>
        ) : null}
        <div className="geo-status-pills">
          <span className={`pill ${asset.status}`}>{String(asset.status || "").toUpperCase()}</span>
          {String(asset.data_source || "").toUpperCase() === "SIMULATOR" ? <span className="pill sim">{locale === "ja" ? "シミュレータ故障注入中" : locale === "ko" ? "시뮬레이터 장애 주입 활성" : "Simulated fault active"}</span> : null}
        </div>
      </div>
      <div className="geo-drill-row">
        <button className="geo-drill-btn" onClick={() => selectNeighbor(prevAsset)} disabled={!prevAsset}>{locale === "ja" ? "前へ" : locale === "ko" ? "이전" : "Prev"}</button>
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
        <button className="geo-drill-btn" onClick={() => selectNeighbor(nextAsset)} disabled={!nextAsset}>{locale === "ja" ? "次へ" : locale === "ko" ? "다음" : "Next"}</button>
      </div>
      <div className="geo-drill-meta-grid">
        <div className="geo-drill-meta-cell"><span>{locale === "ja" ? "資産ID" : locale === "ko" ? "자산 ID" : "Asset ID"}</span><strong>{asset.asset_id || "-"}</strong></div>
        <div className="geo-drill-meta-cell"><span>{locale === "ja" ? "タイプ" : locale === "ko" ? "유형" : "Type"}</span><strong>{asset.type || "-"}</strong></div>
        <div className="geo-drill-meta-cell"><span>{locale === "ja" ? "RUL (時間)" : locale === "ko" ? "RUL (시간)" : "RUL (hrs)"}</span><strong>{formatNumber(asset.rul_hours)}</strong></div>
        <div className="geo-drill-meta-cell"><span>{locale === "ja" ? "信頼度" : locale === "ko" ? "신뢰도" : "Confidence"}</span><strong>{formatNumber(Math.round(Number(asset.confidence || 0) * 100))}%</strong></div>
      </div>
      {asset.top_alert ? (
        <div className="geo-drill-alert">
          <div className="geo-drill-alert-title">{locale === "ja" ? "最重要アラート" : locale === "ko" ? "최상위 알림" : "Top alert"}</div>
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
            placeholder={locale === "ja" ? "この資産について AI に質問..." : locale === "ko" ? "이 자산에 대해 AI에게 질문..." : "Ask AI about this asset..."}
            rows={1}
          />
          <button onClick={ask} disabled={isLoading}>{locale === "ja" ? "質問" : locale === "ko" ? "질문" : "Ask"}</button>
        </div>
        {asset.active_alert ? (
          <div className="geo-action-row">
            <button disabled={!!actionDone} onClick={() => act("approve")}>{locale === "ja" ? "承認" : locale === "ko" ? "승인" : "Approve"}</button>
            <button disabled={!!actionDone} onClick={() => act("defer")}>{locale === "ja" ? "保留" : locale === "ko" ? "보류" : "Defer"}</button>
            <button disabled={!!actionDone} onClick={() => act("reject")}>{locale === "ja" ? "却下" : locale === "ko" ? "거절" : "Reject"}</button>
            {actionDone && actionDone !== "pending" ? <span className="geo-action-confirm">{locale === "ja" ? `保存済み: ${actionDone}` : locale === "ko" ? `저장됨: ${actionDone}` : `Action saved: ${actionDone}`}</span> : null}
          </div>
        ) : null}
      </div>
      <div className="geo-chat-area">
        {messages.map((m, idx) => (
          <div key={`${m.role}-${idx}`} className={`geo-chat-msg ${m.role}`}>{m.text}</div>
        ))}
        {isLoading ? <div className="geo-chat-msg assistant">{locale === "ja" ? "考え中..." : locale === "ko" ? "생각 중..." : "Thinking..."}</div> : null}
      </div>
    </aside>
  );
}
