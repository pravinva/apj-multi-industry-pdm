import { useCallback, useEffect, useRef, useState } from "react";

const ALL_GEO_INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"];

function mergeSites(prev, chunk) {
  const byId = new Map((prev || []).map((s) => [String(s.site_id), s]));
  for (const s of chunk || []) {
    if (s && s.site_id != null) byId.set(String(s.site_id), s);
  }
  return Array.from(byId.values()).sort((a, b) => {
    const ia = String(a.industry || "").localeCompare(String(b.industry || ""));
    if (ia !== 0) return ia;
    return String(a.site_id || "").localeCompare(String(b.site_id || ""));
  });
}

/**
 * Loads geo sites per industry in parallel; merges as each response arrives.
 * - sitesLoading: true while any industry request is still in flight
 * - loading: true only until the first sites appear or all requests finish (empty / failure)
 */
export function useGeoData(activeIndustries, currency = "", enabled = true) {
  const [sites, setSites] = useState([]);
  const [loading, setLoading] = useState(false);
  const [sitesLoading, setSitesLoading] = useState(false);
  const [error, setError] = useState("");
  const loadGenRef = useRef(0);

  const runLoad = useCallback(async () => {
    const gen = ++loadGenRef.current;
    const picked = Array.from(activeIndustries || []).sort();
    const list = picked.length ? picked : [...ALL_GEO_INDUSTRIES];
    const n = list.length;

    setLoading(true);
    setSitesLoading(n > 0);
    setError("");
    setSites([]);
    const cur = currency && currency !== "AUTO" ? currency : "";

    const failures = [];
    let pending = n;

    const finishOne = () => {
      if (loadGenRef.current !== gen) return;
      pending -= 1;
      setSitesLoading(pending > 0);
      if (pending === 0) {
        setLoading(false);
        if (failures.length === n) {
          setSites([]);
          setError("Failed to load geo sites");
        } else if (failures.length > 0) {
          setError("Some industries could not be loaded");
        } else {
          setError("");
        }
      }
    };

    await Promise.all(
      list.map(async (ind) => {
        try {
          const params = new URLSearchParams();
          params.set("industries", ind);
          if (cur) params.set("currency", cur);
          const res = await fetch(`/api/geo/sites?${params.toString()}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const payload = await res.json();
          const chunk = Array.isArray(payload?.sites) ? payload.sites : [];
          if (loadGenRef.current !== gen) return;
          setSites((prev) => mergeSites(prev, chunk));
          if (chunk.length > 0) setLoading(false);
        } catch (e) {
          failures.push(e);
        } finally {
          finishOne();
        }
      })
    );
  }, [activeIndustries, currency]);

  useEffect(() => {
    if (!enabled) {
      setSites([]);
      setLoading(false);
      setSitesLoading(false);
      setError("");
      return;
    }
    runLoad();
  }, [runLoad, enabled]);

  return { sites, loading, sitesLoading, error, refetch: runLoad };
}

export function useAssetData(siteId, currency = "") {
  const [assets, setAssets] = useState([]);
  const [schematic, setSchematic] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!siteId) {
      setAssets([]);
      setSchematic(null);
      setLoading(false);
      return;
    }
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      try {
        const currencyQuery = currency && currency !== "AUTO"
          ? `?currency=${encodeURIComponent(currency)}`
          : "";
        const [assetsRes, schematicRes] = await Promise.all([
          fetch(`/api/geo/assets/${encodeURIComponent(siteId)}${currencyQuery}`),
          fetch(`/api/geo/schematic/${encodeURIComponent(siteId)}${currencyQuery}`)
        ]);
        const assetsPayload = assetsRes.ok ? await assetsRes.json() : { assets: [] };
        const schematicPayload = schematicRes.ok ? await schematicRes.json() : null;
        if (!cancelled) {
          setAssets(Array.isArray(assetsPayload?.assets) ? assetsPayload.assets : []);
          setSchematic(schematicPayload || null);
        }
      } catch {
        if (!cancelled) {
          setAssets([]);
          setSchematic(null);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [siteId, currency]);

  return { assets, schematic, loading };
}
