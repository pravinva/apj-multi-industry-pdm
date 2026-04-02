import { useCallback, useEffect, useState } from "react";

export function useGeoData(activeIndustries, currency = "") {
  const [sites, setSites] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const refetch = useCallback(async () => {
    const industries = Array.from(activeIndustries || []);
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      if (industries.length) params.set("industries", industries.join(","));
      if (currency && currency !== "AUTO") params.set("currency", currency);
      const query = params.toString() ? `?${params.toString()}` : "";
      const res = await fetch(`/api/geo/sites${query}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      setSites(Array.isArray(payload?.sites) ? payload.sites : []);
    } catch (e) {
      setSites([]);
      setError(String(e?.message || "Failed to load geo sites"));
    } finally {
      setLoading(false);
    }
  }, [activeIndustries, currency]);

  useEffect(() => {
    refetch();
  }, [refetch]);

  return { sites, loading, error, refetch };
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
