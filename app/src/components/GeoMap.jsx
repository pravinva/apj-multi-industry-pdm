import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import Map, { Marker, NavigationControl, Popup } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

const INDUSTRY_META = {
  mining: { color: "#B45309", icon: "⛏️" },
  water: { color: "#0284C7", icon: "💧" },
  energy: { color: "#65A30D", icon: "⚡" },
  automotive: { color: "#7C3AED", icon: "🚗" },
  semiconductor: { color: "#0F766E", icon: "🧩" }
};

const APJ_BOUNDS = [
  [65, -30],
  [155, 45]
];

export default function GeoMap({
  sites,
  onSiteClick,
  activeSiteId,
  activeIndustries,
  onToggleIndustry,
  mapLayer,
  onMapLayerChange
}) {
  const [hoverSite, setHoverSite] = useState(null);
  const [mapFatal, setMapFatal] = useState(false);
  const [mapLoaded, setMapLoaded] = useState(false);
  const mapRef = useRef(null);
  const style = useMemo(() => {
    const osmStandard = {
      version: 8,
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          attribution: "© OpenStreetMap contributors"
        }
      },
      layers: [{ id: "osm-standard", type: "raster", source: "osm" }]
    };
    const openTopo = {
      version: 8,
      sources: {
        topo: {
          type: "raster",
          tiles: [
            "https://a.tile.opentopomap.org/{z}/{x}/{y}.png",
            "https://b.tile.opentopomap.org/{z}/{x}/{y}.png",
            "https://c.tile.opentopomap.org/{z}/{x}/{y}.png"
          ],
          tileSize: 256,
          attribution: "© OpenStreetMap contributors, SRTM | Map style: OpenTopoMap"
        }
      },
      layers: [{ id: "osm-topo", type: "raster", source: "topo" }]
    };
    return mapLayer === "satellite" ? osmStandard : openTopo;
  }, [mapLayer]);
  const filtered = useMemo(
    () => (sites || []).filter((s) => activeIndustries.has(String(s.industry))),
    [sites, activeIndustries]
  );
  const showFallback = mapFatal;
  const fitMapToApjSites = (focusSiteId = "") => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const focused = filtered.find((s) => String(s.site_id) === String(focusSiteId));
    if (focused) {
      map.flyTo({
        center: [Number(focused.lng), Number(focused.lat)],
        zoom: Math.max(5.2, map.getZoom() || 0),
        duration: 650
      });
      return;
    }
    if (!filtered.length) return;
    if (filtered.length === 1) {
      map.flyTo({
        center: [Number(filtered[0].lng), Number(filtered[0].lat)],
        zoom: 5.0,
        duration: 650
      });
      return;
    }
    let minLng = 180;
    let maxLng = -180;
    let minLat = 90;
    let maxLat = -90;
    for (const s of filtered) {
      const lng = Number(s.lng);
      const lat = Number(s.lat);
      minLng = Math.min(minLng, lng);
      maxLng = Math.max(maxLng, lng);
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
    }
    const lngPad = Math.max(6, (maxLng - minLng) * 0.25);
    const latPad = Math.max(4, (maxLat - minLat) * 0.3);
    minLng = Math.max(APJ_BOUNDS[0][0], minLng - lngPad);
    maxLng = Math.min(APJ_BOUNDS[1][0], maxLng + lngPad);
    minLat = Math.max(APJ_BOUNDS[0][1], minLat - latPad);
    maxLat = Math.min(APJ_BOUNDS[1][1], maxLat + latPad);
    map.fitBounds(
      [
        [minLng, minLat],
        [maxLng, maxLat]
      ],
      {
        padding: { top: 84, right: 52, bottom: 52, left: 52 },
        duration: 700,
        maxZoom: 6.2
      }
    );
  };
  useEffect(() => {
    if (!mapLoaded || showFallback) return;
    fitMapToApjSites(activeSiteId);
  }, [mapLoaded, activeSiteId, filtered, showFallback]);
  return (
    <div className="geo-map-wrap">
      <div className="geo-map-toolbar">
        <div className="geo-chip-row">
          {Object.keys(INDUSTRY_META).map((ind) => (
            <button key={ind} className={`geo-chip ${activeIndustries.has(ind) ? "active" : ""}`} onClick={() => onToggleIndustry(ind)}>
              {ind}
            </button>
          ))}
        </div>
        <div className="geo-layer-toggle">
          <button onClick={() => fitMapToApjSites()}>Focus APJ</button>
          <button className={mapLayer === "satellite" ? "active" : ""} onClick={() => onMapLayerChange("satellite")}>Satellite</button>
          <button className={mapLayer === "terrain" ? "active" : ""} onClick={() => onMapLayerChange("terrain")}>Topology</button>
        </div>
      </div>
      {showFallback ? (
        <div className="geo-map-fallback">
          <div className="geo-map-fallback-title">Map preview unavailable</div>
          <div className="geo-map-fallback-sub">
            Unable to load map tiles right now. Falling back to site list view.
          </div>
          <div className="geo-site-fallback-grid">
            {filtered.map((site) => (
              <button
                key={site.site_id}
                className={`geo-site-fallback-card ${activeSiteId === site.site_id ? "active" : ""}`}
                onClick={() => onSiteClick(site.site_id)}
              >
                <div className="geo-site-fallback-name">{site.customer} · {site.name}</div>
                <div className="geo-site-fallback-meta">{site.industry} · {site.lat}, {site.lng}</div>
                <div className="geo-site-fallback-counts">
                  Running {site.asset_counts?.running || 0} | Warning {site.asset_counts?.warning || 0} | Critical {site.asset_counts?.critical || 0}
                </div>
              </button>
            ))}
          </div>
        </div>
      ) : (
        <>
          <Map
            ref={mapRef}
            mapLib={maplibregl}
            initialViewState={{ longitude: 112, latitude: 6, zoom: 4.1 }}
            maxBounds={APJ_BOUNDS}
            minZoom={2.6}
            maxZoom={10.5}
            scrollZoom
            dragPan
            touchZoomRotate
            doubleClickZoom
            dragRotate={false}
            touchPitch={false}
            mapStyle={style}
            reuseMaps
            onLoad={() => setMapLoaded(true)}
            onError={() => setMapFatal(true)}
          >
            <NavigationControl position="bottom-right" visualizePitch />
            {filtered.map((site) => {
              const meta = INDUSTRY_META[site.industry] || INDUSTRY_META.mining;
              const counts = site.asset_counts || {};
              const hasCrit = Number(counts.critical || 0) > 0;
              const hasWarn = !hasCrit && Number(counts.warning || 0) > 0;
              return (
                <Marker key={site.site_id} longitude={Number(site.lng)} latitude={Number(site.lat)} anchor="center">
                  <button
                    className={`geo-site-marker ${activeSiteId === site.site_id ? "active" : ""}`}
                    style={{ borderColor: meta.color }}
                    onMouseEnter={() => setHoverSite(site)}
                    onMouseLeave={() => setHoverSite(null)}
                    onClick={() => onSiteClick(site.site_id)}
                  >
                    <span>{meta.icon}</span>
                    {(hasCrit || hasWarn) ? <span className={`geo-marker-badge ${hasCrit ? "critical" : "warning"}`} /> : null}
                  </button>
                </Marker>
              );
            })}
            {hoverSite ? (
              <Popup longitude={Number(hoverSite.lng)} latitude={Number(hoverSite.lat)} closeButton={false} closeOnClick={false} anchor="top">
                <div className="geo-popup">
                  <div className="geo-popup-title">{hoverSite.customer} · {hoverSite.name}</div>
                  <div>Running {hoverSite.asset_counts?.running || 0} | Warning {hoverSite.asset_counts?.warning || 0} | Critical {hoverSite.asset_counts?.critical || 0}</div>
                  {hoverSite.top_alert ? <div className="geo-popup-alert">{hoverSite.top_alert.message}</div> : null}
                </div>
              </Popup>
            ) : null}
          </Map>
          {mapLayer === "terrain" ? <div className="geo-terrain-overlay" /> : null}
        </>
      )}
    </div>
  );
}
