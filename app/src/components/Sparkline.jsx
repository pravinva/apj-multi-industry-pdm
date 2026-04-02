export default function Sparkline({ history, trend, width = 64, height = 20 }) {
  const values = Array.isArray(history) ? history.filter((v) => Number.isFinite(Number(v))).map((v) => Number(v)) : [];
  const pts = values.length ? values : [0.5, 0.5];
  const min = Math.min(...pts);
  const max = Math.max(...pts);
  const range = Math.max(1e-6, max - min);
  const stroke = trend === "up" ? "#FBBF24" : trend === "down" ? "#34D399" : "rgba(255,255,255,0.28)";
  const poly = pts
    .map((v, idx) => {
      const x = pts.length === 1 ? width / 2 : (idx * width) / (pts.length - 1);
      const y = height - ((v - min) / range) * height;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      <polyline fill="none" stroke={stroke} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" points={poly} />
    </svg>
  );
}
