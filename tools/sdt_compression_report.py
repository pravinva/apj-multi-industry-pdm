from __future__ import annotations

import copy
import csv
import argparse
import random
from collections import Counter
from pathlib import Path
import sys

import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config.loader import load_config
from core.simulator.engine import OTSimulator

INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
DEFAULT_TICKS = 300
BASE_SEED = 20260325


def _run_industry(industry: str, ticks: int, sdt_enabled: bool) -> tuple[int, Counter]:
    cfg = copy.deepcopy(load_config(industry))
    cfg.setdefault("simulator", {}).setdefault("sdt", {})["enabled"] = sdt_enabled
    random.seed(BASE_SEED + hash(industry) % 1000)
    sim = OTSimulator(cfg, spark=None, catalog=cfg["catalog"])
    total = 0
    per_tag: Counter = Counter()
    for _ in range(ticks):
        rows = sim.emit_tick()
        total += len(rows)
        for row in rows:
            per_tag[row["tag_name"]] += 1
    return total, per_tag


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _plot_overall(summary_rows: list[dict], out_dir: Path) -> Path:
    industries = [r["industry"] for r in summary_rows]
    drop_pct = [r["drop_pct"] for r in summary_rows]
    kept_pct = [r["kept_pct"] for r in summary_rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(industries, drop_pct, color="#FF6B57", alpha=0.9, label="Drop %")
    ax.plot(industries, kept_pct, color="#1E293B", marker="o", linewidth=2, label="Kept %")
    ax.set_ylim(0, 100)
    ax.set_ylabel("Percent")
    ax.set_title("SDT Compression Impact by Industry")
    ax.grid(axis="y", alpha=0.2, linestyle="--")
    ax.legend(loc="upper right")

    for bar, v in zip(bars, drop_pct):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 1.2, f"{v:.1f}%", ha="center", va="bottom", fontsize=9)

    out = out_dir / "overall_by_industry.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def _plot_per_industry_tag_drop(industry: str, per_tag_rows: list[dict], out_dir: Path) -> Path:
    rows = [r for r in per_tag_rows if r["industry"] == industry]
    rows.sort(key=lambda x: x["drop_pct"], reverse=True)
    tags = [r["tag_name"] for r in rows]
    drop = [r["drop_pct"] for r in rows]

    fig_h = max(4, 0.45 * len(tags) + 1.2)
    fig, ax = plt.subplots(figsize=(11, fig_h))
    ax.barh(tags, drop, color="#FF8C42", alpha=0.9)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("Drop %")
    ax.set_title(f"SDT Tag-Level Drop % - {industry.title()}")
    ax.grid(axis="x", alpha=0.2, linestyle="--")
    for i, v in enumerate(drop):
        ax.text(min(v + 1.0, 98.5), i, f"{v:.1f}%", va="center", fontsize=8)

    out = out_dir / f"{industry}_tag_drop.png"
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _write_markdown(
    out_path: Path,
    ticks: int,
    summary_rows: list[dict],
    overall_img: Path,
    per_industry_imgs: dict[str, Path],
) -> None:
    lines: list[str] = []
    lines.append("# SDT Compression Report")
    lines.append("")
    lines.append(f"- Ticks per industry: `{ticks}`")
    lines.append(f"- Base seed: `{BASE_SEED}`")
    lines.append("- Method: simulator replay with identical seed, comparing SDT disabled vs enabled")
    lines.append("")
    lines.append("## Overall Compression")
    lines.append("")
    lines.append("| Industry | Raw Points | SDT Kept | Kept % | Drop % |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in summary_rows:
        lines.append(
            f"| {r['industry']} | {r['raw_total']} | {r['sdt_total']} | {r['kept_pct']:.2f} | {r['drop_pct']:.2f} |"
        )
    lines.append("")
    lines.append(f"![Overall SDT impact]({overall_img.name})")
    lines.append("")
    lines.append("## Tag-Level Drop Charts")
    lines.append("")
    for industry in INDUSTRIES:
        lines.append(f"### {industry.title()}")
        lines.append("")
        lines.append(f"![{industry} tag-level drop]({per_industry_imgs[industry].name})")
        lines.append("")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(ticks: int = DEFAULT_TICKS, out_dir: Path | None = None) -> None:
    out_dir = out_dir or (Path("docs") / "sdt-compression" / str(ticks))
    _ensure_dir(out_dir)

    summary_rows: list[dict] = []
    per_tag_rows: list[dict] = []

    for industry in INDUSTRIES:
        raw_total, raw_tag = _run_industry(industry, ticks=ticks, sdt_enabled=False)
        sdt_total, sdt_tag = _run_industry(industry, ticks=ticks, sdt_enabled=True)
        kept_pct = (sdt_total / raw_total * 100.0) if raw_total else 0.0
        drop_pct = 100.0 - kept_pct
        summary_rows.append(
            {
                "industry": industry,
                "raw_total": raw_total,
                "sdt_total": sdt_total,
                "kept_pct": kept_pct,
                "drop_pct": drop_pct,
            }
        )
        for tag_name in sorted(set(raw_tag) | set(sdt_tag)):
            r = raw_tag.get(tag_name, 0)
            s = sdt_tag.get(tag_name, 0)
            t_kept = (s / r * 100.0) if r else 0.0
            per_tag_rows.append(
                {
                    "industry": industry,
                    "tag_name": tag_name,
                    "raw_count": r,
                    "sdt_count": s,
                    "kept_pct": t_kept,
                    "drop_pct": 100.0 - t_kept if r else 0.0,
                }
            )

    summary_rows.sort(key=lambda r: r["industry"])
    per_tag_rows.sort(key=lambda r: (r["industry"], r["tag_name"]))

    overall_img = _plot_overall(summary_rows, out_dir)
    per_industry_imgs = {
        industry: _plot_per_industry_tag_drop(industry, per_tag_rows, out_dir)
        for industry in INDUSTRIES
    }

    _write_csv(
        out_dir / "overall_summary.csv",
        summary_rows,
        ["industry", "raw_total", "sdt_total", "kept_pct", "drop_pct"],
    )
    _write_csv(
        out_dir / "tag_level_summary.csv",
        per_tag_rows,
        ["industry", "tag_name", "raw_count", "sdt_count", "kept_pct", "drop_pct"],
    )
    _write_markdown(
        out_dir / "report.md",
        ticks=ticks,
        summary_rows=summary_rows,
        overall_img=overall_img,
        per_industry_imgs=per_industry_imgs,
    )

    print(f"Wrote SDT report to: {out_dir.resolve()}")
    print(f"Open: {(out_dir / 'report.md').resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticks", type=int, default=DEFAULT_TICKS)
    parser.add_argument("--out-dir", type=str, default="")
    args = parser.parse_args()
    main(
        ticks=max(10, int(args.ticks)),
        out_dir=Path(args.out_dir) if args.out_dir else None,
    )
