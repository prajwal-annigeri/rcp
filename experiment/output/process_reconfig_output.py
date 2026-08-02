#!/usr/bin/env python3
"""Process reconfiguration experiment outputs and plot latency curves.

Run from `experiment/output` by default. Reads `./reconfig/run*/` unless overridden.

Behavior:
- Parse all report entries (SUCCESS and FAILED) between:
  "=== Reconfiguration Latency Report ===" and
  "=== Reconfiguration Latency Summary ==="
- If `--run` is provided: plot that run for each protocol.
- If `--run` is not provided: plot per-t averages across all runs for each protocol.
- Plot total latency and active latency separately.
- `-t/--t` selects a tail window size from the back (last N points).
- Plot and stats use only that tail window; if omitted, all points are used.
"""

from __future__ import annotations

import argparse
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt


MODE_ORDER = ["joint", "recraft", "orca"]
MODE_COLORS = {
    "joint": "tab:blue",
    "recraft": "tab:orange",
    "orca": "tab:green",
}
MODE_LABELS = {
    "joint": "Joint",
    "recraft": "ReCraft",
    "orca": "Orca",
}

REPORT_START = "=== Reconfiguration Latency Report ==="
REPORT_END = "=== Reconfiguration Latency Summary ==="

ENTRY_RE = re.compile(
    r"t=(?P<t>\d+)s\s+voters=\[[^\]]*\]\s+status=(?P<status>SUCCESS|FAILED)\s+"
    r"latency_total=(?P<total>\S+)\s+latency_active=(?P<active>\S+)\s+"
    r"retry_delay=\S+\s+attempts=\d+\s+contact=\S+.*$"
)

MODE_RE = re.compile(r"mode=(joint|recraft|orca)")
RUN_RE = re.compile(r"-run-(\d+)\.txt$")


@dataclass
class RunData:
    file_path: Path
    mode: str
    run_id: int
    by_t: Dict[int, Tuple[float, float]]  # t -> (total_ms, active_ms)


def duration_to_ms(token: str) -> float:
    if token.endswith("ms"):
        return float(token[:-2])
    if token.endswith("µs"):
        return float(token[:-2]) / 1000.0
    if token.endswith("us"):
        return float(token[:-2]) / 1000.0
    if token.endswith("ns"):
        return float(token[:-2]) / 1_000_000.0
    if token.endswith("s"):
        return float(token[:-1]) * 1000.0
    raise ValueError(f"unsupported duration token: {token}")


def mode_label(mode: str) -> str:
    return MODE_LABELS.get(mode, mode)


def percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    sorted_vals = sorted(values)
    rank = math.ceil((p / 100.0) * len(sorted_vals))
    rank = max(1, min(rank, len(sorted_vals)))
    return sorted_vals[rank - 1]


def parse_mode_and_run(file_path: Path) -> Optional[Tuple[str, int]]:
    m = MODE_RE.search(file_path.name)
    if not m:
        return None
    mode = m.group(1)

    rm = RUN_RE.search(file_path.name)
    if rm:
        return mode, int(rm.group(1))

    # Unsuffixed files are treated as run 1 by convention.
    return mode, 1


def parse_report_section(file_path: Path) -> Dict[int, Tuple[float, float]]:
    in_report = False
    parsed: Dict[int, Tuple[float, float]] = {}

    with file_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()

            if REPORT_START in line:
                in_report = True
                continue

            if REPORT_END in line and in_report:
                break

            if not in_report:
                continue

            m = ENTRY_RE.search(line)
            if not m:
                continue

            t = int(m.group("t"))
            total_ms = duration_to_ms(m.group("total"))
            active_ms = duration_to_ms(m.group("active"))
            parsed[t] = (total_ms, active_ms)

    return parsed


def load_runs(input_dir: Path) -> List[RunData]:
    txt_files = sorted(input_dir.glob("run*/*.txt"))
    runs: List[RunData] = []

    for fp in txt_files:
        meta = parse_mode_and_run(fp)
        if meta is None:
            continue
        mode, run_id = meta
        by_t = parse_report_section(fp)
        if by_t:
            runs.append(RunData(file_path=fp, mode=mode, run_id=run_id, by_t=by_t))

    return runs


def drop_unsuffixed_if_suffixed_exists(runs: List[RunData]) -> List[RunData]:
    # In this repo workflow, unsuffixed files are often the latest run and can
    # duplicate `-run-1` artifacts. If any suffixed files exist for a mode,
    # keep only suffixed files for that mode.
    mode_has_suffixed = {m: False for m in MODE_ORDER}
    for r in runs:
        if RUN_RE.search(r.file_path.name):
            mode_has_suffixed[r.mode] = True

    filtered: List[RunData] = []
    for r in runs:
        if mode_has_suffixed.get(r.mode, False) and RUN_RE.search(r.file_path.name) is None:
            continue
        filtered.append(r)

    return filtered


def select_series_for_run(runs: List[RunData], run_id: int) -> Dict[str, Dict[int, Tuple[float, float]]]:
    selected: Dict[str, Dict[int, Tuple[float, float]]] = {}

    for mode in MODE_ORDER:
        candidates = [r for r in runs if r.mode == mode and r.run_id == run_id]
        if not candidates:
            continue
        # If duplicates exist, prefer newest file.
        candidates.sort(key=lambda r: r.file_path.stat().st_mtime, reverse=True)
        selected[mode] = candidates[0].by_t

    return selected


def average_series_across_runs(runs: List[RunData]) -> Tuple[Dict[str, Dict[int, Tuple[float, float]]], Dict[str, Dict[int, List[Tuple[float, float]]]]]:
    # raw_points[mode][t] = list[(total_ms, active_ms)] across runs
    raw_points: Dict[str, Dict[int, List[Tuple[float, float]]]] = {m: {} for m in MODE_ORDER}

    for r in runs:
        mode_points = raw_points.setdefault(r.mode, {})
        for t, pair in r.by_t.items():
            mode_points.setdefault(t, []).append(pair)

    averaged: Dict[str, Dict[int, Tuple[float, float]]] = {}
    for mode in MODE_ORDER:
        by_t_list = raw_points.get(mode, {})
        if not by_t_list:
            continue
        avg_by_t: Dict[int, Tuple[float, float]] = {}
        for t, vals in by_t_list.items():
            avg_total = mean(v[0] for v in vals)
            avg_active = mean(v[1] for v in vals)
            avg_by_t[t] = (avg_total, avg_active)
        averaged[mode] = dict(sorted(avg_by_t.items(), key=lambda kv: kv[0]))

    return averaged, raw_points


def tail_keys(keys: List[int], tail_n: Optional[int]) -> List[int]:
    if tail_n is None:
        return keys
    if tail_n <= 0:
        return []
    return keys[-tail_n:]


def limit_series_tail(
    series: Dict[str, Dict[int, Tuple[float, float]]], tail_n: Optional[int]
) -> Dict[str, Dict[int, Tuple[float, float]]]:
    limited: Dict[str, Dict[int, Tuple[float, float]]] = {}
    for mode, by_t in series.items():
        ks = sorted(by_t.keys())
        keep = set(tail_keys(ks, tail_n))
        limited_mode = {t: by_t[t] for t in ks if t in keep}
        if limited_mode:
            limited[mode] = limited_mode
    return limited


def filter_series_by_parity(
    series: Dict[str, Dict[int, Tuple[float, float]]], parity: int
) -> Dict[str, Dict[int, Tuple[float, float]]]:
    filtered: Dict[str, Dict[int, Tuple[float, float]]] = {}
    for mode, by_t in series.items():
        mode_filtered = {t: v for t, v in by_t.items() if t % 2 == parity}
        if mode_filtered:
            filtered[mode] = dict(sorted(mode_filtered.items(), key=lambda kv: kv[0]))
    return filtered


def filter_raw_points_by_parity(
    raw_points: Dict[str, Dict[int, List[Tuple[float, float]]]], parity: int
) -> Dict[str, Dict[int, List[Tuple[float, float]]]]:
    filtered: Dict[str, Dict[int, List[Tuple[float, float]]]] = {}
    for mode, by_t in raw_points.items():
        mode_filtered = {t: vals for t, vals in by_t.items() if t % 2 == parity}
        if mode_filtered:
            filtered[mode] = dict(sorted(mode_filtered.items(), key=lambda kv: kv[0]))
    return filtered


def limit_raw_points_tail(
    raw_points: Dict[str, Dict[int, List[Tuple[float, float]]]], tail_n: Optional[int]
) -> Dict[str, Dict[int, List[Tuple[float, float]]]]:
    limited: Dict[str, Dict[int, List[Tuple[float, float]]]] = {}
    for mode, by_t in raw_points.items():
        ks = sorted(by_t.keys())
        keep = set(tail_keys(ks, tail_n))
        mode_limited = {t: by_t[t] for t in ks if t in keep}
        if mode_limited:
            limited[mode] = mode_limited
    return limited


def plot_metric(
    series: Dict[str, Dict[int, Tuple[float, float]]],
    metric_idx: int,
    ylabel: str,
    output_path: Path,
    xlim: Optional[Tuple[float, float]] = None,
    ylim: Optional[Tuple[float, float]] = None,
) -> None:
    plt.figure(figsize=(10, 5))
    all_ts: List[int] = []
    for by_t in series.values():
        all_ts.extend(by_t.keys())
    if not all_ts:
        plt.close()
        return
    min_t = min(all_ts)

    for mode in MODE_ORDER:
        if mode not in series:
            continue
        by_t = series[mode]
        xs_raw = sorted(by_t.keys())
        xs = [t - min_t + 1 for t in xs_raw]
        ys = [by_t[t][metric_idx] for t in xs_raw]
        if not xs_raw:
            continue
        plt.plot(xs, ys, marker="o", label=mode_label(mode), color=MODE_COLORS.get(mode))

    plt.xlabel("Timestamp (seconds)")
    plt.ylabel(ylabel)
    if xlim is not None:
        plt.xlim(xlim)
    if ylim is not None:
        plt.ylim(ylim)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_metric_if_nonempty(
    series: Dict[str, Dict[int, Tuple[float, float]]],
    metric_idx: int,
    ylabel: str,
    output_path: Path,
    xlim: Optional[Tuple[float, float]] = None,
    ylim: Optional[Tuple[float, float]] = None,
) -> bool:
    has_points = any(len(by_t) > 0 for by_t in series.values())
    if not has_points:
        return False
    plot_metric(
        series=series,
        metric_idx=metric_idx,
        ylabel=ylabel,
        output_path=output_path,
        xlim=xlim,
        ylim=ylim,
    )
    return True


def compute_axis_limits(
    series: Dict[str, Dict[int, Tuple[float, float]]], metric_idx: int
) -> Tuple[Optional[Tuple[float, float]], Optional[Tuple[float, float]]]:
    all_ts: List[int] = []
    all_vals: List[float] = []
    for by_t in series.values():
        for t, pair in by_t.items():
            all_ts.append(t)
            all_vals.append(pair[metric_idx])

    if not all_ts or not all_vals:
        return None, None

    min_t = min(all_ts)
    max_t = max(all_ts)
    xlim = (1.0, float(max_t - min_t + 1))

    min_v = min(all_vals)
    max_v = max(all_vals)
    if math.isclose(min_v, max_v):
        pad = max(1.0, abs(min_v) * 0.05)
        ylim = (min_v - pad, max_v + pad)
    else:
        span = max_v - min_v
        pad = span * 0.05
        ylim = (min_v - pad, max_v + pad)

    return xlim, ylim


def print_window_stats_for_run(
    series: Dict[str, Dict[int, Tuple[float, float]]], tail_n: Optional[int], label: str
) -> None:
    print(f"=== Window Statistics ({label}) ===")
    for mode in MODE_ORDER:
        if mode not in series or not series[mode]:
            print(f"{mode_label(mode)}: no data")
            continue
        ks = sorted(series[mode].keys())
        win = tail_keys(ks, tail_n)
        if not win:
            print(f"{mode_label(mode)}: no data in selected tail window")
            continue
        totals = [series[mode][t][0] for t in win]
        actives = [series[mode][t][1] for t in win]
        print(f"{mode_label(mode)}: t_range={win[0]}..{win[-1]} samples={len(win)}")
        print(
            "  total_ms: "
            f"min={min(totals):.6f} max={max(totals):.6f} avg={mean(totals):.6f} "
            f"p50={percentile(totals, 50):.6f} p90={percentile(totals, 90):.6f} "
            f"p95={percentile(totals, 95):.6f} p99={percentile(totals, 99):.6f}"
        )
        print(
            "  active_ms: "
            f"min={min(actives):.6f} max={max(actives):.6f} avg={mean(actives):.6f} "
            f"p50={percentile(actives, 50):.6f} p90={percentile(actives, 90):.6f} "
            f"p95={percentile(actives, 95):.6f} p99={percentile(actives, 99):.6f}"
        )


def print_window_stats_for_average(
    raw_points: Dict[str, Dict[int, List[Tuple[float, float]]]], tail_n: Optional[int], label: str
) -> None:
    print(f"=== Window Statistics ({label}) ===")
    for mode in MODE_ORDER:
        by_t = raw_points.get(mode, {})
        if not by_t:
            print(f"{mode_label(mode)}: no data")
            continue

        ks = sorted(by_t.keys())
        win = tail_keys(ks, tail_n)
        if not win:
            print(f"{mode_label(mode)}: no data in selected tail window")
            continue
        totals: List[float] = []
        actives: List[float] = []
        for t in win:
            vals = by_t[t]
            totals.extend(v[0] for v in vals)
            actives.extend(v[1] for v in vals)

        print(f"{mode_label(mode)}: t_range={win[0]}..{win[-1]} samples={len(totals)}")
        print(
            "  total_ms: "
            f"min={min(totals):.6f} max={max(totals):.6f} avg={mean(totals):.6f} "
            f"p50={percentile(totals, 50):.6f} p90={percentile(totals, 90):.6f} "
            f"p95={percentile(totals, 95):.6f} p99={percentile(totals, 99):.6f}"
        )
        print(
            "  active_ms: "
            f"min={min(actives):.6f} max={max(actives):.6f} avg={mean(actives):.6f} "
            f"p50={percentile(actives, 50):.6f} p90={percentile(actives, 90):.6f} "
            f"p95={percentile(actives, 95):.6f} p99={percentile(actives, 99):.6f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Process reconfiguration output files and plot latencies.")
    parser.add_argument(
        "--input-dir",
        default="./reconfig",
        help="Directory containing run folders (default: ./reconfig/run*/)",
    )
    parser.add_argument("--run", type=int, default=None, help="Specific run number to plot (e.g., 1, 2, 3)")
    parser.add_argument(
        "-t",
        "--t",
        type=int,
        default=None,
        help="Tail window size from back (last N points) for plot + stats; default uses all points",
    )
    parser.add_argument("--output-prefix", default="reconfig_latency", help="Output plot filename prefix")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        raise SystemExit(f"Input directory not found: {input_dir}")

    runs = load_runs(input_dir)
    runs = drop_unsuffixed_if_suffixed_exists(runs)
    if not runs:
        raise SystemExit(f"No parseable reconfig files found in {input_dir}")

    if args.run is not None:
        selected = select_series_for_run(runs, args.run)
        if not selected:
            raise SystemExit(f"No data found for --run {args.run}")
        selected_limited = limit_series_tail(selected, args.t)
        if not selected_limited:
            raise SystemExit("No data left after applying tail window.")

        total_plot = Path(f"{args.output_prefix}_run{args.run}_total.png")
        active_plot = Path(f"{args.output_prefix}_run{args.run}_active.png")
        odd_total_plot = Path(f"{args.output_prefix}_run{args.run}_odd_total.png")
        odd_active_plot = Path(f"{args.output_prefix}_run{args.run}_odd_active.png")
        even_total_plot = Path(f"{args.output_prefix}_run{args.run}_even_total.png")
        even_active_plot = Path(f"{args.output_prefix}_run{args.run}_even_active.png")
        total_xlim, total_ylim = compute_axis_limits(selected_limited, metric_idx=0)
        active_xlim, active_ylim = compute_axis_limits(selected_limited, metric_idx=1)

        plot_metric(
            selected_limited,
            metric_idx=0,
            ylabel="total latency (ms)",
            output_path=total_plot,
            xlim=total_xlim,
            ylim=total_ylim,
        )
        plot_metric(
            selected_limited,
            metric_idx=1,
            ylabel="active latency (ms)",
            output_path=active_plot,
            xlim=active_xlim,
            ylim=active_ylim,
        )
        odd_series = filter_series_by_parity(selected_limited, parity=1)
        even_series = filter_series_by_parity(selected_limited, parity=0)
        odd_total_saved = plot_metric_if_nonempty(
            odd_series,
            metric_idx=0,
            ylabel="total latency (ms)",
            output_path=odd_total_plot,
            xlim=total_xlim,
            ylim=total_ylim,
        )
        odd_active_saved = plot_metric_if_nonempty(
            odd_series,
            metric_idx=1,
            ylabel="active latency (ms)",
            output_path=odd_active_plot,
            xlim=active_xlim,
            ylim=active_ylim,
        )
        even_total_saved = plot_metric_if_nonempty(
            even_series,
            metric_idx=0,
            ylabel="total latency (ms)",
            output_path=even_total_plot,
            xlim=total_xlim,
            ylim=total_ylim,
        )
        even_active_saved = plot_metric_if_nonempty(
            even_series,
            metric_idx=1,
            ylabel="active latency (ms)",
            output_path=even_active_plot,
            xlim=active_xlim,
            ylim=active_ylim,
        )

        print_window_stats_for_run(selected_limited, None, "selected run, all t")
        print_window_stats_for_run(odd_series, None, "selected run, odd t")
        print_window_stats_for_run(even_series, None, "selected run, even t")
        print(f"Saved: {total_plot}")
        print(f"Saved: {active_plot}")
        if odd_total_saved:
            print(f"Saved: {odd_total_plot}")
        if odd_active_saved:
            print(f"Saved: {odd_active_plot}")
        if even_total_saved:
            print(f"Saved: {even_total_plot}")
        if even_active_saved:
            print(f"Saved: {even_active_plot}")
        return

    averaged, raw_points = average_series_across_runs(runs)
    if not averaged:
        raise SystemExit("No averaged data could be computed.")
    averaged_limited = limit_series_tail(averaged, args.t)
    if not averaged_limited:
        raise SystemExit("No averaged data left after applying tail window.")

    total_plot = Path(f"{args.output_prefix}_avg_total.png")
    active_plot = Path(f"{args.output_prefix}_avg_active.png")
    odd_total_plot = Path(f"{args.output_prefix}_avg_odd_total.png")
    odd_active_plot = Path(f"{args.output_prefix}_avg_odd_active.png")
    even_total_plot = Path(f"{args.output_prefix}_avg_even_total.png")
    even_active_plot = Path(f"{args.output_prefix}_avg_even_active.png")
    total_xlim, total_ylim = compute_axis_limits(averaged_limited, metric_idx=0)
    active_xlim, active_ylim = compute_axis_limits(averaged_limited, metric_idx=1)

    plot_metric(
        averaged_limited,
        metric_idx=0,
        ylabel="Latency (ms)",
        output_path=total_plot,
        xlim=total_xlim,
        ylim=total_ylim,
    )
    plot_metric(
        averaged_limited,
        metric_idx=1,
        ylabel="Latency (ms)",
        output_path=active_plot,
        xlim=active_xlim,
        ylim=active_ylim,
    )
    odd_series = filter_series_by_parity(averaged_limited, parity=1)
    even_series = filter_series_by_parity(averaged_limited, parity=0)
    odd_total_saved = plot_metric_if_nonempty(
        odd_series,
        metric_idx=0,
        ylabel="Latency (ms)",
        output_path=odd_total_plot,
        xlim=total_xlim,
        ylim=total_ylim,
    )
    odd_active_saved = plot_metric_if_nonempty(
        odd_series,
        metric_idx=1,
        ylabel="Latency (ms)",
        output_path=odd_active_plot,
        xlim=active_xlim,
        ylim=active_ylim,
    )
    even_total_saved = plot_metric_if_nonempty(
        even_series,
        metric_idx=0,
        ylabel="Latency (ms)",
        output_path=even_total_plot,
        xlim=total_xlim,
        ylim=total_ylim,
    )
    even_active_saved = plot_metric_if_nonempty(
        even_series,
        metric_idx=1,
        ylabel="Latency (ms)",
        output_path=even_active_plot,
        xlim=active_xlim,
        ylim=active_ylim,
    )

    # Tail window is applied per mode in stats too.
    raw_points_limited = limit_raw_points_tail(raw_points, args.t)
    odd_raw_points = filter_raw_points_by_parity(raw_points_limited, parity=1)
    even_raw_points = filter_raw_points_by_parity(raw_points_limited, parity=0)
    print_window_stats_for_average(raw_points_limited, None, "across runs, all t")
    print_window_stats_for_average(odd_raw_points, None, "across runs, odd t")
    print_window_stats_for_average(even_raw_points, None, "across runs, even t")
    print(f"Saved: {total_plot}")
    print(f"Saved: {active_plot}")
    if odd_total_saved:
        print(f"Saved: {odd_total_plot}")
    if odd_active_saved:
        print(f"Saved: {odd_active_plot}")
    if even_total_saved:
        print(f"Saved: {even_total_plot}")
    if even_active_saved:
        print(f"Saved: {even_active_plot}")


if __name__ == "__main__":
    main()
