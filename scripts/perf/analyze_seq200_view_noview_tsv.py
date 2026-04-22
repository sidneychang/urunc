#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def to_int(v: str) -> int | None:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def to_float(v: str) -> float | None:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def mib_from_kb(kb: int | None) -> float | None:
    if kb is None:
        return None
    return kb / 1024.0


def mib_from_bytes(b: int | None) -> float | None:
    if b is None:
        return None
    return b / 1024.0 / 1024.0


def load_rows_by_tag(path: Path) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for d in reader:
            tag = (d.get("tag") or "").strip()
            if not tag:
                continue
            rows[tag] = d
    return rows


def pss_total_kb(r: dict[str, str]) -> int | None:
    parts = [
        to_int(r.get("containerd_main_pss_kb", "")),
        to_int(r.get("shim_urunc_pss_sum_kb", "")),
        to_int(r.get("qemu_pss_sum_kb", "")),
    ]
    if any(v is None for v in parts):
        return None
    return int(parts[0] + parts[1] + parts[2])  # type: ignore[operator]


def rss_total_kb(r: dict[str, str]) -> int | None:
    parts = [
        to_int(r.get("containerd_main_rss_kb", "")),
        to_int(r.get("shim_urunc_rss_sum_kb", "")),
        to_int(r.get("qemu_rss_sum_kb", "")),
    ]
    if any(v is None for v in parts):
        return None
    return int(parts[0] + parts[1] + parts[2])  # type: ignore[operator]


def get_metric_stable(r: dict[str, str]) -> dict[str, Any]:
    mem_shmem_kb = to_int(r.get("mem_shmem_kb", ""))
    mem_avail_kb = to_int(r.get("mem_avail_kb", ""))
    mem_anon_kb = to_int(r.get("mem_anonpages_kb", ""))
    pss_kb = pss_total_kb(r)
    rss_kb = rss_total_kb(r)

    thin_data_used_bytes = to_int(r.get("thin_data_used_bytes", ""))
    thin_meta_used_bytes = to_int(r.get("thin_meta_used_bytes", ""))
    thin_data_used_blocks = to_int(r.get("thin_data_used_blocks", ""))
    thin_meta_used_blocks = to_int(r.get("thin_meta_used_blocks", ""))
    thin_data_pct = to_float(r.get("thin_data_pct", ""))
    thin_meta_pct = to_float(r.get("thin_meta_pct", ""))
    snapshotter_root_bytes = to_int(r.get("snapshotter_root_bytes", ""))
    run_containerd_bytes = to_int(r.get("run_containerd_bytes", ""))
    run_tmpfs_size_bytes = to_int(r.get("run_tmpfs_size_bytes", ""))
    run_tmpfs_used_bytes = to_int(r.get("run_tmpfs_used_bytes", ""))
    run_tmpfs_avail_bytes = to_int(r.get("run_tmpfs_avail_bytes", ""))

    return {
        "mem_shmem_mib": mib_from_kb(mem_shmem_kb),
        "mem_avail_mib": mib_from_kb(mem_avail_kb),
        "mem_anon_mib": mib_from_kb(mem_anon_kb),
        "pss_total_mib": mib_from_kb(pss_kb),
        "rss_total_mib": mib_from_kb(rss_kb),
        "thin_data_used_mib": mib_from_bytes(thin_data_used_bytes),
        "thin_meta_used_mib": mib_from_bytes(thin_meta_used_bytes),
        "thin_data_pct": thin_data_pct,
        "thin_meta_pct": thin_meta_pct,
        "thin_data_used_blocks": thin_data_used_blocks,
        "thin_meta_used_blocks": thin_meta_used_blocks,
        "snapshotter_root_mib": mib_from_bytes(snapshotter_root_bytes),
        "run_containerd_mib": mib_from_bytes(run_containerd_bytes),
        "run_tmpfs_size_mib": mib_from_bytes(run_tmpfs_size_bytes),
        "run_tmpfs_used_mib": mib_from_bytes(run_tmpfs_used_bytes),
        "run_tmpfs_avail_mib": mib_from_bytes(run_tmpfs_avail_bytes),
        # raw ints for debugging/secondary analysis
        "_raw": {
            "mem_shmem_kb": mem_shmem_kb,
            "mem_avail_kb": mem_avail_kb,
            "mem_anonpages_kb": mem_anon_kb,
            "pss_total_kb": pss_kb,
            "rss_total_kb": rss_kb,
            "thin_data_used_bytes": thin_data_used_bytes,
            "thin_meta_used_bytes": thin_meta_used_bytes,
            "thin_data_used_blocks": thin_data_used_blocks,
            "thin_meta_used_blocks": thin_meta_used_blocks,
            "thin_data_pct": thin_data_pct,
            "thin_meta_pct": thin_meta_pct,
            "snapshotter_root_bytes": snapshotter_root_bytes,
            "run_containerd_bytes": run_containerd_bytes,
            "run_tmpfs_size_bytes": run_tmpfs_size_bytes,
            "run_tmpfs_used_bytes": run_tmpfs_used_bytes,
            "run_tmpfs_avail_bytes": run_tmpfs_avail_bytes,
        },
    }


def delta_metrics(stable: dict[str, Any], before: dict[str, Any]) -> dict[str, Any]:
    # both dicts share the MiB numeric keys
    out: dict[str, Any] = {}
    for k, v in stable.items():
        if k == "_raw":
            continue
        bv = before.get(k)
        if v is None or bv is None:
            out[k] = None
        else:
            out[k] = float(v) - float(bv)
    return out


def print_maybe(v: float | None, unit: str = "") -> str:
    if v is None:
        return "NA"
    if unit:
        return f"{v:.1f}{unit}"
    return f"{v:.1f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--view", type=Path, required=True)
    ap.add_argument("--noview", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=False)
    args = ap.parse_args()

    n = args.n
    before_tag = f"before_sequential_{n}"
    stable_tag = f"after_start_{n}_of_{n}_settle_60s"
    cleanup_tag = f"after_cleanup_sequential_{n}"
    cleanup_settle_tag = f"after_cleanup_sequential_{n}_settle_60s"

    view_rows = load_rows_by_tag(args.view)
    noview_rows = load_rows_by_tag(args.noview)

    missing: list[str] = []
    for tag in [before_tag, stable_tag]:
        if tag not in view_rows:
            missing.append(f"view missing {tag}")
        if tag not in noview_rows:
            missing.append(f"noview missing {tag}")
    if missing:
        raise SystemExit("Missing required tags:\n" + "\n".join(missing))

    view_before = get_metric_stable(view_rows[before_tag])
    noview_before = get_metric_stable(noview_rows[before_tag])
    view_stable = get_metric_stable(view_rows[stable_tag])
    noview_stable = get_metric_stable(noview_rows[stable_tag])

    view_delta = delta_metrics(view_stable, view_before)
    noview_delta = delta_metrics(noview_stable, noview_before)

    # Trend: after_start_{i}_of_{n} for i=10..n step 10
    points = list(range(10, n + 1, 10))
    trend: list[dict[str, Any]] = []
    for i in points:
        tag = f"after_start_{i}_of_{n}"
        if tag not in view_rows or tag not in noview_rows:
            continue
        v = get_metric_stable(view_rows[tag])
        nv = get_metric_stable(noview_rows[tag])
        trend.append(
            {
                "i": i,
                "view": {k: v[k] for k in v.keys() if k != "_raw"},
                "noview": {k: nv[k] for k in nv.keys() if k != "_raw"},
                "diff_view_minus_noview": {
                    k: (None if v[k] is None or nv[k] is None else float(v[k]) - float(nv[k]))
                    for k in v.keys()
                    if k != "_raw"
                },
            }
        )

    # Pretty print stable summary.
    print("== Stable point comparison ==")
    print(f"Tags: {before_tag} -> {stable_tag}")
    print("")
    print("Memory (MiB, deltas are stable-before):")
    print(
        f"- view  : PSS_total={print_maybe(view_stable['pss_total_mib'])} (delta {print_maybe(view_delta['pss_total_mib'])}) "
        f"Shmem={print_maybe(view_stable['mem_shmem_mib'])} (delta {print_maybe(view_delta['mem_shmem_mib'])}) "
        f"RSS_total={print_maybe(view_stable['rss_total_mib'])}"
    )
    print(
        f"- noview: PSS_total={print_maybe(noview_stable['pss_total_mib'])} (delta {print_maybe(noview_delta['pss_total_mib'])}) "
        f"Shmem={print_maybe(noview_stable['mem_shmem_mib'])} (delta {print_maybe(noview_delta['mem_shmem_mib'])}) "
        f"RSS_total={print_maybe(noview_stable['rss_total_mib'])}"
    )
    print("")
    print("Storage (MiB, deltas are stable-before):")
    print(
        f"- view  : thin_data={print_maybe(view_stable['thin_data_used_mib'])} (delta {print_maybe(view_delta['thin_data_used_mib'])}), "
        f"thin_meta={print_maybe(view_stable['thin_meta_used_mib'])} (delta {print_maybe(view_delta['thin_meta_used_mib'])}), "
        f"thin_meta_pct={print_maybe(view_stable['thin_meta_pct'])} (delta {print_maybe(view_delta['thin_meta_pct'])}), "
        f"thin_meta_blocks={view_stable['thin_meta_used_blocks'] if view_stable['thin_meta_used_blocks'] is not None else 'NA'} "
        f"(delta {print_maybe(view_delta['thin_meta_used_blocks'])}), "
        f"run_containerd={print_maybe(view_stable['run_containerd_mib'])} (delta {print_maybe(view_delta['run_containerd_mib'])}), "
        f"run_tmpfs_used={print_maybe(view_stable['run_tmpfs_used_mib'])} (delta {print_maybe(view_delta['run_tmpfs_used_mib'])})"
    )
    print(
        f"- noview: thin_data={print_maybe(noview_stable['thin_data_used_mib'])} (delta {print_maybe(noview_delta['thin_data_used_mib'])}), "
        f"thin_meta={print_maybe(noview_stable['thin_meta_used_mib'])} (delta {print_maybe(noview_delta['thin_meta_used_mib'])}), "
        f"thin_meta_pct={print_maybe(noview_stable['thin_meta_pct'])} (delta {print_maybe(noview_delta['thin_meta_pct'])}), "
        f"thin_meta_blocks={noview_stable['thin_meta_used_blocks'] if noview_stable['thin_meta_used_blocks'] is not None else 'NA'} "
        f"(delta {print_maybe(noview_delta['thin_meta_used_blocks'])}), "
        f"run_containerd={print_maybe(noview_stable['run_containerd_mib'])} (delta {print_maybe(noview_delta['run_containerd_mib'])}), "
        f"run_tmpfs_used={print_maybe(noview_stable['run_tmpfs_used_mib'])} (delta {print_maybe(noview_delta['run_tmpfs_used_mib'])})"
    )
    print("")

    # Provide a concise "who is better" heuristic.
    shmem_view_delta = view_delta.get("mem_shmem_mib")
    shmem_noview_delta = noview_delta.get("mem_shmem_mib")
    pss_view_delta = view_delta.get("pss_total_mib")
    pss_noview_delta = noview_delta.get("pss_total_mib")
    thin_data_view_delta = view_delta.get("thin_data_used_mib")
    thin_data_noview_delta = noview_delta.get("thin_data_used_mib")
    thin_meta_view_delta = view_delta.get("thin_meta_used_mib")
    thin_meta_noview_delta = noview_delta.get("thin_meta_used_mib")

    def better(a: float | None, b: float | None) -> str:
        if a is None or b is None:
            return "NA"
        if a < b:
            return "view"
        if a > b:
            return "noview"
        return "equal"

    mem_winner = better(shmem_view_delta, shmem_noview_delta)
    pss_winner = better(pss_view_delta, pss_noview_delta)
    thin_winner = better((thin_data_view_delta if thin_data_view_delta is not None else None), (thin_data_noview_delta if thin_data_noview_delta is not None else None))
    thin_meta_winner = better(thin_meta_view_delta, thin_meta_noview_delta)

    print("== Winner heuristic ==")
    print(f"Shmem(delta): {mem_winner}")
    print(f"PSS_total(delta): {pss_winner}")
    print(f"thin_data_used(delta): {thin_winner}")
    print(f"thin_meta_used(delta): {thin_meta_winner}")
    print("")

    # Print trend (one table) for key metrics.
    print("== Trend @ after_start_{i}_of_{n} (MiB) ==")
    print("| i | view.Shmem | noview.Shmem | view.PSS_total | noview.PSS_total | view.thin_data_used | noview.thin_data_used |")
    print("|---:|---:|---:|---:|---:|---:|---:|")
    for p in trend:
        i = p["i"]
        v = p["view"]
        nv = p["noview"]
        print(
            f"| {i} | {print_maybe(v['mem_shmem_mib'])} | {print_maybe(nv['mem_shmem_mib'])} | "
            f"{print_maybe(v['pss_total_mib'])} | {print_maybe(nv['pss_total_mib'])} | "
            f"{print_maybe(v['thin_data_used_mib'])} | {print_maybe(nv['thin_data_used_mib'])} |"
        )

    # Write JSON for later / automation.
    out: dict[str, Any] = {
        "n": n,
        "before_tag": before_tag,
        "stable_tag": stable_tag,
        "cleanup_tag": cleanup_tag,
        "cleanup_settle_tag": cleanup_settle_tag,
        "view": {"before": view_before, "stable": view_stable, "delta": view_delta},
        "noview": {"before": noview_before, "stable": noview_stable, "delta": noview_delta},
        "trend_after_start_points": trend,
    }

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"\n[json] wrote: {args.out}")


if __name__ == "__main__":
    main()

