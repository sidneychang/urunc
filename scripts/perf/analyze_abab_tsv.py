#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class Row:
    tag: str
    label: str
    snapshotter: str
    mem_shmem_kb: int | None
    mem_avail_kb: int | None
    mem_anonpages_kb: int | None
    mem_slab_kb: int | None
    mem_sreclaimable_kb: int | None
    containerd_main_pss_kb: int | None
    shim_urunc_pss_sum_kb: int | None
    qemu_pss_sum_kb: int | None
    containerd_main_rss_kb: int | None
    shim_urunc_rss_sum_kb: int | None
    qemu_rss_sum_kb: int | None
    cgv2_runtime_current_bytes_sum: int | None
    cgv2_runtime_anon_bytes_sum: int | None
    cgv2_runtime_file_bytes_sum: int | None
    cgv2_runtime_slab_bytes_sum: int | None
    cgv2_runtime_shmem_bytes_sum: int | None
    system_pss_all_kb: int | None
    run_containerd_bytes: int | None
    thin_data_used_bytes: int | None
    thin_meta_used_bytes: int | None


def _int(v: str) -> int | None:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def parse_tsv(path: Path) -> list[Row]:
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows: list[Row] = []
        for d in reader:
            rows.append(
                Row(
                    tag=d.get("tag", ""),
                    label=d.get("bench_label", ""),
                    snapshotter=d.get("snapshotter", ""),
                    mem_shmem_kb=_int(d.get("mem_shmem_kb", "")),
                    mem_avail_kb=_int(d.get("mem_avail_kb", "")),
                    mem_anonpages_kb=_int(d.get("mem_anonpages_kb", "")),
                    mem_slab_kb=_int(d.get("mem_slab_kb", "")),
                    mem_sreclaimable_kb=_int(d.get("mem_sreclaimable_kb", "")),
                    containerd_main_pss_kb=_int(d.get("containerd_main_pss_kb", "")),
                    shim_urunc_pss_sum_kb=_int(d.get("shim_urunc_pss_sum_kb", "")),
                    qemu_pss_sum_kb=_int(d.get("qemu_pss_sum_kb", "")),
                    containerd_main_rss_kb=_int(d.get("containerd_main_rss_kb", "")),
                    shim_urunc_rss_sum_kb=_int(d.get("shim_urunc_rss_sum_kb", "")),
                    qemu_rss_sum_kb=_int(d.get("qemu_rss_sum_kb", "")),
                    cgv2_runtime_current_bytes_sum=_int(d.get("cgv2_runtime_current_bytes_sum", "")),
                    cgv2_runtime_anon_bytes_sum=_int(d.get("cgv2_runtime_anon_bytes_sum", "")),
                    cgv2_runtime_file_bytes_sum=_int(d.get("cgv2_runtime_file_bytes_sum", "")),
                    cgv2_runtime_slab_bytes_sum=_int(d.get("cgv2_runtime_slab_bytes_sum", "")),
                    cgv2_runtime_shmem_bytes_sum=_int(d.get("cgv2_runtime_shmem_bytes_sum", "")),
                    system_pss_all_kb=_int(d.get("system_pss_all_kb", "")),
                    run_containerd_bytes=_int(d.get("run_containerd_bytes", "")),
                    thin_data_used_bytes=_int(d.get("thin_data_used_bytes", "")),
                    thin_meta_used_bytes=_int(d.get("thin_meta_used_bytes", "")),
                )
            )
        return rows


def get(rows: list[Row], label: str, tag: str) -> Row | None:
    for r in rows:
        if r.label == label and r.tag == tag:
            return r
    return None


def pss_total_kb(r: Row) -> int | None:
    parts = [r.containerd_main_pss_kb, r.shim_urunc_pss_sum_kb, r.qemu_pss_sum_kb]
    if any(v is None for v in parts):
        return None
    return int(parts[0] + parts[1] + parts[2])  # type: ignore[operator]


def rss_total_kb(r: Row) -> int | None:
    parts = [r.containerd_main_rss_kb, r.shim_urunc_rss_sum_kb, r.qemu_rss_sum_kb]
    if any(v is None for v in parts):
        return None
    return int(parts[0] + parts[1] + parts[2])  # type: ignore[operator]


def fmt_delta(v: int | None) -> str:
    return "NA" if v is None else f"{v:+d}"


def fmt_mib(kb: int | None) -> str:
    if kb is None:
        return "NA"
    return f"{kb/1024:.1f} MiB"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tsv", type=Path)
    ap.add_argument("--n", type=int, default=50, help="sequential N used in tags (default: 50)")
    args = ap.parse_args()

    rows = parse_tsv(args.tsv)
    labels = sorted({r.label for r in rows if r.label})
    if not labels:
        raise SystemExit("no bench_label rows found")

    # group labels by mode/round from naming: abab_r<round>_<mode>
    modes: dict[str, list[str]] = defaultdict(list)
    for lab in labels:
        if "_noview" in lab:
            modes["noview"].append(lab)
        elif "_view" in lab:
            modes["view"].append(lab)
        else:
            modes["other"].append(lab)

    before_tag = f"before_sequential_{args.n}"
    after_stable_tag = f"after_start_{args.n}_of_{args.n}_settle_60s"

    print(f"Input: {args.tsv}")
    print(f"Key tags: {before_tag} -> {after_stable_tag}")
    print("")
    print(
        "| label | mode | ΔPSS_total | ΔRSS_total | ΔShmem | ΔMemAvailable | ΔAnonPages | ΔSlab | ΔSReclaimable | Δcg_runtime_current | Δcg_anon | Δcg_file | Δcg_slab | Δcg_shmem |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    def delta(a: int | None, b: int | None) -> int | None:
        if a is None or b is None:
            return None
        return b - a

    agg: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for lab in labels:
        mode = "view" if "_view" in lab else ("noview" if "_noview" in lab else "other")
        b = get(rows, lab, before_tag)
        a = get(rows, lab, after_stable_tag)
        if b is None or a is None:
            continue
        d_pss = delta(pss_total_kb(b), pss_total_kb(a))
        d_rss = delta(rss_total_kb(b), rss_total_kb(a))
        d_shm = delta(b.mem_shmem_kb, a.mem_shmem_kb)
        d_ma = delta(b.mem_avail_kb, a.mem_avail_kb)
        d_anon = delta(b.mem_anonpages_kb, a.mem_anonpages_kb)
        d_slab = delta(b.mem_slab_kb, a.mem_slab_kb)
        d_srec = delta(b.mem_sreclaimable_kb, a.mem_sreclaimable_kb)
        d_td = delta(b.thin_data_used_bytes, a.thin_data_used_bytes)
        d_tm = delta(b.thin_meta_used_bytes, a.thin_meta_used_bytes)
        d_cg_cur = delta(b.cgv2_runtime_current_bytes_sum, a.cgv2_runtime_current_bytes_sum)
        d_cg_anon = delta(b.cgv2_runtime_anon_bytes_sum, a.cgv2_runtime_anon_bytes_sum)
        d_cg_file = delta(b.cgv2_runtime_file_bytes_sum, a.cgv2_runtime_file_bytes_sum)
        d_cg_slab = delta(b.cgv2_runtime_slab_bytes_sum, a.cgv2_runtime_slab_bytes_sum)
        d_cg_shmem = delta(b.cgv2_runtime_shmem_bytes_sum, a.cgv2_runtime_shmem_bytes_sum)
        d_pss_all = delta(b.system_pss_all_kb, a.system_pss_all_kb)

        agg[mode].append(
            dict(
                d_pss_kb=d_pss,
                d_rss_kb=d_rss,
                d_shmem_kb=d_shm,
                d_ma_kb=d_ma,
                d_cg_cur_b=d_cg_cur,
                d_pss_all_kb=d_pss_all,
            )
        )

        def fmt_b_mib(v: int | None) -> str:
            return "NA" if v is None else f"{v/1024/1024:+.1f} MiB"

        print(
            f"| {lab} | {mode} | {fmt_mib(d_pss)} | {fmt_mib(d_rss)} | {fmt_mib(d_shm)} | {fmt_mib(d_ma)} | "
            f"{fmt_mib(d_anon)} | {fmt_mib(d_slab)} | {fmt_mib(d_srec)} | "
            f"{fmt_b_mib(d_cg_cur)} | {fmt_b_mib(d_cg_anon)} | {fmt_b_mib(d_cg_file)} | {fmt_b_mib(d_cg_slab)} | {fmt_b_mib(d_cg_shmem)} |"
        )

    def mean(vals: list[int]) -> float:
        return sum(vals) / len(vals) if vals else float("nan")

    print("")
    print("Mode means (MiB):")
    for mode in ("noview", "view"):
        xs = agg.get(mode, [])
        if not xs:
            continue
        dpss = [x["d_pss_kb"] for x in xs if x.get("d_pss_kb") is not None]
        drss = [x["d_rss_kb"] for x in xs if x.get("d_rss_kb") is not None]
        dshm = [x["d_shmem_kb"] for x in xs if x.get("d_shmem_kb") is not None]
        dma = [x["d_ma_kb"] for x in xs if x.get("d_ma_kb") is not None]
        dcg = [x["d_cg_cur_b"] for x in xs if x.get("d_cg_cur_b") is not None]
        dpss_all = [x["d_pss_all_kb"] for x in xs if x.get("d_pss_all_kb") is not None]
        print(
            f"- {mode}: ΔPSS_total={mean(dpss)/1024:.1f} MiB, ΔRSS_total={mean(drss)/1024:.1f} MiB, "
            f"ΔShmem={mean(dshm)/1024:.1f} MiB, ΔMemAvailable={mean(dma)/1024:.1f} MiB, "
            f"Δcg_runtime_current={mean(dcg)/1024/1024:.1f} MiB, Δsystem_PSS_all={mean(dpss_all)/1024:.1f} MiB (n={len(xs)})"
        )


if __name__ == "__main__":
    main()

