#!/usr/bin/env bash
# 被 run_* 脚本 source：资源采样、devmapper 改 base_image_size 等。
# 常用入口请用 urunc_bench.sh（少记变量）；本节 DEVMAPPER_* 仅高级用法需要。
# shellcheck disable=SC1091

set -euo pipefail

_LIB_BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# nerdctl（Go）会读 HTTP_PROXY / HTTPS_PROXY 等；sudo 默认 env_reset 会丢掉当前 shell 的代理，
# 导致 pull 直连失败。经「sudo env VAR=…」传入子进程。
# 桌面「系统代理」若未写入环境变量，请先在终端 export，或写入 ~/.bashrc 后再跑脚本。
_bench_fill_sudo_env_cmd() {
  _BENCH_SUDO_ENV=(env)
  # sudo 常把 PATH 缩成 secure_path；显式传入可避免 /usr/local/bin 里的 nerdctl 找不到。
  [[ -n "${PATH:-}" ]] && _BENCH_SUDO_ENV+=(PATH="$PATH")
  # Go 的 net/http 会认两套名字；各传一份同值，避免只配了小写时部分路径不生效。
  local hp hps np ap
  hp="${HTTP_PROXY:-${http_proxy:-}}"
  hps="${HTTPS_PROXY:-${https_proxy:-}}"
  np="${NO_PROXY:-${no_proxy:-}}"
  ap="${ALL_PROXY:-${all_proxy:-}}"
  [[ -n "$hp" ]] && _BENCH_SUDO_ENV+=(HTTP_PROXY="$hp" http_proxy="$hp")
  [[ -n "$hps" ]] && _BENCH_SUDO_ENV+=(HTTPS_PROXY="$hps" https_proxy="$hps")
  [[ -n "$np" ]] && _BENCH_SUDO_ENV+=(NO_PROXY="$np" no_proxy="$np")
  [[ -n "$ap" ]] && _BENCH_SUDO_ENV+=(ALL_PROXY="$ap" all_proxy="$ap")
}

bench_sudo() {
  _bench_fill_sudo_env_cmd
  sudo "${_BENCH_SUDO_ENV[@]}" "$@"
}

# /usr/bin/time 只能 exec 外部命令，不能调用 bench_sudo 函数；与 bench_sudo 使用相同代理转发。
bench_time_nerdctl() {
  _bench_fill_sudo_env_cmd
  /usr/bin/time -f "%e" sudo "${_BENCH_SUDO_ENV[@]}" nerdctl "$@"
}
CONTAINERD_CONFIG="${CONTAINERD_CONFIG:-/etc/containerd/config.toml}"
DEVMAPPER_ROOT_PATH="${DEVMAPPER_ROOT_PATH:-/var/lib/containerd/io.containerd.snapshotter.v1.devmapper}"

DEVMAPPER_BASE_IMAGE_SIZE="${DEVMAPPER_BASE_IMAGE_SIZE:-}"
APPLY_DEVMAPPER_BASE_IMAGE_SIZE="${APPLY_DEVMAPPER_BASE_IMAGE_SIZE:-0}"
DEVMAPPER_PREP_BEFORE_RESIZE="${DEVMAPPER_PREP_BEFORE_RESIZE:-1}"
DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE="${DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE:-0}"
DEVMAPPER_RESET_NAMESPACES="${DEVMAPPER_RESET_NAMESPACES:-}"
DEVMAPPER_RESET_RUNTIME_REGEX="${DEVMAPPER_RESET_RUNTIME_REGEX:-io.containerd.urunc}"
DEVMAPPER_HINT_FORCE_PULL_AFTER_RESIZE="${DEVMAPPER_HINT_FORCE_PULL_AFTER_RESIZE:-1}"
DEVMAPPER_SLEEP_AFTER_RESTART_SEC="${DEVMAPPER_SLEEP_AFTER_RESTART_SEC:-1}"

read_devmapper_base_image_size() {
  local cfg="$1"
  if [[ ! -r "$cfg" ]]; then
    echo ""
    return 0
  fi
  awk '
    /^\[plugins.*snapshotter\.v1\.devmapper/ { indm=1; next }
    indm && /^\[/ { indm=0 }
    indm && /^[[:space:]]*base_image_size[[:space:]]*=/ {
      gsub(/^[[:space:]]*base_image_size[[:space:]]*=[[:space:]]*/, "")
      gsub(/^["'\'']|["'\'']$/, "")
      print
      exit 0
    }
  ' "$cfg"
}

# pool_name from containerd devmapper section (matches dm / lvs thin-pool LV name on typical LVM setups).
read_devmapper_pool_name() {
  local cfg="$1"
  if [[ ! -r "$cfg" ]]; then
    echo ""
    return 0
  fi
  awk '
    /^\[plugins.*snapshotter\.v1\.devmapper/ { indm=1; next }
    indm && /^\[/ { indm=0 }
    indm && /^[[:space:]]*pool_name[[:space:]]*=/ {
      gsub(/^[[:space:]]*pool_name[[:space:]]*=[[:space:]]*/, "")
      gsub(/^["'\'']|["'\'']$/, "")
      print
      exit 0
    }
  ' "$cfg"
}

# Normalize pool name for fuzzy match (containerd pool_name vs LVM lv_name).
_bench_norm_pool() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | tr -d '_-'
}

# Echo one VG/LV usable by lvs (thin-pool). Empty if unknown.
# Prefers DEVMAPPER_THIN_LV; else resolves pool_name from CONTAINERD_CONFIG + lvs/dmsetup.
# Set DEVMAPPER_THIN_LV_AUTO=0 to disable auto resolution.
bench_resolve_devmapper_thin_lv() {
  local cfg="${CONTAINERD_CONFIG:-/etc/containerd/config.toml}"
  if [[ -n "${DEVMAPPER_THIN_LV:-}" ]]; then
    echo "${DEVMAPPER_THIN_LV}"
    return 0
  fi
  [[ "${DEVMAPPER_THIN_LV_AUTO:-1}" == "1" ]] || return 1
  local pool out npool nlv
  pool="$(read_devmapper_pool_name "$cfg")"
  [[ -z "$pool" ]] && return 1
  npool="$(_bench_norm_pool "$pool")"

  if command -v lvs >/dev/null 2>&1; then
    # Pipe-separated avoids awk field-split issues with spaces in names.
    while IFS='|' read -r vg lv seg; do
      [[ -z "${vg// }" ]] && continue
      lv="${lv// }"
      seg="${seg// }"
      nlv="$(_bench_norm_pool "$lv")"
      if [[ "$seg" == *thin*pool* ]] || [[ "$seg" == "thin-pool" ]] || [[ "$seg" == *Twi* ]]; then
        if [[ "$lv" == "$pool" ]] || [[ "$nlv" == "$npool" ]]; then
          echo "${vg// }/${lv}"
          return 0
        fi
      fi
    done < <(bench_sudo lvs -a -o vg_name,lv_name,segtype --noheadings --separator '|' 2>/dev/null)

    while IFS='|' read -r vg lv seg; do
      [[ -z "${vg// }" ]] && continue
      lv="${lv// }"
      seg="${seg// }"
      if [[ "$seg" == *thin*pool* ]] || [[ "$seg" == "thin-pool" ]] || [[ "$seg" == *Twi* ]]; then
        echo "${vg// }/${lv}"
        return 0
      fi
    done < <(bench_sudo lvs -a -o vg_name,lv_name,segtype --noheadings --separator '|' 2>/dev/null)
  fi

  # dmsetup: pool_name often matches a mapped device (non-LVM or different naming)
  if command -v dmsetup >/dev/null 2>&1; then
    if bench_sudo dmsetup info -c --noheadings -o name "$pool" >/dev/null 2>&1; then
      echo "dm:${pool}"
      return 0
    fi
    out="$(bench_sudo dmsetup ls --tree 2>/dev/null | awk -v p="$pool" '$0 ~ p {print; exit}')"
    [[ -n "$out" ]] && echo "dm:${pool}" && return 0
  fi
  return 1
}

# Parse dmsetup status for dm-thin pool.
# Typical format:
#   0 <len> thin-pool <tid> <data_u>/<data_t> <meta_u>/<meta_t> ... - <blocksize_sectors>
# Where data_u/meta_u are in *blocks* and the last field is the data block size in *sectors*.
# We convert blocks -> bytes using: blocks * blocksize_sectors * sector_bytes.
_bench_dmsetup_thin_pool_bytes() {
  local dmname="$1"
  local line
  line="$(bench_sudo dmsetup status "$dmname" 2>/dev/null | head -1)"
  [[ -z "$line" ]] && return 1
  echo "$line" | awk -v ss="${BENCH_DM_SECTOR_BYTES:-512}" '
    {
      bs = $NF
      if (bs !~ /^[0-9]+$/ || bs <= 0) bs = 1
      for (i = 1; i <= NF; i++) {
        if ($i == "thin-pool" && i + 3 <= NF) {
          split($(i + 2), a, "/")
          split($(i + 3), b, "/")
          du = (a[1] + 0) * bs * ss
          dt = (a[2] + 0) * bs * ss
          mu = (b[1] + 0) * bs * ss
          mt = (b[2] + 0) * bs * ss
          print du, dt, mu, mt
          exit
        }
      }
    }
  '
}

# Outputs: pool_id pct_d pct_m data_used_bytes meta_used_bytes source
# source: lvs|dmsetup|none
_bench_devmapper_thin_metrics() {
  local lv pct_d pct_m du_b meta_used_b src pool_id
  local lsize line msize secs d_used d_tot m_used m_tot
  pct_d="" && pct_m="" && du_b="" && meta_used_b="" && src="none" && pool_id=""

  lv="${DEVMAPPER_THIN_LV:-}"
  [[ -z "$lv" ]] && lv="$(bench_resolve_devmapper_thin_lv 2>/dev/null || true)"
  pool_id="${lv:-}"

  if [[ -n "$lv" ]] && [[ "$lv" != dm:* ]] && command -v lvs >/dev/null 2>&1; then
    if bench_sudo lvs "$lv" &>/dev/null; then
      lsize="$(bench_sudo lvs --units b --nosuffix -o lv_size --noheadings "$lv" 2>/dev/null | tr -d ' ')"
      [[ -z "$lsize" ]] && lsize="$(bench_sudo lvs --units b --nosuffix -o size --noheadings "$lv" 2>/dev/null | tr -d ' ')"
      line="$(bench_sudo lvs -o data_percent,metadata_percent --noheadings --units k "$lv" 2>/dev/null | head -1)"
      if [[ -n "$line" ]]; then
        pct_d="$(echo "$line" | awk '{gsub(/%/,"",$1); print $1+0}')"
        pct_m="$(echo "$line" | awk '{gsub(/%/,"",$2); print $2+0}')"
        if [[ -n "$lsize" ]] && [[ "$lsize" =~ ^[0-9]+$ ]] && [[ -n "$pct_d" ]]; then
          du_b="$(awk -v s="$lsize" -v p="$pct_d" 'BEGIN { printf "%.0f", s * p / 100 }')"
        fi
        msize="$(bench_sudo lvs --units b --nosuffix -o metadata_size --noheadings "$lv" 2>/dev/null | tr -d ' ')"
        if [[ -n "$msize" ]] && [[ "$msize" =~ ^[0-9]+$ ]] && [[ -n "$pct_m" ]]; then
          meta_used_b="$(awk -v s="$msize" -v p="$pct_m" 'BEGIN { printf "%.0f", s * p / 100 }')"
        fi
        src="lvs"
      fi
    fi
  fi

  if [[ "$src" == "none" ]] && [[ -n "$lv" ]] && [[ "$lv" == dm:* ]]; then
    secs="$(_bench_dmsetup_thin_pool_bytes "${lv#dm:}")"
    if [[ -n "$secs" ]]; then
      read -r d_used d_tot m_used m_tot <<<"$secs"
      du_b="$d_used"
      meta_used_b="$m_used"
      [[ -n "$d_tot" && "$d_tot" != 0 ]] && pct_d="$(awk -v u="$d_used" -v t="$d_tot" 'BEGIN { if (t>0) printf "%.4f", 100*u/t; else print "" }')"
      [[ -n "$m_tot" && "$m_tot" != 0 ]] && pct_m="$(awk -v u="$m_used" -v t="$m_tot" 'BEGIN { if (t>0) printf "%.4f", 100*u/t; else print "" }')"
      src="dmsetup"
    fi
  fi

  if [[ "$src" == "none" ]]; then
    local pname
    pname="$(read_devmapper_pool_name "${CONTAINERD_CONFIG:-/etc/containerd/config.toml}")"
    if [[ -n "$pname" ]]; then
      secs="$(_bench_dmsetup_thin_pool_bytes "$pname")"
      if [[ -n "$secs" ]]; then
        read -r d_used d_tot m_used m_tot <<<"$secs"
        du_b="$d_used"
        meta_used_b="$m_used"
        pool_id="dm:${pname}"
        [[ -n "$d_tot" && "$d_tot" != 0 ]] && pct_d="$(awk -v u="$d_used" -v t="$d_tot" 'BEGIN { if (t>0) printf "%.4f", 100*u/t; else print "" }')"
        [[ -n "$m_tot" && "$m_tot" != 0 ]] && pct_m="$(awk -v u="$m_used" -v t="$m_tot" 'BEGIN { if (t>0) printf "%.4f", 100*u/t; else print "" }')"
        src="dmsetup"
      fi
    fi
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "${pool_id:-}" "${pct_d:-}" "${pct_m:-}" "${du_b:-}" "${meta_used_b:-}" "$src"
}

# Replace base_image_size inside the devmapper plugin section only.
_patch_devmapper_base_image_size_file() {
  local cfg="$1" new="$2" out="$3"
  awk -v new="$new" '
    BEGIN { indm=0; done=0 }
    /^\[plugins.*snapshotter\.v1\.devmapper/ { indm=1; print; next }
    indm && /^\[/ { indm=0 }
    indm && /^[[:space:]]*base_image_size[[:space:]]*=/ && !done {
      match($0, /^[[:space:]]*/)
      sp = substr($0, 1, RLENGTH)
      print sp "base_image_size = \"" new "\""
      done=1
      next
    }
    { print }
    END { exit !done }
  ' "$cfg" >"$out"
}

# 切换 devmapper base_image_size 前：清理容器、任务、非 committed /（可选）committed 快照，避免旧层仍按旧 size。
bench_devmapper_unprepare_before_resize() {
  [[ "${DEVMAPPER_PREP_BEFORE_RESIZE:-1}" == "1" ]] || return 0

  local reset_script="${RESET_SCRIPT:-$_LIB_BENCH_DIR/reset_urunc_bench_state.sh}"
  if [[ ! -f "$reset_script" ]]; then
    echo "[devmapper] unprepare: 找不到 RESET_SCRIPT: $reset_script" >&2
    return 1
  fi

  local nss="${DEVMAPPER_RESET_NAMESPACES:-}"
  if [[ -z "$nss" ]]; then
    if [[ -n "${NS:-}" ]]; then
      nss="$NS"
    else
      nss="default"
    fi
  fi

  echo "[devmapper] unprepare: 修改 base_image_size 前清理 namespace 与 devmapper 快照: $nss"
  if [[ "${DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE:-0}" == "1" ]]; then
    echo "[devmapper] unprepare: DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE=1，将 rmi 并移除 committed 快照（该 NS 下镜像需重新 pull）"
  fi

  local ns
  for ns in $nss; do
    echo "[devmapper] unprepare: 执行 reset_urunc_bench_state NS=$ns SNAPSHOTTER=devmapper"
    NS="$ns" SNAPSHOTTER="devmapper" \
      PRUNE_IMAGES="${DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE:-0}" \
      RUNTIME_REGEX="${DEVMAPPER_RESET_RUNTIME_REGEX:-io.containerd.urunc}" \
      bash "$reset_script"
  done
  sync 2>/dev/null || true
  echo "[devmapper] unprepare 完成。"
}

bench_apply_devmapper_base_image_size() {
  [[ "${SNAPSHOTTER:-}" == "devmapper" ]] || return 0
  [[ -n "${DEVMAPPER_BASE_IMAGE_SIZE}" ]] || return 0

  local current
  current="$(read_devmapper_base_image_size "$CONTAINERD_CONFIG")"
  if [[ "$current" == "${DEVMAPPER_BASE_IMAGE_SIZE}" ]]; then
    echo "[devmapper] base_image_size 已是 ${DEVMAPPER_BASE_IMAGE_SIZE}（config: $CONTAINERD_CONFIG）"
    return 0
  fi

  echo "[devmapper] 当前 base_image_size=${current:-<未找到>}，目标=${DEVMAPPER_BASE_IMAGE_SIZE}"

  if [[ "${APPLY_DEVMAPPER_BASE_IMAGE_SIZE}" != "1" ]]; then
    echo "[devmapper] 未设置 APPLY_DEVMAPPER_BASE_IMAGE_SIZE=1，跳过写配置与重启 containerd。"
    echo "[devmapper] 请手动将 devmapper 的 base_image_size 设为 ${DEVMAPPER_BASE_IMAGE_SIZE} 后重启 containerd，或执行:"
    echo "  APPLY_DEVMAPPER_BASE_IMAGE_SIZE=1 DEVMAPPER_BASE_IMAGE_SIZE=${DEVMAPPER_BASE_IMAGE_SIZE} $0 ..."
    return 0
  fi

  bench_devmapper_unprepare_before_resize

  local tmp
  tmp="$(mktemp)"
  if ! _patch_devmapper_base_image_size_file "$CONTAINERD_CONFIG" "$DEVMAPPER_BASE_IMAGE_SIZE" "$tmp"; then
    rm -f "$tmp"
    echo "[devmapper] 错误: 在 $CONTAINERD_CONFIG 的 devmapper 段未找到 base_image_size 行，无法自动替换。" >&2
    return 1
  fi
  sudo mv "$tmp" "$CONTAINERD_CONFIG"
  echo "[devmapper] 已更新 $CONTAINERD_CONFIG，正在重启 containerd..."
  sudo systemctl restart containerd
  echo "[devmapper] containerd 已重启。"
  if [[ "${DEVMAPPER_SLEEP_AFTER_RESTART_SEC:-0}" =~ ^[0-9]+$ ]] && [[ "${DEVMAPPER_SLEEP_AFTER_RESTART_SEC:-0}" -gt 0 ]]; then
    sleep "${DEVMAPPER_SLEEP_AFTER_RESTART_SEC}"
  fi
  if [[ "${DEVMAPPER_HINT_FORCE_PULL_AFTER_RESIZE:-1}" == "1" ]]; then
    echo "[devmapper] 提示: 若希望镜像层完全按新 base_image_size 重建，可设 DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE=1 后重跑，"
    echo "[devmapper]       或在拉镜像步骤使用 FORCE_PULL=1（若仍inspect 到旧解包层，建议 prune 或 rmi 后再 pull）。"
  fi
}

# One TSV line: time, tag, mem_avail_kb, containerd_rss_kb, devmapper_root_bytes, snap_count
bench_resource_tsv_line() {
  local tag="${1:-}"
  local ns="${2:-}"
  local snap="${3:-devmapper}"

  local mem_avail_kb rss dm_bytes snap_count
  mem_avail_kb="$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)"
  rss="$(ps -o rss= -p "$(pgrep -x containerd 2>/dev/null | head -1)" 2>/dev/null | tr -d ' \t')"
  rss="${rss:-0}"
  if [[ -d "$DEVMAPPER_ROOT_PATH" ]]; then
    dm_bytes="$(sudo du -sb "$DEVMAPPER_ROOT_PATH" 2>/dev/null | awk '{print $1}')"
  else
    dm_bytes=""
  fi
  snap_count=""
  if [[ -n "$ns" ]]; then
    snap_count="$(sudo ctr -n "$ns" snapshots --snapshotter "$snap" ls 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -Iseconds)" "$tag" "$mem_avail_kb" "$rss" "${dm_bytes:-}" "${snap_count:-}" "${snap}"
}

bench_resource_print_header() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "timestamp_iso" "tag" "mem_avail_kb" "containerd_rss_kb" "devmapper_root_bytes" "snapshot_count" "snapshotter"
}


# --- Resource sampling v2: coarse (system-wide) + fine (per-process / paths) ---
# RESOURCE_GRANULARITY=both|coarse|fine  (default both)
# RESOURCE_SAMPLE_LIGHT=1  skip du on large trees (still collects RSS + meminfo)
# RESOURCE_FORMAT=legacy     use bench_resource_tsv_line / bench_resource_print_header
# DEVMAPPER_THIN_LV=vg/lv   optional; thin pool % columns if unset: auto from pool_name in config.toml + lvs
# DEVMAPPER_THIN_LV_AUTO=0  disable auto thin LV resolution (leave thin_% columns empty unless DEVMAPPER_THIN_LV set)
# Timing (set by run_* scripts, optional): BENCH_CONTAINER_START_SEC 单次 nerdctl run 耗时(s)
#   BENCH_CONTAINER_START_MEAN_SEC  并发批内各容器启动耗时均值(s)；BENCH_BATCH_ELAPSED_SEC 整批 wall time(s)
#
# 存储口径摘要（详见 scripts/perf/STORAGE_METRICS.md）:
# - snapshotter_root_bytes: du 插件目录；devmapper 下多为元数据/小文件，真实块数据在 thin pool（看 thin_* 列）
# - run_containerd_bytes: du /run/containerd（bundle/monRootfs 等）
# - thin_* : LVM thin-pool（lvs）或 dmsetup 解析；主指标用于 devmapper 真实占用
# - blockfile: snapshotter_root_bytes 更接近稀疏镜像文件总占用；cleanup 后可能异步回收，见 RESOURCE_CLEANUP_*
CONTAINERD_ROOT="${CONTAINERD_ROOT:-/var/lib/containerd}"
CONTAINERD_RUN_ROOT="${CONTAINERD_RUN_ROOT:-/run/containerd}"
DEVMAPPER_THIN_LV="${DEVMAPPER_THIN_LV:-}"
DEVMAPPER_THIN_LV_AUTO="${DEVMAPPER_THIN_LV_AUTO:-1}"
RESOURCE_GRANULARITY="${RESOURCE_GRANULARITY:-both}"
RESOURCE_SAMPLE_LIGHT="${RESOURCE_SAMPLE_LIGHT:-0}"
# cleanup 后额外多次采样（在首条 after_cleanup 之后）：RESOURCE_CLEANUP_SETTLE=1
# RESOURCE_CLEANUP_SAMPLE_DELAYS=5,15,30,60  从 cleanup 完成时刻起的「绝对秒数」间隔（内部用增量 sleep）
RESOURCE_CLEANUP_SETTLE="${RESOURCE_CLEANUP_SETTLE:-0}"
RESOURCE_CLEANUP_SAMPLE_DELAYS="${RESOURCE_CLEANUP_SAMPLE_DELAYS:-5,15,30,60}"

# 在首条 after_cleanup 行写入 TSV 之后调用；再写若干 settle_${秒}s 行
bench_resource_v2_emit_cleanup_settle() {
  local tag_base="$1" ns="$2" snap="$3"
  [[ "${RESOURCE_CLEANUP_SETTLE:-0}" != "1" ]] && return 0
  [[ "${RESOURCE_SAMPLE:-0}" != "1" ]] && return 0
  [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]] && return 0
  local prev=0 d line
  IFS=',' read -ra delays <<<"${RESOURCE_CLEANUP_SAMPLE_DELAYS:-5,15,30,60}"
  for d in "${delays[@]}"; do
    d="${d// /}"
    [[ -z "$d" ]] || ! [[ "$d" =~ ^[0-9]+$ ]] && continue
    sleep $((d - prev)) || true
    prev=$d
    line="$(bench_resource_sample_line_v2 "${tag_base}_settle_${d}s" "$ns" "$snap")"
    printf '%s\n' "[resource] $line"
    [[ -n "${RESULT_TSV:-}" ]] && printf '%s\n' "$line" >>"${RESULT_TSV}"
  done
}

bench_snapshotter_storage_path() {
  local snap="${1:-devmapper}"
  case "$snap" in
    devmapper) echo "${CONTAINERD_ROOT}/io.containerd.snapshotter.v1.devmapper" ;;
    blockfile) echo "${CONTAINERD_ROOT}/io.containerd.snapshotter.v1.blockfile" ;;
    *)         echo "${CONTAINERD_ROOT}/io.containerd.snapshotter.v1.${snap}" ;;
  esac
}

_bench_meminfo_coarse_kb() {
  awk '
    /^MemTotal:/   { mt=$2 }
    /^MemAvailable:/ { ma=$2 }
    /^MemFree:/    { mf=$2 }
    /^Cached:/     { c=$2 }
    /^Slab:/       { s=$2 }
    /^Buffers:/    { b=$2 }
    END { printf "%s\t%s\t%s\t%s\t%s\t%s\n", mt+0, ma+0, mf+0, c+0, s+0, b+0 }
  ' /proc/meminfo
}

_bench_meminfo_breakdown_kb() {
  awk '
    /^AnonPages:/    { anon=$2 }
    /^SReclaimable:/ { srec=$2 }
    /^KernelStack:/  { kstk=$2 }
    /^PageTables:/   { pt=$2 }
    /^Shmem:/        { shm=$2 }
    END {
      printf "%s\t%s\t%s\t%s\t%s\n", anon+0, srec+0, kstk+0, pt+0, shm+0
    }
  ' /proc/meminfo
}

_bench_containerd_main_rss_kb() {
  local mainpid rss
  mainpid="$(systemctl show containerd.service -p MainPID --value 2>/dev/null || true)"
  if [[ -z "$mainpid" || "$mainpid" == "0" ]]; then
    mainpid="$(pidof containerd 2>/dev/null | awk '{print $1}')"
  fi
  if [[ -z "$mainpid" ]]; then
    echo "0"
    return 0
  fi
  rss="$(ps -o rss= -p "$mainpid" 2>/dev/null | tr -d ' \t')"
  echo "${rss:-0}"
}

_bench_containerd_main_pid() {
  local mainpid
  mainpid="$(systemctl show containerd.service -p MainPID --value 2>/dev/null || true)"
  if [[ -z "$mainpid" || "$mainpid" == "0" ]]; then
    mainpid="$(pidof containerd 2>/dev/null | awk '{print $1}')"
  fi
  [[ -n "$mainpid" && "$mainpid" != "0" ]] || return 1
  echo "$mainpid"
}

_bench_rss_sum_args_match() {
  local pattern="${1:?}"
  ps -eo rss=,args= 2>/dev/null | awk -v re="$pattern" '
    $0 ~ re {
      gsub(/^[[:space:]]+/, "", $1)
      sum += ($1 + 0)
    }
    END { print sum + 0 }
  '
}

_bench_pss_kb_pid() {
  local pid="${1:?}"
  [[ -r "/proc/${pid}/smaps_rollup" ]] || true
  bench_sudo awk '/^Pss:/ { print $2 + 0; exit } END { if (NR == 0) print 0 }' "/proc/${pid}/smaps_rollup" 2>/dev/null || echo 0
}

_bench_pss_sum_args_match() {
  local pattern="${1:?}"
  local sum=0 pid pss
  while read -r pid _; do
    [[ -n "$pid" ]] || continue
    pss="$(_bench_pss_kb_pid "$pid")"
    sum=$((sum + ${pss:-0}))
  done < <(ps -eo pid=,args= 2>/dev/null | awk -v re="$pattern" '$0 ~ re {gsub(/^[[:space:]]+/, "", $1); print $1}')
  echo "$sum"
}

_bench_pss_sum_all_kb() {
  # Sum Pss from /proc/*/smaps_rollup. This is expensive; enable only when needed.
  local sum=0 pid pss
  for pid in /proc/[0-9]*; do
    pid="${pid#/proc/}"
    [[ -r "/proc/${pid}/smaps_rollup" ]] || continue
    pss="$(bench_sudo awk '/^Pss:/ {print $2 + 0; exit}' "/proc/${pid}/smaps_rollup" 2>/dev/null || echo 0)"
    sum=$((sum + ${pss:-0}))
  done
  echo "$sum"
}

_bench_cg_mode() {
  # echo: v2 | v1 | none
  if [[ -f /sys/fs/cgroup/cgroup.controllers ]]; then
    echo v2
  elif [[ -d /sys/fs/cgroup/memory ]] && [[ -f /sys/fs/cgroup/memory/memory.usage_in_bytes ]]; then
    echo v1
  else
    echo none
  fi
}

_bench_cg_read_current_bytes() {
  local cg_rel="${1:?}"
  local mode
  mode="$(_bench_cg_mode)"
  if [[ "$mode" == "v2" ]]; then
    local p="/sys/fs/cgroup${cg_rel}/memory.current"
    [[ -r "$p" ]] || { echo ""; return 0; }
    cat "$p" 2>/dev/null || echo ""
    return 0
  fi
  if [[ "$mode" == "v1" ]]; then
    local p="/sys/fs/cgroup/memory${cg_rel}/memory.usage_in_bytes"
    [[ -r "$p" ]] || { echo ""; return 0; }
    cat "$p" 2>/dev/null || echo ""
    return 0
  fi
  echo ""
}

_bench_cg_read_stat_value() {
  local cg_rel="${1:?}" key="${2:?}"
  local mode
  mode="$(_bench_cg_mode)"
  if [[ "$mode" == "v2" ]]; then
    local p="/sys/fs/cgroup${cg_rel}/memory.stat"
    [[ -r "$p" ]] || { echo ""; return 0; }
    awk -v k="$key" '$1==k {print $2; exit}' "$p" 2>/dev/null || echo ""
    return 0
  fi
  if [[ "$mode" == "v1" ]]; then
    local p="/sys/fs/cgroup/memory${cg_rel}/memory.stat"
    [[ -r "$p" ]] || { echo ""; return 0; }
    awk -v k="$key" '$1==k {print $2; exit}' "$p" 2>/dev/null || echo ""
    return 0
  fi
  echo ""
}

_bench_pid_cg_path_memory_controller() {
  local pid="${1:?}"
  local mode
  mode="$(_bench_cg_mode)"
  if [[ "$mode" == "v2" ]]; then
    awk -F: '$1=="0" {print $3; exit}' "/proc/${pid}/cgroup" 2>/dev/null || true
    return 0
  fi
  if [[ "$mode" == "v1" ]]; then
    # v1 line format: <hier>:<controllers>:<path>
    awk -F: '$2 ~ /(^|,)memory(,|$)/ {print $3; exit}' "/proc/${pid}/cgroup" 2>/dev/null || true
    return 0
  fi
  echo ""
}

_bench_systemd_service_cg_path() {
  local svc="${1:?}"
  systemctl show "$svc" -p ControlGroup --value 2>/dev/null || true
}

_bench_cgv2_sum_current_bytes_for_pids() {
  local sum=0
  declare -A seen=()
  local pid cg cur
  for pid in "$@"; do
    [[ -n "$pid" ]] || continue
    cg="$(_bench_pid_cg_path_memory_controller "$pid")"
    [[ -n "$cg" ]] || continue
    [[ -n "${seen[$cg]:-}" ]] && continue
    seen[$cg]=1
    cur="$(_bench_cg_read_current_bytes "$cg")"
    [[ -n "$cur" ]] && sum=$((sum + cur))
  done
  echo "$sum"
}

_bench_cgv2_sum_stat_value_for_pids() {
  local key="${1:?}"
  shift
  local sum=0
  declare -A seen=()
  local pid cg v
  for pid in "$@"; do
    [[ -n "$pid" ]] || continue
    cg="$(_bench_pid_cg_path_memory_controller "$pid")"
    [[ -n "$cg" ]] || continue
    [[ -n "${seen[$cg]:-}" ]] && continue
    seen[$cg]=1
    v="$(_bench_cg_read_stat_value "$cg" "$key")"
    [[ -n "$v" ]] && sum=$((sum + v))
  done
  echo "$sum"
}

bench_resource_print_header_v2() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "timestamp_iso" "tag" "bench_label" "ns" "snapshotter" \
    "mem_total_kb" "mem_avail_kb" "mem_free_kb" "mem_cached_kb" "mem_slab_kb" "mem_buffers_kb" "mem_shmem_kb" \
    "mem_anonpages_kb" "mem_sreclaimable_kb" "mem_kernelstack_kb" "mem_pagetables_kb" \
    "containerd_main_rss_kb" "shim_urunc_rss_sum_kb" "qemu_rss_sum_kb" "containerd_main_pss_kb" "shim_urunc_pss_sum_kb" "qemu_pss_sum_kb" \
    "cgv2_containerd_current_bytes" "cgv2_shim_current_bytes_sum" "cgv2_qemu_current_bytes_sum" "cgv2_runtime_current_bytes_sum" \
    "cgv2_runtime_anon_bytes_sum" "cgv2_runtime_file_bytes_sum" "cgv2_runtime_slab_bytes_sum" "cgv2_runtime_shmem_bytes_sum" \
    "system_pss_all_kb" \
    "snapshotter_root_bytes" "run_containerd_bytes" \
    "thin_pool_id" "thin_data_pct" "thin_meta_pct" "thin_data_used_bytes" "thin_meta_used_bytes" "thin_metrics_source" \
    "snapshot_count" "running_containers" \
    "container_start_sec" "batch_elapsed_sec" \
    "resource_granularity_note"
}

bench_resource_sample_line_v2() {
  local tag="${1:-}"
  local ns="${2:-}"
  local snap="${3:-devmapper}"
  local label="${URUNC_BENCH_LABEL:-}"
  local gran="${RESOURCE_GRANULARITY:-both}"

  local mt ma mf c s b
  mt="" && ma="" && mf="" && c="" && s="" && b=""
  local shmem
  shmem=""
  local anon srec kstk pt
  anon="" && srec="" && kstk="" && pt=""
  if [[ "$gran" == "both" || "$gran" == "coarse" ]]; then
    IFS=$'\t' read -r mt ma mf c s b < <(_bench_meminfo_coarse_kb)
    shmem="$(awk '/^Shmem:/ {print $2 + 0; exit}' /proc/meminfo)"
    IFS=$'\t' read -r anon srec kstk pt _ < <(_bench_meminfo_breakdown_kb)
  fi

  local ctr_rss shim_rss qemu_rss snap_bytes run_bytes
  local ctr_pss shim_pss qemu_pss
  local cg_ctr_cur cg_shim_cur cg_qemu_cur cg_rt_cur
  local cg_rt_anon cg_rt_file cg_rt_slab cg_rt_shmem
  local pss_all
  local thin_pool_id thin_d thin_m thin_du thin_meta_u thin_msrc
  ctr_rss="" && shim_rss="" && qemu_rss="" && snap_bytes="" && run_bytes=""
  ctr_pss="" && shim_pss="" && qemu_pss=""
  cg_ctr_cur="" && cg_shim_cur="" && cg_qemu_cur="" && cg_rt_cur=""
  cg_rt_anon="" && cg_rt_file="" && cg_rt_slab="" && cg_rt_shmem=""
  pss_all=""
  thin_pool_id="" && thin_d="" && thin_m="" && thin_du="" && thin_meta_u="" && thin_msrc=""

  if [[ "$gran" == "both" || "$gran" == "fine" ]]; then
    ctr_rss="$(_bench_containerd_main_rss_kb)"
    shim_rss="$(_bench_rss_sum_args_match 'containerd-shim-urunc-v2')"
    qemu_rss="$(_bench_rss_sum_args_match 'qemu-system')"
    local cpid
    cpid="$(_bench_containerd_main_pid 2>/dev/null || true)"
    if [[ -n "$cpid" ]]; then
      ctr_pss="$(_bench_pss_kb_pid "$cpid")"
    fi
    shim_pss="$(_bench_pss_sum_args_match 'containerd-shim-urunc-v2')"
    qemu_pss="$(_bench_pss_sum_args_match 'qemu-system')"

    if [[ "${RESOURCE_PSS_ALL:-0}" == "1" ]]; then
      local re="${RESOURCE_PSS_ALL_TAG_REGEX:-^(before_|after_start_.*_settle_|after_cleanup_)}"
      if [[ "$tag" =~ $re ]]; then
        pss_all="$(_bench_pss_sum_all_kb)"
      fi
    fi

    if [[ "$(_bench_cg_mode)" != "none" ]]; then
      # containerd cgroup: prefer systemd ControlGroup path; fallback to pid cgroup.
      local cg_ctr
      cg_ctr="$(_bench_systemd_service_cg_path containerd.service)"
      [[ -z "$cg_ctr" && -n "$cpid" ]] && cg_ctr="$(_bench_pid_cg_path_memory_controller "$cpid")"
      if [[ -n "$cg_ctr" ]]; then
        cg_ctr_cur="$(_bench_cg_read_current_bytes "$cg_ctr")"
      fi

      # runtime related pids: shims + qemu
      local shim_pids qemu_pids
      shim_pids="$(ps -eo pid=,args= 2>/dev/null | awk '$0 ~ /containerd-shim-urunc-v2/ {gsub(/^[[:space:]]+/, "", $1); print $1}')"
      qemu_pids="$(ps -eo pid=,args= 2>/dev/null | awk '$0 ~ /qemu-system/ {gsub(/^[[:space:]]+/, "", $1); print $1}')"
      # sum current bytes by unique cgroup path
      # shellcheck disable=SC2206
      cg_shim_cur="$(_bench_cgv2_sum_current_bytes_for_pids ${shim_pids:-})"
      # shellcheck disable=SC2206
      cg_qemu_cur="$(_bench_cgv2_sum_current_bytes_for_pids ${qemu_pids:-})"
      if [[ -n "$cg_ctr_cur" ]]; then
        cg_rt_cur=$((cg_ctr_cur + cg_shim_cur + cg_qemu_cur))
      else
        cg_rt_cur=$((cg_shim_cur + cg_qemu_cur))
      fi

      # breakdown from memory.stat (anon/file/slab/shmem) for shim+qemu (containerd omitted to avoid double-count if parent slice includes them)
      # shellcheck disable=SC2206
      if [[ "$(_bench_cg_mode)" == "v2" ]]; then
        # v2 keys
        # shellcheck disable=SC2206
        cg_rt_anon="$(_bench_cgv2_sum_stat_value_for_pids anon ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_file="$(_bench_cgv2_sum_stat_value_for_pids file ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_slab="$(_bench_cgv2_sum_stat_value_for_pids slab ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_shmem="$(_bench_cgv2_sum_stat_value_for_pids shmem ${shim_pids:-} ${qemu_pids:-})"
      else
        # v1 memory.stat total_* keys (approx mapping)
        # shellcheck disable=SC2206
        cg_rt_anon="$(_bench_cgv2_sum_stat_value_for_pids total_rss ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_file="$(_bench_cgv2_sum_stat_value_for_pids total_cache ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_slab="$(_bench_cgv2_sum_stat_value_for_pids total_slab ${shim_pids:-} ${qemu_pids:-})"
        # shellcheck disable=SC2206
        cg_rt_shmem="$(_bench_cgv2_sum_stat_value_for_pids total_shmem ${shim_pids:-} ${qemu_pids:-})"
      fi
    fi

    if [[ "${RESOURCE_SAMPLE_LIGHT:-0}" != "1" ]]; then
      local spath
      spath="$(bench_snapshotter_storage_path "$snap")"
      if [[ -d "$spath" ]]; then
        snap_bytes="$(bench_sudo du -sb "$spath" 2>/dev/null | awk '{print $1}')"
      fi
      if [[ -d "${CONTAINERD_RUN_ROOT}" ]]; then
        run_bytes="$(bench_sudo du -sb "${CONTAINERD_RUN_ROOT}" 2>/dev/null | awk '{print $1}')"
      fi
      if [[ "$snap" == "devmapper" ]]; then
        IFS=$'\t' read -r thin_pool_id thin_d thin_m thin_du thin_meta_u thin_msrc < <(_bench_devmapper_thin_metrics)
      fi
    fi
  fi

  local snap_count run_cnt
  snap_count=""
  run_cnt=""
  if [[ "$gran" == "both" || "$gran" == "fine" ]]; then
    if [[ -n "$ns" ]]; then
      snap_count="$(sudo ctr -n "$ns" snapshots --snapshotter "$snap" ls 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')"
    fi
    run_cnt="$(bench_sudo nerdctl -n "$ns" ps -q 2>/dev/null | wc -l | tr -d ' ')"
  fi

  local note
  note="v2"
  if [[ "$gran" == "coarse" ]]; then
    note="v2_coarse_only_system_meminfo"
  elif [[ "$gran" == "fine" ]]; then
    note="v2_fine_no_system_meminfo_columns"
  fi
  if [[ "${RESOURCE_SAMPLE_LIGHT:-0}" == "1" ]]; then
    note="${note}+light_no_du"
  fi

  local cstart bel
  cstart="${BENCH_CONTAINER_START_SEC:-${BENCH_CONTAINER_START_MEAN_SEC:-}}"
  bel="${BENCH_BATCH_ELAPSED_SEC:-}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -Iseconds)" "$tag" "$label" "$ns" "$snap" \
    "${mt:-}" "${ma:-}" "${mf:-}" "${c:-}" "${s:-}" "${b:-}" "${shmem:-}" \
    "${anon:-}" "${srec:-}" "${kstk:-}" "${pt:-}" \
    "${ctr_rss:-}" "${shim_rss:-}" "${qemu_rss:-}" "${ctr_pss:-}" "${shim_pss:-}" "${qemu_pss:-}" \
    "${cg_ctr_cur:-}" "${cg_shim_cur:-}" "${cg_qemu_cur:-}" "${cg_rt_cur:-}" \
    "${cg_rt_anon:-}" "${cg_rt_file:-}" "${cg_rt_slab:-}" "${cg_rt_shmem:-}" \
    "${pss_all:-}" \
    "${snap_bytes:-}" "${run_bytes:-}" \
    "${thin_pool_id:-}" "${thin_d:-}" "${thin_m:-}" "${thin_du:-}" "${thin_meta_u:-}" "${thin_msrc:-}" \
    "${snap_count:-}" "${run_cnt:-}" \
    "${cstart:-}" "${bel:-}" \
    "$note"
}

# after_start 后额外多次采样（在首条 after_start_* 行写入 TSV 之后调用；容器仍在运行时）
# RESOURCE_START_SETTLE=1
# RESOURCE_START_SAMPLE_DELAYS=5,20,60  从 after_start 时刻起的「绝对秒数」间隔（内部用增量 sleep）
RESOURCE_START_SETTLE="${RESOURCE_START_SETTLE:-0}"
RESOURCE_START_SAMPLE_DELAYS="${RESOURCE_START_SAMPLE_DELAYS:-5,20,60}"

bench_resource_v2_emit_start_settle() {
  local tag_base="$1" ns="$2" snap="$3"
  [[ "${RESOURCE_START_SETTLE:-0}" != "1" ]] && return 0
  [[ "${RESOURCE_SAMPLE:-0}" != "1" ]] && return 0
  [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]] && return 0
  local prev=0 d line
  IFS=',' read -ra delays <<<"${RESOURCE_START_SAMPLE_DELAYS:-5,20,60}"
  for d in "${delays[@]}"; do
    d="${d// /}"
    [[ -z "$d" ]] || ! [[ "$d" =~ ^[0-9]+$ ]] && continue
    sleep $((d - prev)) || true
    prev=$d
    line="$(bench_resource_sample_line_v2 "${tag_base}_settle_${d}s" "$ns" "$snap")"
    printf '%s\n' "[resource] $line"
    [[ -n "${RESULT_TSV:-}" ]] && printf '%s\n' "$line" >>"${RESULT_TSV}"
  done
}
