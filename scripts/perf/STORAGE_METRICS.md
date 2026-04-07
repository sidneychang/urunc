# 性能基准脚本中的「存储」指标说明

本文档回答：各列含义、主指标与辅助指标、devmapper vs blockfile、以及 cleanup 后残留与采样时机。

## 1. TSV v2 列与含义

| 列 | 含义 |
|----|------|
| `snapshotter_root_bytes` | `du -sb` 对 `…/io.containerd.snapshotter.v1.<name>` 插件目录。**不是**整块物理盘用量。 |
| `run_containerd_bytes` | `du -sb` 对 `/run/containerd`（可用 `CONTAINERD_RUN_ROOT` 覆盖）。含 bundle、socket、**monRootfs** 等。 |
| `thin_pool_id` | 解析到的 thin pool：`vg/lv`、`dm:<name>` 或空。 |
| `thin_data_pct` / `thin_meta_pct` | thin pool 数据区 / 元数据区占用百分比（`lvs` 或根据 `dmsetup status` 推算）。 |
| `thin_data_used_bytes` / `thin_meta_used_bytes` | 估算的已用字节（`lvs`：池大小 × 百分比；`dmsetup`：扇区 × 扇区大小）。 |
| `thin_metrics_source` | `lvs` \| `dmsetup` \| `none`（无法解析时）。 |

## 2. 各 snapshotter：真实存储应看哪里

### devmapper

- **主指标**：**thin pool**（`thin_*` 列 + `dmsetup status <pool_name>`）。  
  真实数据在 **LVM thin LV 或 dm-thin 设备**里，**不在**插件目录的 `du` 里线性体现。
- **辅助**：`snapshotter_root_bytes` — 多为 **元数据、配置、小文件**；若几乎不变，**符合预期**（大块数据在 pool 里）。
- **管理面**：containerd 通过 **devicemapper thin** 与 pool 交互；`pool_name` 在 `config.toml` 的 devmapper 段，应对应 **dm 设备名或 LVM thin-pool LV 名**。

### blockfile

- **主指标**：**`snapshotter_root_bytes`**（插件目录下稀疏文件、块文件等随容器/快照增长）。  
  若每容器约 **固定步长**（如 500MiB 量级），多与 **snapshotter 为每层/每 writable 预分配稀疏文件** 的实现一致。
- **辅助**：`run_containerd_bytes` — 运行时 bundle；non-view 路径可能明显涨 **/run** 侧。
- **cleanup 后**：若目录仍大，常见原因包括：**异步 GC**、**仍存在的 Committed 快照**（镜像层）、**删除未刷盘**、需 `flush`/`nerdctl system prune` 等。

## 3. 为什么 `thin_data_pct` 曾经一直为空（已改进）

可能原因包括：

1. **只匹配 `segtype == thin-pool`**，而本机 `lvs` 输出字段/空格导致解析失败 → 已改为 **`--separator '|'`** 与 **名称模糊匹配**。
2. **`pool_name` ≠ LV 名** → 已增加 **规范化比较**（忽略 `-`/`_`、大小写）。
3. **非 LVM**（纯 dm 设备）→ 增加 **`dmsetup status` + `thin-pool` 行解析**（`thin_metrics_source=dmsetup`）。
4. **`lvs`/`dmsetup` 需 sudo** → 仍通过 **`bench_sudo`** 执行。
5. **`RESOURCE_SAMPLE_LIGHT=1`** → 会跳过 **du**，但 **thin 列**仍会采集（若未 light 且代码路径一致；若你希望 light 也采 thin，可再调）。

## 4. blockfile cleanup 后仍多约 5×500MiB：脚本问题还是 snapshotter？

可能叠加：

| 因素 | 说明 |
|------|------|
| **异步删除** | containerd 快照删除与文件系统回收可能 **滞后**；`du` 立刻下降不明显。 |
| **Committed 快照** | `reset_urunc_bench_state.sh` 默认 **不删 Committed**（除非 `PRUNE_IMAGES=1`）；镜像层会 **长期占用**。 |
| **采样太早** | 首条 `after_cleanup` 若紧接在 `rm` 后，**未反映 GC 完成**。 |
| **非脚本漏删** | 若 `ctr snapshots ls` 仍有叶快照，需 **`flush_namespace_snapshots.sh`** 按叶删除。 |

**结论**：多为 **清理时机 + snapshotter GC 行为**，不一定是「少删容器」；应用 **`RESOURCE_CLEANUP_SETTLE=1`** 在 cleanup 后 **5/15/30/60s** 再采，对比 `snapshotter_root_bytes` 与 `ctr snapshots ls` 行数。

## 5. cleanup 后多次采样（已实现）

- 环境变量：`RESOURCE_CLEANUP_SETTLE=1`，`RESOURCE_CLEANUP_SAMPLE_DELAYS=5,15,30,60`（从 cleanup 完成起算的**绝对秒数**，脚本内部用增量 `sleep`）。
- 在首条 `after_cleanup_*` 写入后，再写 `after_cleanup_*_settle_5s`、`_settle_15s` …
- **判据**：对比各点 `snapshotter_root_bytes`、`thin_*`、`ctr` snapshot 数是否 **稳定**；若仍不降，考虑 **flush**、**等待更久** 或 **镜像 prune**。

## 6. 建议实验流程

1. 基线：记 TSV + `sudo ctr -n <ns> snapshots --snapshotter <snap> ls | wc -l`。
2. 压测：顺序/并发脚本 + `--tsv`。
3. cleanup：脚本内 `reset` 或手动 `urunc_bench.sh reset`。
4. 设 **`RESOURCE_CLEANUP_SETTLE=1`** 采 settle 曲线。
5. 若 blockfile 仍异常：同一 NS 跑 **`./scripts/perf/flush_namespace_snapshots.sh`**，再观察 `du`。