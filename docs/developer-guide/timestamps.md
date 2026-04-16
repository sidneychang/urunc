---
layout: base
title: "Execution time breakdown"
description: "Measuring `urunc` execution"
---

To facilitate performance measurements, a few timestamps have been added to the code base to provide a clear view of the time spent on each part of the execution flow.

## Timestamps

The timestamps currently depicting each unikernel container execution are the following:

| Timestamp ID | Process | Description                                   |
|--------------|---------|-----------------------------------------------|
| TS00         | create  | `urunc create` was invoked                    |
| TS01         | create  | unikontainer struct created for spec          |
| TS02         | create  | initial setup completed                       |
| TS03         | create  | start reexec process (with or without pty)    |
| TS04         | reexec  | `urunc create --reexec` was invoked           |
| TS05         | reexec  | close nsenter pipes and setup base dir        |
| TS06         | create  | received pids from nsenter                    |
| TS07         | create  | executed `CreateRuntime` hooks                |
| TS08         | create  | sent `ACK` IPC message to `reexec` process    |
| TS09         | reexec  | received `ACK` message from `create`          |
| TS10         | create  | `urunc create` terminated                     |
| TS11         | start   | `urunc start` was invoked                     |
| TS12         | start   | unikontainer struct created from spec         |
| TS13         | start   | sent `START` IPC message to `reexec`          |
| TS14         | reexec  | received `START` message from `start`         |
| TS15         | reexec  | joined sandbox network namespace              |
| TS16         | reexec  | network setup completed                       |
| TS17         | reexec  | disk setup completed                          |
| TS18         | reexec  | `execve` the hypervisor process               |

## Timestamping logging method

To log the timestamps with minimal overhead, we opted to use the [zerolog](https://github.com/rs/zerolog) package. We were able to keep the delay caused by the timestamp logging in a low level, around 38351ns for the 20 timestamps required. In comparison, when using [logrus](https://github.com/sirupsen/logrus) the overhead was measured at around 71589ns.

Timestamp logging is now handled through a fixed schema using zerolog. The previous logger benchmark suite has been removed, as it is no longer relevant to the current timestamping implementation.

## How to enable timestamping

In order to capture the timestamps, a separate `containerd-shim` and container runtime must be configured in your system.

To create the "timestamping" version of `containerd-shim-urunc-v2`:

```bash
sudo tee -a /usr/local/bin/containerd-shim-uruncts-v2 > /dev/null << 'EOT'
#!/bin/bash
URUNC_TIMESTAMPS=1 /usr/local/bin/containerd-shim-urunc-v2 $@
EOT
sudo chmod +x /usr/local/bin/containerd-shim-uruncts-v2
```

To add the "timestamping" urunc to containerd config:

```bash
sudo tee -a /etc/containerd/config.toml > /dev/null << 'EOT'
# timestamping urunc
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.uruncts]
    runtime_type = "io.containerd.uruncts.v2"
    container_annotations = ["com.urunc.unikernel.*"]
    pod_annotations = ["com.urunc.unikernel.*"]
    snapshotter = "devmapper"
EOT
sudo systemctl restart containerd.service
```

## How to gather timestamps

Now we need to run a unikernel using the new container runtime `uruncts`:

```bash
sudo nerdctl run --rm --snapshotter devmapper --runtime io.containerd.uruncts.v2 harbor.nbfc.io/nubificus/urunc/hello-hvt-rumprun:latest
```

The timestamp logs are located at `/tmp/urunc.zlog`:

```console
$ cat /tmp/urunc.zlog | grep TS
{"containerID":"faaf830245ffab0df81927cebd7f11065e70c7703121fbc1b11d4bca49bab461","timestampID":"TS00","timestampName":"CR.invoked","timestampOrder":0,"time":1703676366849599657}
{"containerID":"faaf830245ffab0df81927cebd7f11065e70c7703121fbc1b11d4bca49bab461","timestampID":"TS01","timestampName":"CR.unikontainer_created","timestampOrder":1,"time":1703676366850466038}
{"containerID":"faaf830245ffab0df81927cebd7f11065e70c7703121fbc1b11d4bca49bab461","timestampID":"TS02","timestampName":"CR.initial_setup","timestampOrder":2,"time":1703676366850709857}
{"containerID":"faaf830245ffab0df81927cebd7f11065e70c7703121fbc1b11d4bca49bab461","timestampID":"TS03","timestampName":"CR.start_reexec","timestampOrder":3,"time":1703676366850900287}
# ... (rest of the output)
```

> Note: the timestamp destination (`/tmp/urunc.zlog`) is hardcoded for the time being.

## Startup phase profiling for rootfs/view analysis

Besides the fixed `TSxx` timestamps, `urunc` also supports a lightweight
phase-based startup profiler for diagnosing:

- normal serial startup latency
- concurrent startup latency
- snapshot-view versus no-view overhead
- cleanup costs on the delete path

Enable it by setting:

```bash
export URUNC_PROFILE_STARTUP=1
```

When enabled, both `containerd-shim-urunc-v2` and `urunc` emit structured log
entries with:

- `phase`
- `duration_ms`
- `container`
- `container_id`
- `shared_view_id`
- `view_key`
- `mount_path`
- `from_snapshot_view`

Representative `shiminject` phases:

- `shiminject.create_snapshot_view.total`
- `shiminject.resolve_snapshot_key.total`
- `shiminject.shared_view.lock.wait`
- `shiminject.shared_view.lock.hold`
- `shiminject.shared_view.snapshot_view`
- `shiminject.shared_view.snapshot_mounts`
- `shiminject.shared_view.mount.total`
- `shiminject.shared_view.mount.perform`
- `shiminject.shared_view.register_user`
- `shiminject.config.inject_view_path.total`
- `shiminject.cleanup_snapshot_view.total`

Representative `unikontainers` phases:

- `unikontainers.rootfs.choose`
- `unikontainers.rootfs.try_container_block`
- `unikontainers.block.handle_rootfs_total`
- `unikontainers.block.handle_container_rootfs_as_block`
- `unikontainers.block.bind_view_files`
- `unikontainers.block.copy_mountfiles`
- `unikontainers.block.extract_files`
- `unikontainers.block.prepare_dmas_block`
- `unikontainers.delete.cleanup_snapshot_view_bind_mounts`

This profiling is intended to answer questions such as:

- is the slowdown caused by `SnapshotService.View` / `Mounts`?
- are concurrent runs waiting on the shared-view lock?
- is the no-view path actually dominated by file copy, or by later block setup?
- is delete/cleanup interfering with subsequent create timings?

## Gathering the timestamps

There are 3 Python utilities inside the `script/performance` directory to help gather the timestamps.

### Measure single container execution

To gather the timestamps produced by a single unikernel container execution, you can use the `measure_single.py` script, passing the desired container id.

```bash
cd urunc/script/performance
python3 measure_single.py 15c769b9be14c59174626521f7964a8ae06e75c48c5cfd91e2829317c15d455b
```

If no container ID is specified, it will return an error:

```console
$ python3 measure_single.py 
Error: Container ID not specified!

Usage:
        measure_single.py <CONTAINER_ID>
```

Sample output:

```console
$ python3 measure_single.py 1bd50216c1709b854f78d50ec36cbbc55e0d4bc2e1509344082b51edc974af6d
TS00 -> TS01:   1086512 ns
TS01 -> TS02:   97936 ns
TS02 -> TS03:   119786 ns
# ... (rest of the output)
```

### Automatically measure multiple containers

To automatically gather the timestamps produced by multiple unikernel container executions you can use the `measure.py` script, passing the desired iterations amount. Make sure to use `sudo` or execute this script as root, as it relies on `nerdctl` for spawning the unikernel containers.

```bash
cd urunc/script/performance
sudo python3 measure.py 5
```

If the amount of iterations is not specified, it will return an error:

```console
$ sudo python3 measure.py 
Error: Iterations not specified!

Usage:
        measure.py <ITERATIONS>
```

Sample output:

```console
$ sudo python3 measure.py 2
{'TS00 -> TS01': {'average': '11544405 ns',
                  'maximum': '22292698 ns',
                  'minimum': '796112 ns'},
 'TS01 -> TS02': {'average': '127228 ns',
                  'maximum': '157051 ns',
                  'minimum': '97405 ns'},
 'TS02 -> TS03': {'average': '120198 ns',
                  'maximum': '162634 ns',
                  'minimum': '77763 ns'},
# ... (rest of the output)
```


The same functionality is provided by `measure_to_json.py`, but instead of `stdout` the results are saved in a .json file:

```console
$ sudo python3 measure_to_json.py 5 ts.json
$ cat ts.json | jq
{
  "TS00 -> TS01": {
    "maximum": "989525 ns",
    "minimum": "474103 ns",
    "average": "719644 ns"
  },
  "TS01 -> TS02": {
    "maximum": "212337 ns",
    "minimum": "76951 ns",
    "average": "122868 ns"
# ... (rest of the output)
```

If the amount of iterations or output file are not specified, it will return an error:

```console
$ sudo python3 measure_to_json.py 5 
Error: Iterations or output file not specified!

Usage:
        measure_to_json.py <ITERATIONS> <OUTPUT>
```
