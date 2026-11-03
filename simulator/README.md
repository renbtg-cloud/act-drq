# James Input Simulator

A standalone HTTP workload generator for the activation service, independent of the application's state stores and provider. Python coordinates separate Java 21 JVMs. Each Java process uses a bounded HTTP client and virtual-thread concurrency. The coordinator aggregates response, scheduling and optional persisted-state observations.

## Build and run

```bash
./simulator/build.sh
python3 simulator/run.py --config simulator/examples/steady.yaml \
  --url http://127.0.0.1:18081 --output-dir simulator/results/steady-example
python3 simulator/run.py --config simulator/examples/burst.yaml \
  --auto-jvms --auto-max-jvms 64 --heap-max-mb 32 \
  --requests 600 --keys 150 --output-dir simulator/results/burst-example
python3 simulator/compare.py \
  simulator/results/steady-example/summary.json \
  simulator/results/burst-example/summary.json \
  --output-prefix simulator/results/comparison
```

Requires Java 21 (`java`, `javac`, `jar`) and Python 3.10+. YAML configuration additionally requires PyYAML; a CLI-only workload does not. The target defaults to localhost; `--allow-remote` is required for non-loopback targets. Load tests must use authorized environments.

Put credentials only in environment variables: `JAMES_SIM_WRITE_TOKEN` and, for `--verify-state`, `JAMES_SIM_READ_TOKEN`. Never include access tokens in run IDs, YAML or command arguments. Results are published into new directories without overwriting prior runs.

## Arrival profiles

| Profile | Scheduling model |
| --- | --- |
| `steady` | Globally paced, open-loop target arrival rate |
| `ramp-up` | Increasing planned arrival rate over a specified duration |
| `burst` | Repeated burst periods separated by baseline arrival rate |
| `soak` | Requests distributed across an explicitly chosen duration |

`--rate` and related profile parameters define *planned* arrivals. Measured scheduling lag and actual HTTP throughput account for resource and downstream limits; planned requests per second are not represented as achieved throughput.

## Identity and collision controls

The first `--keys` requests introduce each key with its original immutable payload. Subsequent requests either reuse keys or create a conflicting payload according to the selected distribution and `--conflict-percent`.

- `sequential`: cyclical reuse of key slots
- `unique`: one key per request (`keys == requests`)
- `hotspot`: configurable preference for a subset of keys
- `skewed`: deterministic power-skew sampling, not an exact Zipf distribution

Use `--seed` to make schedules and identity selection reproducible for a fixed configuration. CLI options override YAML. Unknown YAML keys and incorrectly typed values are rejected.

## Resource controls

`--jvms`, `--concurrency`, `--heap-min-mb`, `--heap-max-mb`, `--active-processors` and `--auto-jvms` configure producer allocation. Auto-selection uses cgroup memory headroom, chosen work volume and a bounded per-JVM memory estimate; it cannot guarantee immunity to out-of-memory failures. `--unsafe-resource-override` disables selected safeguards and should be limited to isolated environments.

`--timeout-ms`, `--max-error-percent`, `--verify-state`, `--await-done-seconds`, `--start-delay-ms`, `--run-id`, `--output-dir` and `--url` control transport and reporting. The local start barrier does not provide a physically synchronized cross-host clock.

## Output semantics

`summary.json` records the chosen JVM count, concurrent activity, admission outcomes, HTTP status groups, conflicts, latency percentiles, scheduling lag, and observed admission throughput. Each producer emits independent plan/result TSV files. Reported `POST/s` is **HTTP admission throughput**, not guaranteed provider-completed effects. `compare.py` produces a Markdown/CSV comparison; it cannot establish causal performance differences across incomparable environments.

## Diagnostics

```bash
python3 simulator/saturation.py --help
python3 simulator/saturation_compare.py --help
python3 simulator/thread_diagnostics.py --help
```

Optional Java Flight Recorder and CPU-affinity diagnostics can identify candidate bottlenecks but do not prove causal attribution. The loopback ingress witness and terminal PostgreSQL observer provide additional bounded measurements when configured. Never expose the local witness on a public interface.

## Distributed hosts and signed evidence

To create a deterministic plan for independent Linux producer hosts, use `simulator/distributed_prepare.py`. Preflight with `host_readiness.py`, record and optionally sign each host's results, transfer the signed shards through an authenticated channel, collect with `shard_collection.py`, then verify and reconcile with `verify_collection.py` and `distributed_merge.py`.

The collector requires independently managed host public-key pins and launch/session inputs for authenticated verification. These signatures authenticate the bound keys and bytes, not physical host identity, clock accuracy or the truthfulness of an external provider.

For complete CLI examples and operator responsibilities, see [`docs/REAL-MULTIHOST-PROTOCOL.md`](../docs/REAL-MULTIHOST-PROTOCOL.md).
