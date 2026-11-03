# James — Asynchronous fault-tolerant product activation

**Backend-agnostic activation workers.** The worker processing logic, durable business-state transitions, retry/reconciliation, and provider integration are shared across delivery backends. A `DeliveryBackend` abstraction isolates transport-specific message claiming, consumption and acknowledgment: **RabbitMQ and PostgreSQL are the first two concrete implementations, not fixed architectural dependencies**. RabbitMQ is the default and more extensively validated backend; the PostgreSQL-only alternative has passed local integration and concurrency tests but has received substantially less adversarial testing.

An asynchronous product-activation service with durable admission, asynchronous provider delivery, idempotency, retry/reconciliation, OAuth2/JWT authentication, and independently runnable load and provider simulators.

The three cooperating components are:

1. **Activation service:** Spring Boot (Java 21), PostgreSQL (durable state and outbox), selectable RabbitMQ or PostgreSQL delivery (locally tested, but less extensively validated than RabbitMQ), Flyway migrations and JWT resource-server authorization.
2. **Independent Mock Provider:** HTTP provider implemented in Python with SQLite-backed idempotency and configurable failure responses.
3. **Input Simulator:** separate Java JVM processes coordinated by Python; generates real HTTP requests using steady, ramp-up, burst and soak profiles, with controllable idempotency-key collisions and independent reports.

**AI-assisted software engineering.** I chose to work with AI models not merely as coding assistants, but as active partners in technical discovery, architectural reasoning, and critical review. GPT-6 (High) helped shape the architecture and challenged my decisions—for example, making a compelling case for RabbitMQ over Kafka, a choice that proved well suited to this project's requirements. Gemini 3.1 Pro (High) served as an independent architectural reviewer and contributed original ideas, including treating the incoming-transaction simulator as a first-class deliverable alongside the external Mock Provider. DeepSeek V4.1-Flash-1017 handled implementation, using Max reasoning for structural changes, High for feature development, and Low for smaller fixes. This combination gave me access to competing technical perspectives rather than a single assistant agreeing with my assumptions, while keeping implementation economically efficient by matching reasoning effort to task complexity. I retained responsibility for the decisions and used executable tests to challenge our conclusions.

## Architecture and repository layout

| Directory | Purpose |
| --- | --- |
| `core/` | Request admission, JSON contracts and policy |
| `store-postgres/` | Durable state, idempotency, dispatch records and outbox |
| `runtime-delivery/` | Backend-agnostic worker orchestration and shared provider processing, with RabbitMQ and PostgreSQL delivery adapters |
| `adapter-springboot/` | HTTP API, OAuth2 resource server, Flyway migrations |
| `provider/` | Independent SQLite-backed HTTP provider simulator |
| `simulator/` | Workload runner, host coordination, evidence verification and reporting |
| `scripts/` | Build, local demo, security fixture and diagnostic commands |
| `tests/` | Executable verification, including integration and fault scenarios |
| `docs/REAL-MULTIHOST-PROTOCOL.md` | Operator instructions for independently provisioned Linux hosts |

`adapter-httpjdk/` and `lab-java-providers/` provide alternative and laboratory adapters; they are not the intended Spring Boot deployment.

## Requirements

- Java 21 JDK and Maven (for the Spring Boot application)
- PostgreSQL 17; RabbitMQ 4.4 only when using the RabbitMQ delivery backend
- Python 3.12+; `cryptography` for signing and independent provider evidence
- Python `PyYAML` only when loading YAML workload profiles
- Docker only when using the optional local Keycloak example

Services must be configured explicitly for your environment; do not expose development credentials or the sample provider on public networks.

## Build and execute

```bash
mvn -B -ntp -f adapter-springboot/pom.xml package
java -jar adapter-springboot/target/james-springboot-0.10.0-SNAPSHOT.jar
```

The Spring Boot application requires PostgreSQL and authentication configuration; RabbitMQ connectivity is needed only in RabbitMQ delivery mode. Database migrations run using Flyway. Its JWT resource-server mode is selected by default (`APP_AUTH_MODE=jwt`). Configure `APP_JWT_ISSUER`, `APP_JWT_JWKS_URI` and `APP_JWT_AUDIENCE` for your identity provider. Writer operations require `activation:write`, and protected reads require `activation:read`. The service verifies JWTs; it does not issue them.

For an isolated development environment with PostgreSQL and RabbitMQ already running:

```bash
python3 -u scripts/dev_demo.py
```

For a disposable, loopback-only local Keycloak realm (Docker required):

```bash
python3 scripts/local_keycloak.py up
eval "$(python3 scripts/local_keycloak.py env --provider local)"
# Start the application in another shell configured with the exported values.
python3 scripts/local_keycloak.py status
python3 scripts/local_keycloak.py down
```

Use only local/test credentials with these helpers. The optional `APP_AUTH_MODE=lab` is restricted to isolated test scenarios, not production deployments.

## API and processing model

The public activation endpoint is `POST /api/v1/requests`. An admitted request persists its business identity and immutable payload before asynchronous dispatch. Repeat submissions with the same identity are reconciled by idempotency rules; conflicting payload reuse is rejected. Workers process eligible operations, record dispatch and settlement, and reconcile external provider observations with durable request state.

External effects depend on the provider's idempotency contract. Retries can be at least once; the service does not promise universal exactly-once execution or independently attested external side effects.

## Delivery backend selection

The application supports two **startup-selected** delivery backends. Both preserve
PostgreSQL as the authoritative store for admission, request state, idempotency,
dispatch and settlement. This is a deployment choice, **not** a hot-swappable
backend within a running cluster.

```bash
# Default: quorum-queue delivery through RabbitMQ (requires a running RabbitMQ server)
export ACTIVATION_DELIVERY_BACKEND=rabbitmq

# Alternative: PostgreSQL-only delivery (no RabbitMQ server required)
export ACTIVATION_DELIVERY_BACKEND=postgres
export ACTIVATION_PG_WORKERS=4  # per service JVM; supported range 1..64
```

In PostgreSQL mode, workers consume due entries from the existing transactional
`p9_outbox` using `SELECT ... FOR UPDATE SKIP LOCKED` and short committed claim
transactions. They execute the same fenced business transitions, reconciliation
and provider gateway as RabbitMQ workers, and acknowledge a queue entry only
after the processing handler returns. Worker failures leave durable entries
eligible for redelivery after their claim leases expire; business idempotency
and provider-side idempotency remain essential. No extra PostgreSQL tables or
schema migrations are required.

**Trade-offs / operational limitations:** PostgreSQL polling adds queries and
connection pressure to the same database that stores business state; under
high contention or sustained traffic it may have higher latency or lower
throughput than RabbitMQ. PostgreSQL mode does not provide RabbitMQ's
broker-specific consumer prefetch, independent queue scaling or broker-side
backpressure characteristics. Its 30-second queue-claim lease can cause
redelivery of slow or interrupted tasks; duplicate hints are handled through
fenced PostgreSQL state transitions, but *exactly-once external side effects*
are not guaranteed by either backend. Capacity and failure behavior must be
measured for the actual workload: this comparison is an architectural
caution, **not** a benchmark claiming that RabbitMQ is always faster.

**Validation maturity:** The PostgreSQL-only delivery backend is fully implemented and has passed local integration and concurrency tests. However, it has undergone substantially less extensive testing than the RabbitMQ backend, particularly regarding prolonged workloads, failure recovery, and adversarial concurrency scenarios. RabbitMQ therefore remains the default and the more thoroughly validated option.

**Switching modes:** Set the same backend value across all participating
service JVMs. Do not mix backends in a live deployment or switch while
RabbitMQ holds unconsumed deliveries: first drain and quiesce the existing
workers and reconcile pending work. The PostgreSQL outbox's `published_ms`
is used as a completed queue-hint acknowledgment in PostgreSQL mode, whereas
RabbitMQ mode records the confirmed publication to the broker. A live
migration requires a separate, explicitly tested procedure.

## Input simulator

The simulator communicates through the public HTTP API, not directly with PostgreSQL or RabbitMQ. Run only against systems you own or have permission to test.

```bash
./simulator/build.sh
python3 simulator/run.py --config simulator/examples/steady.yaml \
  --url http://127.0.0.1:18081 \
  --output-dir simulator/results/example-steady
python3 simulator/compare.py \
  simulator/results/example-steady/summary.json \
  simulator/results/another-run/summary.json \
  --output-prefix simulator/results/comparison
```

Set `JAMES_SIM_WRITE_TOKEN` and optionally `JAMES_SIM_READ_TOKEN` through the process environment. Non-loopback targets require `--allow-remote`. The tool supports multiple JVMs, resource caps, planned arrival schedules, collision distributions and workload comparison. See [`simulator/README.md`](simulator/README.md) for options, result semantics and the signed multi-host workflow.

## Run verification

```bash
./scripts/build.sh
./scripts/test-http-facade.sh
python3 -m unittest discover -s simulator/tests -v
./scripts/test-spring-real.sh --output /tmp/james-spring-acceptance.json
```

The last command requires local PostgreSQL and the provider test dependencies; RabbitMQ is required in the default backend. The CI includes a separate PostgreSQL-only delivery job. It provisions isolated database and queue names for the test run. The GitHub Actions workflow `.github/workflows/spring-real-integration.yml` runs a focused set of independent real-infrastructure integration scenarios. Source files under `tests/` are runnable verification tooling, not evidence that every environment or deployment has been certified.

## Security and operational limits

- JWT mode requires trusted issuer/JWKS/audience configuration; never enable HTTP-based JWKS except for the explicit loopback development option.
- Keep private signing keys, service tokens and provider credentials out of Git and shared evidence bundles.
- PostgreSQL, the selected delivery backend and provider availability govern delivery and recovery behavior; size database pools, heap and worker concurrency to match the deployment.
- Gateway and simulator ingress throughput is not the same metric as completed provider activations.
- Signed evidence authenticates possession of independently enrolled keys; it does not prove that a provider is truthful or that hosts are physically independent.
- The included service and simulators are engineering examples; production operation requires appropriate security hardening, observability, deployment configuration and independent acceptance testing.

For a multi-host simulation with pinned keys, clock checks and offline evidence validation, follow [`docs/REAL-MULTIHOST-PROTOCOL.md`](docs/REAL-MULTIHOST-PROTOCOL.md).
