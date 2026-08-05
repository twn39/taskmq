# AGENTS.md (AI Agent Guidelines)

This file serves as the "README for machines" in this repository. It provides context, rules of engagement, and commands for AI Coding Agents (such as Antigravity, Claude Code, Cursor, Roo Code, and Aider).

## 🚀 Tech Stack Overview
- **Language**: Go 1.23
- **Dependency Injection**: Uber Fx (`go.uber.org/fx`)
- **HTTP Framework**: Echo v5 (`github.com/labstack/echo/v5`)
- **Config Management**: Viper (`github.com/spf13/viper`)
- **Logging**: Zap (`go.uber.org/zap`)
- **Database / Cache**: Redis Client v9 (`github.com/redis/go-redis/v9`)
- **Task Queue**: TaskMQ (custom distributed Redis-backed queue)

---

## 🛠️ Build, Lint & Test Commands

Run all commands from the workspace root directory:

| Task | Command | Description |
|---|---|---|
| **Build** | `go build ./...` | Compile the entire project. |
| **Run Server** | `go run cmd/server/main.go` | Start the HTTP & gRPC server (default port `8080`). |
| **Mod Tidy** | `go mod tidy` | Ensure `go.mod` and `go.sum` are up to date. |
| **Lint (local optional)** | `golangci-lint run` | Not enforced in CI; config in `.golangci.yml`. |
| **Key schema** | `./scripts/check_keys_schema.sh` | Ensure Redis key literals only live in `keys` package. |
| **Unit Tests** | `go test ./internal/... ./tests/unit/... -count=1` | Unit tests (miniredis where needed). |
| **Coverage config** | [`coverage.yaml`](coverage.yaml) | Per-package floors, report packages, exclude globs, `covermode`. |
| **Coverage gates** | `./scripts/check_coverage.sh` or `make cover-gate` | Enforce floors from `coverage.yaml`. |
| **Coverage report** | `./scripts/check_coverage.sh --report` or `make cover` | Gates + `coverage.out` / `coverage.html`. |
| **Integration Tests** | `go test ./tests/integration/... -v` | Needs Redis on `localhost:6379`. |
| **Cluster smoke** | `TASKMQ_REDIS_CLUSTER_ADDRS=… go test ./tests/integration/ -run ClusterHashTag` | Optional; see `docker-compose.cluster.yml`. |
| **All Tests** | `go test ./...` | Unit + integration (Redis required). |
| **Update Graph** | `codegraph build . -e third_party/` | Rebuild the codebase knowledge graph. |
| **CI** | `.github/workflows/ci.yml` | Unit + race + coverage gates/artifact, lint, integration (+ nightly race/cluster). |

---

## 📂 Project Structure
- `api/proto/taskmq/v1/`: Protobuf API definition for the distributed queue.
- `cmd/server/main.go`: Application entrypoint.
- `internal/`:
  - `config/`: Configuration manager utilizing Viper.
  - `handler/`: HTTP Echo request handlers.
  - `logger/`: Zap structured logger.
  - `redis/`: Redis connection client initialization.
  - `server/`: HTTP Echo server lifecycle setup.
  - `taskmq/`: Distributed task queue (worker pool, scheduler, janitor, cron manager).
- `tests/integration/`: Integration and flow tests.

---

## ⚙️ Configuration & Environment
- **Default Config**: Loaded from `config.yaml` at root.
- **Environment Overrides**: Set `APP_ENV` to load environment-specific config files (e.g., `APP_ENV=dev` loads `config.dev.yaml`).
- **Env Variable Override Prefix**: Variables starting with `TASKMQ_` override yaml settings (e.g., `TASKMQ_SERVER_PORT=:3000`).

---

## 🤖 AI Agent Rules of Engagement

### 1. Codebase Knowledge Graph (`.codegraph/`)
This project maintains a codebase knowledge graph at `.codegraph/`.
- **Read First**: Before answering architecture, design, or layout questions, you **MUST** read [.codegraph/README.md](.codegraph/README.md) to understand modularity, god nodes, and component structure.
- **Use Nodes & Components**: Leverage [.codegraph/components/](.codegraph/components/) and [.codegraph/nodes/](.codegraph/nodes/) to navigate boundaries and symbol definitions instead of scanning raw files.
- **Keep Graph Synced**: Rebuild the graph using `codegraph build . -e third_party/` whenever you create, delete, or modify code files. Proactively remind the user to do the same.
- **AI Architectural Insights**: Maintain the `## AI Architectural Insights` section in [.codegraph/README.md](.codegraph/README.md). If missing, run a deep review and write findings using Chinese as requested by `.codegraph/AGENT_PROMPT.md`.

### 2. Implementation Guidelines (DOs and DON'Ts)
- **DO** use Uber Fx lifecycle hooks (`fx.Hook`) to register startups/shutdowns of background loops or servers.
- **DO** decouple third-party/external calls and sub-components (like queue runners) by declaring them as separate Fx providers and injecting them via constructor options. Use `client.Producer` (`client.NewProducer`) when sub-components only require enqueue capabilities without administrative controls.
- **DO** use `settleCtx` (`context.WithoutCancel(parent)` with bounded timeout) for all broker settlement actions (`MoveToDLQ`, `ScheduleRetry`, `CompleteTask`) so Redis outcome transitions always execute cleanly even if the task execution context was cancelled or timed out.
- **DO** pre-load Lua scripts (`lifecycle.LoadScripts`) before pipeline execution (`rdb.Pipeline()`) to ensure script SHA1 cache hits and prevent `NOSCRIPT` errors in Redis pipelines.
- **DO** leverage `EnqueueBulk` with micro-batch chunking (1,000 tasks/chunk) for high-throughput batch ingestion of both unique and non-unique tasks.
- **DO** use the custom `BinaryCodec` for high-performance and zero-allocation serialization in TaskMQ where performance is critical.
- **DO** keep Redis key literals in the `keys` package only; multi-key Lua must stay hash-tagged under `{queue}` (see `docs/LUA_SCRIPTS.md`, `scripts/check_keys_schema.sh`).
- **DO** follow terminal-outcome rules in `docs/TERMINAL_OUTCOMES.md`: handlers return `task.SkipRetry`/`error`; middleware returns `worker.Handled`/`Abort`; never XACK from handlers. Extend `tests/integration/settlement_matrix_test.go` for Redis-visible outcomes.
- **DO** share one `*lifecycle.Lifecycle` between client and workers in production (`BuildWorkerTopologyWithLifecycle` / Fx module). Bare `BuildWorkerTopology` is tests/tools only.
- **DON'T** introduce circular dependency chains across packages. Keep packages clean and single-purpose.
- **DON'T** swallow errors. Log them with Zap structured context (`zap.Error(err)`) and return them.
- **DON'T** ignore lint failures. Although `gofmt` and `typecheck` linters may be disabled or bypassed on external libraries, your Go files must compile cleanly with `go build ./...`.
- **DON'T** re-export `worker.ErrHandled` to application handlers or block settlement on best-effort meta/events/metrics writes.

## codegraph-gen

This project maintains a codebase knowledge graph at `.codegraph/`.

### Guidelines for AI Agents (Antigravity, Claude Code, Cursor, Roo Code, etc.)

You MUST follow these rules when working in this codebase:

1. **Prioritize the Knowledge Graph**:
   - Before answering architecture, design, or codebase structure questions, you **MUST** read [.codegraph/README.md](.codegraph/README.md) to understand the system overview, god nodes, and logical community structure.
   - Use [.codegraph/components/](.codegraph/components/) and [.codegraph/nodes/](.codegraph/nodes/) to navigate component boundaries, file relationships, and symbol definitions. This is much faster and more token-efficient than reading raw source files directly.

2. **AI Architectural Insights**:
   - Check [.codegraph/README.md](.codegraph/README.md) for a section titled `AI Architectural Insights`.
   - If this section is missing, incomplete, or contains placeholders, read [.codegraph/AGENT_PROMPT.md](.codegraph/AGENT_PROMPT.md), perform a deep architectural analysis of the project, and write your report into that section. Do not overwrite other sections.

3. **Keep Graph Sync'd**:
   - Whenever you create, delete, or modify code files, you **SHOULD** remind the user to run `codegraph build .` to rebuild the knowledge graph and keep it current.
   - When running the build command, exclude irrelevant or generated directories (e.g., third-party dependencies, build folders, or documentation) using the `-e`/`--exclude` flag to keep the graph focused and clean (e.g., `codegraph build . -e third_party/`).
