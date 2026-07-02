# AGENTS.md

## Dev environment tips
- **Run Server**: Use `go run cmd/server/main.go` to start the application. (Default port: 8080)
- **Dependencies**: Run `go mod tidy` to ensure `go.mod` and `go.sum` are up to date.
- **Linting**: Run `golangci-lint run` to check for code style and potential errors. Ensure your `golangci-lint` binary matches the configuration version (v2).
- **Configuration**:
    - `config.yaml` is the default config file.
    - Set `APP_ENV` environment variable to load specific configs (e.g., `export APP_ENV=dev` loads `config.dev.yaml`).
    - Supported environments: `dev`, `uat`, `prod` (create corresponding `config.<env>.yaml` files).
    - Environment variables with prefix `TASKMQ_` can override settings (e.g., `TASKMQ_SERVER_PORT=:3000`).

## Testing instructions
- **Run All Tests**: `go test ./...`
- **Integration Tests**: `go test ./tests/integration/... -v`
- **Linting**: Ensure `golangci-lint run` passes with valid output (exit code 0) before pushing.
- **Fixing Issues**: if `golangci-lint` fails, fix the reported issues. Note that `gofmt` and `typecheck` are disabled in the current configuration.

## Project Structure
- `cmd/server/main.go`: Application entry point.
- `internal/`:
    - `config`: Configuration loading via Viper.
    - `handler`: HTTP handlers and routing logic (Echo).
    - `logger`: Structured logging setup (Zap).
    - `server`: Server lifecycle and Fx dependency injection setup.
- `tests/integration`: Integration tests folder.

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
