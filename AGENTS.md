# AGENTS.md

## Dev environment tips
- **Run Server**: Use `go run cmd/server/main.go` to start the application. (Default port: 8080)
- **Dependencies**: Run `go mod tidy` to ensure `go.mod` and `go.sum` are up to date.
- **Linting**: Run `golangci-lint run` to check for code style and potential errors. Ensure your `golangci-lint` binary matches the configuration version (v2).
- **Frontend Dev**: Run `npm run dev` in `web/` directory to watch for CSS changes. Ensure `npm run build` is run before deployment.
- **Database Migrations**:
    - Run `go run cmd/migrate/main.go` to apply migrations (defaults to `up`).
    - Use `go run cmd/migrate/main.go -direction=down` to rollback.
    - Migration files are located in `migrations/`.
    - **Configuration**:
    - `config.yaml` is the default config file.
    - Set `APP_ENV` environment variable to load specific configs (e.g., `export APP_ENV=dev` loads `config.dev.yaml`).
    - Supported environments: `dev`, `uat`, `prod` (create corresponding `config.<env>.yaml` files).
    - Environment variables with prefix `GOCMS_` can override settings (e.g., `GOCMS_SERVER_PORT=:3000`).

## Testing instructions
- **Run All Tests**: `go test ./...`
- **Integration Tests**: `go test ./tests/integration/... -v`
- **Linting**: Ensure `golangci-lint run` passes with valid output (exit code 0) before pushing.
- **Fixing Issues**: if `golangci-lint` fails, fix the reported issues. Note that `gofmt` and `typecheck` are disabled in the current configuration.

## Project Structure
- `cmd/server/main.go`: Application entry point.
- `cmd/migrate/main.go`: Database migration tool entry point.
- `migrations/`: SQL migration files (`.up.sql`, `.down.sql`).
- `internal/`:
    - `config`: Configuration loading via Viper.
    - `database`: Database connection and GORM setup (SQLite).
    - `handler`: HTTP handlers and routing logic (Echo).
    - `logger`: Structured logging setup (Zap).
    - `server`: Server lifecycle and Fx dependency injection setup.
- `web/`: Frontend assets (Tailwind CSS, Vite config).
    - `main.css`: Main CSS file with Tailwind directives.
    - `views/`: HTML templates.
    - `static/`: Generated static assets (do not edit manually).
- `tests/integration`: Integration tests folder.
