# GoCMS

A simple HTTP API built with Go, featuring dependency injection, modular structure, and configuration management.

## Tech Stack

-   **Language**: Go 1.25+
-   **Web Framework**: [Echo v4](https://echo.labstack.com/)
-   **Dependency Injection**: [Uber Fx](https://github.com/uber-go/fx)
-   **ORM**: [GORM](https://gorm.io/) (SQLite)
-   **Logging**: [Zap](https://github.com/uber-go/zap)
-   **Configuration**: [Viper](https://github.com/spf13/viper)
-   **Frontend**: HTML Templates, Tailwind CSS (via Vite)

## Features

-   Modular architecture (Handler, Server, Logger, Database, Config)
-   Global configuration support (YAML and Environment Variables)
-   Structured logging
-   Graceful shutdown
-   Integration tests
-   Modern UI with Tailwind CSS

## Getting Started

### Prerequisites

-   Go 1.25 or higher installed
-   Node.js & npm (for frontend assets)

### Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/twn39/gocms.git
    cd gocms
    ```

2.  Install dependencies:
    ```bash
    go mod tidy
    ```

### Frontend Development
2.  Install dependencies:
    ```bash
    cd web
    npm install
    ```

3.  Build assets (development with watch):
    ```bash
    npm run dev
    ```

4.  Build assets (production):
    ```bash
    npm run build
    ```
### Database Migrations

Run migrations to set up the database schema:

```bash
# Apply migrations (Up)
go run cmd/migrate/main.go

# Rollback migrations (Down)
go run cmd/migrate/main.go -direction=down
```
### Running the Application

Run the server:
```bash
go run cmd/server/main.go
```

The server will start on port `8080` (default). Access the updated UI at `http://localhost:8080`.

### Configuration

Configuration is managed via `config.yaml` or environment variables (prefix `GOCMS_`).

Default `config.yaml`:
```yaml
server:
  port: ":8080"
database:
  dsn: "gocms.db"
logger:
  level: "info"
```

To override via environment variable:
```bash
GOCMS_SERVER_PORT=:3000 go run cmd/server/main.go
```

## API Endpoints

-   `GET /`: Hello message
-   `GET /users`: List all users
-   `POST /users`: Create a new user
    -   Body: `{"name": "Name", "email": "email@example.com"}`

## Testing

Run integration tests:
```bash
go test ./tests/integration/... -v
```

## Linting

To run static code analysis, install [golangci-lint](https://golangci-lint.run/usage/install/) and run:
```bash
golangci-lint run
```

