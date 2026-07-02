# TaskMQ

A simple HTTP API built with Go, featuring dependency injection, modular structure, and configuration management.

## Tech Stack

-   **Language**: Go 1.25+
-   **Web Framework**: [Echo v5](https://echo.labstack.com/)
-   **Dependency Injection**: [Uber Fx](https://github.com/uber-go/fx)
-   **Logging**: [Zap](https://github.com/uber-go/zap)
-   **Configuration**: [Viper](https://github.com/spf13/viper)

## Features

-   Modular architecture (Handler, Server, Logger, Config)
-   Global configuration support (YAML and Environment Variables)
-   Structured logging
-   Graceful shutdown via Echo v5 server context lifecycle
-   Integration tests
-   In-memory thread-safe user repository

## Getting Started

### Prerequisites

-   Go 1.25 or higher installed

### Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/twn39/taskmq.git
    cd taskmq
    ```

2.  Install dependencies:
    ```bash
    go mod tidy
    ```

### Running the Application

Run the server:
```bash
go run cmd/server/main.go
```

The server will start on port `8080` (default). Access the JSON API at `http://localhost:8080`.

### Configuration

Configuration is managed via `config.yaml` or environment variables (prefix `TASKMQ_`).

Default `config.yaml`:
```yaml
server:
  port: ":8080"
logger:
  level: "info"
```

To override via environment variable:
```bash
TASKMQ_SERVER_PORT=:3000 go run cmd/server/main.go
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
