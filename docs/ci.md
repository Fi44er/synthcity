# CI (GitHub Actions)

Конфигурация: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). Запускается на каждый Pull Request и на каждый push в `main`.

| Job | Что проверяет | Если упал |
|---|---|---|
| `lint` | `go vet` и `golangci-lint` (конфиг `.golangci.yml`); только по новому коду | Откройте лог, исправьте замечание. Локально: `golangci-lint run` |
| `test` | `go build ./...` и `go test -race -cover ./...` | Локально: `go test -race ./...` |
| `proto` | `buf lint`, `buf breaking` (против `main`), актуальность `api/gen` | Устарел `api/gen` → `make gen-proto` и закоммитить |
| `docker (<сервис>)` | Образ каждого сервиса собирается (без публикации) | Локально: `docker build -f deployments/docker-compose/dockerfiles/Dockerfile --build-arg SERVICE_PATH=services/<сервис>/cmd .` |
| `ci-ok` | Сводка: зелёный, только если зелёные все остальные | Это единственная обязательная проверка для `main` |

## Как читать результат

На странице PR внизу: ✅ — всё прошло, 🟡 — идёт, ❌ — упало (**Details** → раскрыть красный шаг).
После исправления достаточно `git push` — проверки запустятся заново.

## Проверка до push

```bash
go vet ./... && go build ./... && go test -race -cover ./...
golangci-lint run
buf lint && buf generate && git diff --exit-code api/gen
```

## Кэширование

- Go modules и build cache — автоматически (`actions/setup-go`).
- Слои Docker — `type=gha` (кэш GitHub Actions), отдельный для каждого сервиса.

## Защита ветки `main`

Pull Request обязателен; обязательная проверка — `ci-ok`; force push и удаление ветки запрещены.
