# SynthCity — анализ проекта и план реализации

> Симуляция города в реальном времени: машины, спецтранспорт, светофоры, перекрёстки; позже — пешеходы, подземные переходы, управление временем и количеством агентов.
> Стек: Go, gRPC + grpc-gateway, **Kafka (вместо NATS)**, PostgreSQL/PostGIS, OpenTelemetry (LGTM).

---

## Оглавление

1. [Главные решения (TL;DR)](#1-главные-решения-tldr)
2. [Анализ текущего состояния проекта](#2-анализ-текущего-состояния-проекта)
3. [Целевая архитектура](#3-целевая-архитектура)
4. [Kafka: топики, сообщения, настройки](#4-kafka-топики-сообщения-настройки)
5. [Система координат и формат геометрии для фронта](#5-система-координат-и-формат-геометрии-для-фронта)
6. [map-service: ingest, хранилище, API](#6-map-service-ingest-хранилище-api)
7. [simulation-service: движок симуляции](#7-simulation-service-движок-симуляции)
8. [api-gateway: REST и WebSocket](#8-api-gateway-rest-и-websocket)
9. [Спецтранспорт](#9-спецтранспорт)
10. [Пешеходы и подземные переходы — как заложить сейчас](#10-пешеходы-и-подземные-переходы--как-заложить-сейчас)
11. [Миграция с NATS на Kafka — чеклист](#11-миграция-с-nats-на-kafka--чеклист)
12. [Наблюдаемость и тестирование](#12-наблюдаемость-и-тестирование)
13. [Структура репозитория](#13-структура-репозитория)
14. [Дорожная карта по фазам](#14-дорожная-карта-по-фазам)
15. [Риски и открытые вопросы](#15-риски-и-открытые-вопросы)

---

## 1. Главные решения (TL;DR)

| # | Решение | Почему |
|---|---------|--------|
| 1 | **`map-service` = статический, версионируемый мир** (ingest OSM, здания/дороги, чанки, предпросмотр маршрута). **`simulation-service` = динамический мир** (машины, светофоры, время). | Чёткая граница: статика читается много раз и кэшируется, динамика меняется 20 раз в секунду и живёт в памяти. |
| 2 | **Светофоры и машины живут в одном процессе** (`simulation-service`), а не в отдельном `traffic-service`. | Проверка «можно ли ехать» происходит каждый тик для каждой машины у перекрёстка. gRPC `CheckPermission` на каждую машину убьёт производительность и сделает симуляцию недетерминированной. Текущий `traffic.proto` → удалить/переделать. |
| 3 | **Ядро симуляции — один писатель (single-writer) + фиксированный шаг `dt`** и масштаб времени (`speed_factor`). Команды применяются только на границе тика. | Нет мьютексов, детерминизм, простое ускорение/замедление/пауза. |
| 4 | **Kafka — для потоков и команд, gRPC — для запрос-ответ.** | Kafka даёт развязку, буфер, повторное чтение, несколько потребителей (запись/реплей/аналитика). Для «получи данные прямо сейчас» gRPC проще. |
| 5 | **Снимки позиций машин — по Kafka 10 раз/сек (wall-clock), протобуф, 1 сообщение на тик.** Фронт интерполирует. | Не нужно слать каждый шаг симуляции; при ускорении ×10 поток не растёт. |
| 6 | **Фронту отдаётся статическая геометрия в локальных метрах** (формат из вашего ТЗ + небольшие расширения), **кусками (чанками) по ячейкам сетки**, с кэшированием. | Нельзя отдавать весь город одним JSON; чанки позволяют грузить только видимую область. |
| 7 | **Ingest OSM — отдельная команда (CLI), не часть старта сервера.** Результат: граф (бинарник) + PostGIS. | Парсинг PBF — минуты; сервер должен стартовать за секунды. |
| 8 | **Библиотека для Kafka — `franz-go`** (чистый Go, без cgo, есть плагин OpenTelemetry `kotel`). Брокер — Kafka в режиме KRaft (без ZooKeeper). | Быстрее и проще в сборке, чем `confluent-kafka-go`; нативная интеграция с вашим OTel-стеком. |

---

## 2. Анализ текущего состояния проекта

### 2.1. Что есть

```
synthcity/
├── api/proto/{common,map,traffic,vehicle,test}/v1   # gRPC-контракты (buf)
├── services/
│   ├── api-gateway/    # Fiber + grpc-gateway (REST→gRPC). Пустые папки: transport/websocket, transport/nats, middleware
│   ├── map-service/    # OSM PBF → граф дорог (RoadGraph), A*, R-tree, msgpack-кэш, gRPC
│   └── hi-service/     # демо-сервис (gRPC + NATS) — удалить
├── pkg/{logger,telemetry,natslog}                    # zap+OTel, метрики, NATS-middleware
├── deployments/                                      # compose: nats, redis, postgis, otel, loki, tempo, prometheus, grafana, cadvisor
└── temp/                                             # черновики: NATS pub/sub, LocalGrid, a.md (спека геоданных), эталон JSON
```

**Что реализовано и работает (по коду):**

- Парсинг PBF и построение дорожного графа (`graph_processor.go`): скорости, полосы, oneway.
- A* с учётом штрафов за светофор/переход (`route/service.go`).
- R-tree для поиска ближайшего узла (`spatial.go`), бинарный кэш графа (`persistence.go`).
- gRPC `MapService` (GetRoute, GetNearestNode, GetJunctionInfo, UpdateEdgeWeight) + REST через grpc-gateway.
- `LocalGrid` (`pkg/geoconverter`) — перевод WGS84 → ECEF → ENU (локальные метры) и индексы ячеек. Это **ровно то, что нужно для формата `origin_gps` + `cell_size`**.
- Наблюдаемость: OTel (трейсы/метрики/логи) → Loki/Tempo/Prometheus/Grafana.
- Модель `StaticObject` + репозиторий PostGIS (заготовка, не подключена).

**Чего нет:**

- Симуляции вообще (нет сервиса, нет тика, нет модели машин).
- Светофоров как объектов с фазами (есть только `traffic_signals` как тип узла — и тот не проставляется, см. ниже).
- Статического окружения (здания, парки, вода): `StaticProcessor.ProcessWay/ProcessRelation` пустые.
- WebSocket и любого стриминга на фронт.
- Kafka (пока NATS, который по факту используется только в демо `hi-service`).
- Миграций БД (папка `deployments/postgres/init` пуста).
- `map-service` и `simulation-service` в docker-compose.

### 2.2. Найденные проблемы (исправить в Фазе 0)

| Файл | Проблема | Что сделать |
|------|----------|-------------|
| `map-service/.../static_processor.go` | Опечатка `Tags.Find("higway")` — светофоры никогда не находятся; внутри `fmt.Printf` на каждый объект; `highway=crossing` не обрабатывается. | `"highway"`, убрать `Printf`, добавить `crossing`. |
| `map-service/.../loader.go` + `graph_processor.go` | `Node.Type` **никогда не проставляется** → штрафы `NodeTrafficLight/NodeCrossing` в A* не работают. | При чтении узла с тегом `highway=traffic_signals` / `crossing` выставлять `Type` до построения рёбер. |
| `loader.go` | `allNodeMeta` хранит **все** узлы PBF в памяти — для региона это гигабайты. Ошибка сканера → `panic`. | Вариант А: предварительно обрезать PBF `osmium extract -b ...` до зоны симуляции. Вариант Б: многопроходный ingest (см. §6.2). Вернуть `error` вместо `panic`. |
| `loader.go` | `logger.FromContext(context.Background())` отдаёт no-op логгер (в контексте нет логгера) — прогресс загрузки не виден. Телеметрия инициализируется **после** загрузки графа. | Передавать логгер явно; `telemetry.Init` до `New`. |
| `graph.go` `AddNode` | Узел вставляется в R-tree **на каждый сегмент** (дубликаты). | Вставлять только если узла ещё нет в `Nodes`. |
| `graph_processor.go` | `oneway=-1` / `reverse` обрабатывается как прямое направление; `junction=roundabout` не считается односторонним; нет `unclassified`, `*_link` (кроме `motorway_link`), не фильтруются `access=no/private`, `service=parking_aisle/driveway`. | Исправить правила (см. §6.3). |
| `route/service.go` | `gScore` инициализируется для **всех** узлов графа на каждый запрос (O(N)). Нет синхронизации с `UpdateEdgeWeight` (гонка: чтение рёбер без `RLock`). | Ленивая `map` (отсутствие = +∞), читать рёбра под `RLock` или сделать граф неизменяемым и веса — `atomic`. |
| `graph.go` `UpdateEdgeWeight` | Обновляет только одно направление и мутирует общий `*Edge`. | Решить судьбу метода — см. §3.4 (в новой архитектуре он не нужен симуляции). |
| `map-service/.env` vs `config` | В `.env` — `GRPCPort=50054`, в конфиге читается `MAP_GRPC_PORT` (дефолт `50051`). Gateway ждёт `localhost:50054`. | Привести к одному имени: `MAP_GRPC_PORT=50054`. |
| `pkg/telemetry/metrics.go` | `MetricsInterceptor` захардкожен на `"hi-service"`. | Фабрика `NewMetricsInterceptor(serviceName)`. |
| `api/proto/map/v1/map.proto` | Импорты `google/api/annotations.proto` и `buf/validate/validate.proto` **закомментированы**, хотя используются → `buf generate` по этому файлу не соберётся. | Раскомментировать. |
| `postgres/repository.go` | Первичный ключ — только OSM ID, но ID узлов, путей и отношений в OSM **пересекаются**. `Subtype` не сохраняется. `Save` на больших пачках медленный. | Составной ключ `(osm_type, osm_id)`; сохранять `subtype`; `pgx.CopyFrom` или `CreateInBatches` + `ON CONFLICT`. |
| `docker-compose` | Порт `8080` занят и `cadvisor`, и `GATEWAY_HTTP_PORT`; нет `map-service`; ключ `version:` устарел; `.env` с паролями лежит в репозитории. | Развести порты, добавить сервисы, `.env` в `.gitignore`. |
| `api-gateway/app.go` | Префикс `/api` срезается `TrimPrefix`, а паттерны grpc-gateway в proto уже содержат `/api/v1/...`. | Проверить, что маршруты реально находятся (интеграционный тест на `/api/v1/map/route`). |
| `temp/mddoc/a.md` | Рекомендует `qedus/osmpbf`, а в проекте используется `paulmach/osm/osmpbf` (это нормально и лучше — он умеет отношения). | Обновить документ под фактический стек. |
| Данные | Пример JSON с `origin_gps` — Гётеборг (57.7089, 11.9746), а PBF/спека — Оренбург (≈51.77, 55.10). | `origin_gps` должен вычисляться из bbox загруженного PBF (центр), а не быть константой. |

---

## 3. Целевая архитектура

### 3.1. Сервисы и ответственность

```text
                                   ┌──────────────────────────┐
                                   │        Frontend          │
                                   │  (Three.js / Unity / ... │
                                   └───────▲───────────┬──────┘
                       REST (чанки, команды)│           │ WebSocket (бинарный протобуф)
                                           │           │
                                   ┌───────┴───────────▼──────┐
                                   │       api-gateway        │
                                   │  Fiber + grpc-gateway    │
                                   │  WS-хаб, подписки на     │
                                   │  ячейки, ack команд      │
                                   └──┬────────▲───────────┬──┘
                          gRPC        │        │ consume   │ produce
                  (карта, запросы)    │        │           │ (команды)
                                      │   ┌────┴───────────▼─────────────────────────┐
                                      │   │                 KAFKA                    │
                                      │   │ sim.commands | sim.command-results       │
                                      │   │ sim.snapshots | sim.events | sim.lights  │
                                      │   │ sim.status (compact)                     │
                                      │   └────▲───────────┬─────────────────▲───────┘
                                      │        │ produce   │ consume         │ consume
                                      │   ┌────┴───────────▼──────┐   ┌──────┴─────────┐
                                      │   │   simulation-service  │   │ recorder (опц.)│
                                      │   │ tick-loop, часы, IDM, │   │ запись/реплей, │
                                      │   │ светофоры, спавн,     │   │ аналитика      │
                                      │   │ маршруты (A*)         │   └────────────────┘
                                      │   └──────────▲────────────┘
                                      │              │ gRPC: ExportGraph (1 раз на старте)
                                 ┌────▼──────────────┴───┐        ┌──────────────┐
                                 │      map-service      │───────►│ PostgreSQL + │
                                 │ ingest OSM, чанки,    │        │ PostGIS      │
                                 │ route preview, граф   │        └──────────────┘
                                 └───────────────────────┘
```

| Сервис | Отвечает за | Чем общается | Состояние |
|--------|-------------|--------------|-----------|
| **map-service** | Ingest OSM, граф дорог, статические объекты (здания, парки, вода, светофоры, переходы), выдача чанков в локальных метрах, route preview, nearest node, список точек спавна | gRPC (вход от gateway и simulation), PostGIS | Неизменяемое (версионируется `map_version`) |
| **simulation-service** | Время симуляции, агенты (машины, спецтранспорт), светофоры, перекрёстки, маршрутизация агентов, обработка команд | Kafka (команды ← / снимки, события →), gRPC `ExportGraph` к map-service на старте, gRPC для запросов к состоянию (`GetVehicle`, `ListVehicles`) | В памяти, один писатель |
| **api-gateway** | Единственная точка входа для фронта: REST, WebSocket, авторизация (позже), rate limiting | gRPC → map/simulation, Kafka consume (состояние) / produce (команды) | Кэш последнего состояния для новых клиентов |
| **recorder** (опционально, Фаза 8) | Запись потока снимков/событий для реплея и аналитики | Kafka consume | Файлы/ClickHouse/TimescaleDB |

> `hi-service` удаляется. `traffic-service` как отдельный процесс **не создаётся** (см. решение №2). Если светофоры потом понадобится вынести — это делается за интерфейсом `SignalController` внутри симуляции, а не через gRPC на каждую машину.

### 3.2. Правило выбора транспорта

| Сценарий | Транспорт | Причина |
|----------|-----------|---------|
| Позиции машин, фазы светофоров, события симуляции → фронт/аналитика | **Kafka** | Поток, много потребителей, можно переиграть |
| Команды управления (пауза, скорость, спавн, удаление, перекрытие дороги) | **Kafka** (`sim.commands`) + результат в `sim.command-results` | Упорядоченность в одном партиционе, аудит, переживает рестарт gateway |
| Загрузка чанков карты, route preview, nearest node | **gRPC / REST** | Классический запрос-ответ, кэшируется HTTP |
| Детали одной машины, список машин по фильтру | **gRPC** (simulation) | Нужен немедленный ответ |
| Выгрузка графа в симуляцию | **gRPC server-stream** (`ExportGraph`) на старте | Большой разовый объём |

### 3.3. Поток данных в рантайме

1. Пользователь жмёт «×5» → `POST /api/v1/sim/speed {factor: 5}` → gateway кладёт `Command{SetSpeed}` в `sim.commands`, отвечает `202 {command_id}`.
2. `simulation-service` читает команду, ставит в очередь; **на начале следующего тика** применяет → пишет `CommandResult{ok}` в `sim.command-results` и обновлённый `SimStatus` в `sim.status`.
3. Gateway видит результат → шлёт по WS `Ack{command_id}` и `Status`.
4. Каждые 100 мс (wall-clock) симуляция публикует `WorldSnapshot` в `sim.snapshots`.
5. Gateway держит последний снимок в памяти, раз в 100 мс фильтрует его под ячейки каждого WS-клиента и отправляет бинарный кадр.
6. Фронт интерполирует позиции между кадрами по `sim_time`.

### 3.4. Что делать с `UpdateEdgeWeight` и `traffic.proto`

- **Маршрутизация агентов выполняется внутри `simulation-service`** по своей копии графа (с живыми весами от пробок). Иначе каждое обновление веса — сетевой вызов, а каждый спавн 1000 машин — 1000 gRPC-запросов.
- A* выносится в общий пакет `pkg/routing`, который используют и map-service (route preview), и simulation-service.
- `UpdateEdgeWeight` убрать из рабочего пути; перекрытие дороги — это команда `CloseRoad` в симуляцию.
- `traffic.proto`: сервис `TrafficService` удалить; оставить сообщения `LightState` и `TrafficLight` для событий (переносятся в `sim/v1`).

---

## 4. Kafka: топики, сообщения, настройки

### 4.1. Топики

| Топик | Ключ | Партиции | cleanup / retention | Продюсер → Консьюмеры | Содержимое |
|-------|------|----------|---------------------|-----------------------|------------|
| `sim.commands` | `sim_id` | 1 | delete, 1 ч | gateway → simulation | `Command` |
| `sim.command-results` | `command_id` | 1 | delete, 1 ч | simulation → gateway | `CommandResult` (ok/error, сообщение) |
| `sim.snapshots` | `sim_id` | 1 | delete, **60 с** | simulation → gateway, recorder | `WorldSnapshot` (позиции машин) |
| `sim.events` | `sim_id` | 1 | delete, 24 ч | simulation → gateway, recorder | `SimEvent` (spawn, despawn, arrived, incident, siren_on…) |
| `sim.lights` | `light_id` | 3 | **compact** | simulation → gateway | `LightState` (текущая фаза + `next_change_sim_time`) |
| `sim.status` | `sim_id` | 1 | **compact** | simulation → gateway | `SimStatus` (state, speed_factor, effective_speed, sim_time, vehicle_count) |
| `sim.commands.dlq` | `command_id` | 1 | delete, 7 д | simulation → — | Команды, которые не удалось разобрать |

Почему так:

- **Один партиционный `sim.snapshots`** — снимок целостный и упорядоченный; пока машин ≤ ~20–30k, один партиций хватает. Масштабирование через шардирование по районам — Фаза 8.
- **Compacted `sim.lights` и `sim.status`** — новый клиент/перезапущенный gateway прочитает топик с начала и сразу получит актуальное состояние всех светофоров и часов.
- **Снимки с короткой retention** — это «текущее состояние», история не нужна (для истории есть `recorder`).

### 4.2. Настройки продюсера/консьюмера

| Топик | acks | Идемпотентность | Компрессия | linger |
|-------|------|-----------------|------------|--------|
| `sim.snapshots` | 1 (потеря одного снимка не страшна) | нет | `zstd`/`lz4` | 5 мс |
| `sim.commands`, `sim.events`, `sim.command-results`, `sim.lights`, `sim.status` | all | да | `lz4` | 5–10 мс |

Не забыть в брокере: `message.max.bytes` ≥ 4 МБ для `sim.snapshots` (на случай 30k машин × ~24 байта ≈ 720 КБ — запас в 5×).

**Группы консьюмеров:**

- `simulation-service` читает `sim.commands` в группе `simulation` (один экземпляр на `sim_id`).
- **Каждый экземпляр `api-gateway` — своя группа** (`gateway-<instance_id>`), `ConsumeResetOffset(AtEnd)` для `snapshots/events`. Иначе Kafka разделит сообщения между инстансами (load-balancing), а нам нужен broadcast.
- Для compacted-топиков (`lights`, `status`) gateway при старте читает с начала (`AtStart`) и строит `map[key]latest`.

### 4.3. Контракты (protobuf)

Новый пакет `api/proto/sim/v1/sim.proto` (внутренние события — без HTTP-аннотаций):

```proto
syntax = "proto3";
package sim.v1;
option go_package = "github.com/Fi44er/synthcity/api/gen/go/sim/v1;simv1";

enum VehicleType {
  VEHICLE_TYPE_UNSPECIFIED = 0;
  VEHICLE_TYPE_CAR = 1;
  VEHICLE_TYPE_TRUCK = 2;
  VEHICLE_TYPE_BUS = 3;
  VEHICLE_TYPE_AMBULANCE = 4;
  VEHICLE_TYPE_FIRE_TRUCK = 5;
  VEHICLE_TYPE_POLICE = 6;
}

// Битовые флаги состояния машины
enum VehicleFlag {
  VEHICLE_FLAG_NONE = 0;
  VEHICLE_FLAG_SIREN = 1;
  VEHICLE_FLAG_BRAKING = 2;
  VEHICLE_FLAG_BLINKER_L = 4;
  VEHICLE_FLAG_BLINKER_R = 8;
}

// Координаты — в системе фронта (метры, см. §5.1)
message Vehicle {
  uint32 id = 1;
  VehicleType type = 2;
  float x = 3;
  float z = 4;
  float heading = 5;   // градусы, 0 = восток, 90 = север
  float speed = 6;     // м/с
  uint32 flags = 7;    // VehicleFlag bitmask
}

message WorldSnapshot {
  string sim_id = 1;
  uint64 tick = 2;
  double sim_time = 3;        // секунды симуляционного времени
  int64  wall_time_ms = 4;    // unix ms момента публикации
  repeated Vehicle vehicles = 5;
}

message LightState {
  string light_id = 1;        // "light_<osm_node>_<n>", совпадает с id в геометрии
  enum State { RED = 0; YELLOW = 1; GREEN = 2; OFF = 3; }
  State state = 2;
  double next_change_sim_time = 3;  // клиент сам анимирует таймер
}

message SimStatus {
  string sim_id = 1;
  enum RunState { PAUSED = 0; RUNNING = 1; }
  RunState run_state = 2;
  float speed_factor = 3;       // запрошено: 0.1 … 50
  float effective_speed = 4;    // реально достигнуто (если не успеваем)
  double sim_time = 5;
  uint32 vehicle_count = 6;
  string map_version = 7;
}

message Command {
  string command_id = 1;        // UUID
  string sim_id = 2;
  string issued_by = 3;
  oneof payload {
    SetRunState set_run_state = 10;
    SetSpeed set_speed = 11;
    SpawnVehicles spawn_vehicles = 12;
    RemoveVehicles remove_vehicles = 13;
    CloseRoad close_road = 14;
    CreateIncident create_incident = 15;
    SetSignalProgram set_signal_program = 16;
  }
}
// … сообщения-пейлоады: SetSpeed{factor}, SpawnVehicles{count,type,area,route_mode},
//   RemoveVehicles{ids | area | all}, CloseRoad{road_id|edge_ids, closed}, CreateIncident{x,z,kind}

message CommandResult {
  string command_id = 1;
  bool ok = 2;
  string error = 3;
  repeated uint32 affected_ids = 4;  // например, id заспавненных машин
}

message SimEvent {
  double sim_time = 1;
  oneof event {
    VehicleSpawned vehicle_spawned = 10;
    VehicleRemoved vehicle_removed = 11;   // причина: arrived / removed_by_user / stuck
    SirenChanged siren_changed = 12;
    IncidentCreated incident_created = 13;
    IncidentResolved incident_resolved = 14;
  }
}
```

Заголовки Kafka: `traceparent` (проброс OTel-контекста), `content-type: application/x-protobuf`, `schema-version`.

### 4.4. Клиентская библиотека

Создать `pkg/kafkax` (замена `pkg/natslog`):

```go
// pkg/kafkax/client.go
func NewClient(cfg Config, opts ...kgo.Opt) (*kgo.Client, error) {
    kot := kotel.NewKotel(kotel.WithTracer(...), kotel.WithMeter(...)) // github.com/twmb/franz-go/plugin/kotel
    base := []kgo.Opt{
        kgo.SeedBrokers(cfg.Brokers...),
        kgo.WithHooks(kot.Hooks()...),            // трейсы и метрики в ваш OTel-коллектор
        kgo.ProducerBatchCompression(kgo.Lz4Compression()),
        kgo.ProducerLinger(5 * time.Millisecond),
    }
    return kgo.NewClient(append(base, opts...)...)
}
```

Основные паттерны:

- `Produce(ctx, topic, key, proto.Message)` — маршалинг + заголовки.
- `Consume(ctx, topics, group, handler)` — цикл `PollFetches`, на каждую запись создаётся span из заголовка `traceparent` (аналог вашего `natslog.Middleware`).
- Для broadcast-консьюмера: `kgo.ConsumerGroup("gateway-"+instanceID)`, `kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd())`.

### 4.5. Docker Compose (Kafka KRaft + UI)

Заменить блок `nats` в `docker-compose.infra.yaml`:

```yaml
  kafka:
    image: apache/kafka:3.9.0
    container_name: city-kafka
    restart: always
    ports:
      - "${KAFKA_PORT:-9092}:9092"         # для запуска сервисов с хоста
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_MESSAGE_MAX_BYTES: 5242880
      KAFKA_REPLICA_FETCH_MAX_BYTES: 5242880
    networks: [city-network]
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1"]
      interval: 10s
      retries: 10

  kafka-init:
    image: apache/kafka:3.9.0
    depends_on:
      kafka: { condition: service_healthy }
    networks: [city-network]
    entrypoint: ["/bin/sh", "-c"]
    command: |
      "
      B=kafka:19092; T=/opt/kafka/bin/kafka-topics.sh
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.commands         --partitions 1 --config retention.ms=3600000
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.command-results  --partitions 1 --config retention.ms=3600000
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.snapshots        --partitions 1 --config retention.ms=60000 --config segment.ms=30000 --config max.message.bytes=5242880
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.events           --partitions 1 --config retention.ms=86400000
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.lights           --partitions 3 --config cleanup.policy=compact
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.status           --partitions 1 --config cleanup.policy=compact
      $$T --bootstrap-server $$B --create --if-not-exists --topic sim.commands.dlq     --partitions 1 --config retention.ms=604800000
      "

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: city-kafka-ui
    ports: ["8081:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: synthcity
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:19092
    networks: [city-network]
    depends_on: [kafka]
```

> Для локальной разработки можно вместо Apache Kafka взять Redpanda (один контейнер, Kafka-совместимый API, быстрее стартует) — код сервисов не изменится.

`monitor.sh` заменить на `make monitor-kafka` → открыть Kafka UI или `kcat -b localhost:9092 -t sim.events -C -f '%k %s\n'`.

---

## 5. Система координат и формат геометрии для фронта

### 5.1. Система координат (зафиксировать один раз и не менять)

- **Начало** — `origin_gps` (центр bbox загруженной карты, считается при ingest и хранится в БД/конфиге).
- Перевод WGS84 → метры: существующий `LocalGrid.GeoToLocal` (ENU: `east`, `north`).
- **Оси фронта (как в Three.js, правосторонняя, Y вверх):**
  - `x = east`
  - `y = up` (высота над землёй; для мостов > 0, для тоннелей < 0)
  - `z = −north` (т. е. «на юг» = +z)
- `rotation` и `heading` — **градусы вокруг оси Y, 0 = на восток, 90 = на север** (против часовой при взгляде сверху; в Three.js это `object.rotation.y = radians(rotation)`).
- Единицы — метры, углы — градусы.

```go
// pkg/geoconverter — добавить хелпер
func (g *LocalGrid) GeoToWorld(lat, lon, alt float64) (x, y, z float64) {
    east, north := g.GeoToLocal(lat, lon, 0)
    return east, alt, -north
}
```

> Эту же систему используют `WorldSnapshot` (§4.3) и все объекты геометрии — иначе машины «поедут» мимо дорог.

### 5.2. Формат ответа (ваш формат + минимальные расширения)

Ваш формат сохраняется как есть. Расширения помечены `// +` — они необязательны для старого клиента.

```json
{
  "project_info": {
    "origin_gps": { "lat": 57.708871, "lon": 11.97456 },
    "units": "meters",
    "cell_size": 100,
    "axes": { "x": "east", "y": "up", "z": "south" },        // + явная договорённость об осях
    "map_version": "2026-10-02-a1b2c3",                       // + для кэша и инвалидации
    "bounds": { "min": {"x": -2500, "z": -2500}, "max": {"x": 2500, "z": 2500} }  // + границы мира
  },
  "chunk": { "cx": 3, "cz": -2 },                             // + если ответ — чанк
  "features": [
    {
      "id": "bld_w1042",
      "type": "building",
      "position": { "x": 5, "y": 0, "z": 10 },
      "rotation": 0,
      "geometry": {
        "type": "Polygon",
        "points": [[0,0],[10,0],[10,15],[0,15]],
        "holes": [],                                          // + внутренние дворы (список колец)
        "height": 12.5,
        "floor_height": 3.0,
        "floors": 4
      },
      "metadata": {
        "name": "Жилой дом",
        "address": "ул. Гётеборга, 5",
        "gps": { "lat": 57.7089, "lon": 11.9746 },
        "building": "apartments"                              // + исходный тип OSM
      }
    },
    {
      "id": "light_n55_0",
      "type": "traffic_light",
      "position": { "x": 12.5, "y": 0, "z": 8.2 },
      "rotation": 90,
      "metadata": {
        "state": "active",
        "junction_id": 55,                                    // + id узла графа
        "approach_node": 54                                   // + откуда подъезжают машины
      }
    },
    {
      "id": "road_w01",
      "type": "road",
      "position": { "x": 0, "y": 0, "z": 0 },                 // + якорь (см. правило ниже)
      "geometry": {
        "type": "LineString",
        "points": [[0,10],[50,10],[100,15]],
        "width": 8
      },
      "metadata": {
        "highway": "primary", "lanes": 2, "oneway": false,
        "speed_limit_kmh": 60,
        "layer": 0, "bridge": false, "tunnel": false,         // + для мостов/подземных проездов
        "name": "ул. Гётеборга"
      }
    }
  ]
}
```

### 5.3. Правила формата

1. **`position`** — точка привязки объекта. Для зданий/парков/воды — центроид контура. `geometry.points` — **координаты относительно `position`** в плоскости (x, z): первое число — x, второе — z. Для дорог `position` = {0,0,0}, точки — в мировых координатах (так удобнее соединять сегменты) — либо одинаково для всех типов использовать якорь; главное — **одно правило, зафиксированное в `project_info`**. Рекомендация: **для всех** — якорь + относительные точки, у дорог якорь = первая точка.
2. **Обход контура**: `outer` — против часовой при взгляде сверху (с +Y), `holes` — по часовой. Нормализует backend (после инверсии `z = −north` порядок обхода меняется — не забыть!).
3. **Замкнутость**: последняя точка не дублирует первую (в отличие от GeoJSON).
4. **Высота здания** — по приоритету: `height` (OSM) → `building:levels × floor_height` → значение по умолчанию по типу (`house` 6 м, `apartments` 15 м, `commercial` 12 м, иначе 9 м). `floors = round(height / floor_height)`, если не задан `levels`.
5. **Triangulation на фронте** (earcut / `THREE.ShapeGeometry` с `holes`). Backend отдаёт контуры, а не меши — формат компактнее и стабильнее. Позже, если появится узкое место, можно добавить `"triangles"`.
6. **`rotation` у полигонов = 0** (контур уже в мировой ориентации). Для точечных объектов (светофор, переход, остановка) — направление «лицом» к подъезжающему транспорту: `atan2(north_dir, east_dir)` в градусах.
7. **Ширина дороги**: `width = lanes_total × 3.5 м` (или `width` из OSM, если задан); для `oneway` `lanes` = число полос в одном направлении.
8. **Высота**: `position.y` — базовая отметка. `layer` из OSM × 5 м для мостов, `tunnel=yes` → `y = −5` (подземный переход/тоннель — нужен для будущих пешеходов).
9. **ID** — префикс типа OSM + id, чтобы не было коллизий:
   `bld_w<wayId>`, `bld_r<relationId>`, `light_n<nodeId>_<approachIdx>`, `road_w<wayId>`, `park_w…`, `water_r…`, `cross_n…`.
   Тот же `light_…` приходит в `sim.lights` — так фронт связывает статический объект с динамическим состоянием.
10. **Типы feature**: `building`, `road`, `traffic_light`, `crossing`, `park`, `water`, `bus_stop`, (позже) `footway`, `underpass`, `tree`.

### 5.4. Чанки (выдача по частям)

- **Сетка ячеек**: `cx = floor(x / cell_size)`, `cz = floor(z / cell_size)` в **мировых** координатах (после инверсии оси z — поправьте `GeoToCellIndex`/`CellIndexToBounds` в `LocalGrid`, сейчас они считают по `north`).
- **Размер ячейки.** В вашем примере `cell_size = 100`, в `temp/tmp.go` — 500. Рекомендация: `cell_size` в `project_info` — размер *чанка загрузки* = **250–500 м** (меньше — сотни HTTP-запросов на экран); если нужны ячейки 100 м для пространственного индекса симуляции — это другая, внутренняя сетка.
- **Привязка**: здания/парки/вода — к ячейке по центроиду; дороги — **ко всем ячейкам, которые пересекают** (без обрезки, чтобы не было швов; фронт дедуплицирует по `id`); светофоры/переходы — по позиции.
- **API**:
  - `GET /api/v1/map/info` → только `project_info`.
  - `GET /api/v1/map/chunks/{cx}/{cz}` → один чанк.
  - `GET /api/v1/map/chunks?cells=0:0,0:1,1:0` → пачка (до ~25 ячеек).
  - Заголовки: `ETag: "<map_version>-<cx>-<cz>"`, `Cache-Control: public, max-age=31536000, immutable` (URL содержит `?v=<map_version>`), `Content-Encoding: br/gzip`.
- **LOD** (позже): на дальних ячейках отдавать только здания (без `holes`), дороги упрощать (`orb/simplify` Douglas–Peucker).

---

## 6. map-service: ingest, хранилище, API

### 6.1. Разделить на две команды

```
services/map-service/cmd/
├── server/main.go   # gRPC-сервер: читает готовые данные (граф .bin + PostGIS)
└── ingest/main.go   # CLI: PBF → граф .bin + PostGIS (запускается вручную/в CI)
```

`ingest` выполняется один раз на карту, фиксирует `map_version` (хеш PBF + версия кода). Сервер при старте сверяет версию в БД и в `.bin`.

### 6.2. Конвейер ingest (многопроходный, без загрузки всех узлов)

Порядок объектов в PBF: **узлы → пути → отношения**. Чтобы не держать в памяти все узлы региона:

0. (Рекомендуется) Подготовить PBF: `osmium extract -b <minlon>,<minlat>,<maxlon>,<maxlat> region.pbf -o city.pbf`. Для симуляции хватает 5×5 … 10×10 км.
1. **Проход 1 — отношения** (`scanner.SkipNodes/SkipWays = true`; сверьте поля `Skip*`/`Filter*` с `paulmach/osm` v0.9.0): собрать `multipolygon`-отношения с `building`, `leisure=park`, `landuse`, `natural=water|wood`, `type=restriction` → `memberWayIDs`, роли `outer/inner`.
2. **Проход 2 — пути**: оставить дороги (`highway`), здания, парки, воду, а также пути из `memberWayIDs`; запомнить списки `nodeIDs` и множество нужных узлов `neededNodes`.
3. **Проход 3 — узлы**: сохранить координаты **только** `neededNodes` + узлы с тегами (`highway=traffic_signals|crossing|bus_stop`, `amenity=hospital|fire_station|police`, `entrance`).
4. **Сборка геометрии** (в памяти):
   - дорожный граф (как сейчас, с исправлениями §6.3);
   - полигоны зданий/парков/воды; `multipolygon`: сшивка сегментов в замкнутые кольца (стыковка по концам), назначение `outer`/`inner`, проверка замкнутости. Можно взять `paulmach/osm/osmgeojson` или `orb`‑утилиты; если сшивка слишком сложна — предварительно прогнать `osmium export -f geojsonseq` и читать его (Osmium надёжно собирает мультиполигоны);
   - светофоры/переходы/остановки с привязкой к узлу графа и направлению подъезда.
5. **Конвертация в локальные метры** (`LocalGrid`, origin = центр bbox) и расчёт `cell`-индексов.
6. **Сохранение**: граф → `.bin` (msgpack, как сейчас, но с типами узлов); объекты → PostGIS (`COPY`).

### 6.3. Правила построения дорожного графа (исправления)

- Проезжие `highway`: `motorway, trunk, primary, secondary, tertiary, unclassified, residential, living_street, service` + все `*_link`.
- Исключить: `access=no|private` (если нет `motor_vehicle=yes`), `service=parking_aisle|driveway` (опционально оставить как точки спавна).
- Односторонность: `oneway=yes|1|true` → вперёд; `oneway=-1|reverse` → **назад**; `junction=roundabout` → вперёд; у `motorway` по умолчанию односторонние.
- Полосы: `lanes`, `lanes:forward/backward`; для двусторонних `lanes` делится пополам.
- Скорость: `maxspeed` (учесть `mph`, `RU:urban`, `walk`, `none`); дефолты по классу дороги.
- Узлы с `highway=traffic_signals` → `NodeTrafficLight`, `highway=crossing` → `NodeCrossing` (проставлять **до** добавления рёбер).
- Узел — **перекрёсток**, если в нём сходятся ≥ 3 рёбер (в графе много «промежуточных» узлов по форме дороги — для симуляции они сворачиваются, см. §7.2).

### 6.4. Схема БД (миграции — `golang-migrate`/`atlas`, не AutoMigrate)

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE map_meta (
  map_version  text PRIMARY KEY,
  origin_lat   double precision NOT NULL,
  origin_lon   double precision NOT NULL,
  cell_size    integer NOT NULL,
  min_x double precision, min_z double precision, max_x double precision, max_z double precision,
  created_at   timestamptz DEFAULT now()
);

CREATE TABLE static_objects (
  osm_type   char(1)  NOT NULL,           -- 'n' | 'w' | 'r'
  osm_id     bigint   NOT NULL,
  map_version text    NOT NULL REFERENCES map_meta,
  type       text     NOT NULL,           -- building | road | traffic_light | crossing | park | water ...
  subtype    text,
  geom       geometry(Geometry, 4326) NOT NULL,   -- исходные lon/lat (для PostGIS-запросов)
  local      jsonb    NOT NULL,           -- готовый feature в локальных метрах (то, что отдаём фронту)
  cell_x     int NOT NULL,
  cell_z     int NOT NULL,
  props      jsonb,
  PRIMARY KEY (map_version, osm_type, osm_id)
);
CREATE INDEX ON static_objects USING gist (geom);
CREATE INDEX ON static_objects (map_version, cell_x, cell_z);
CREATE INDEX ON static_objects (type, subtype);
```

Идея: `local` содержит **готовый JSON-фрагмент feature** (в вашем формате), поэтому выдача чанка = `SELECT local FROM static_objects WHERE map_version=$1 AND cell_x=$2 AND cell_z=$3` + сборка массива. PostGIS-геометрия остаётся для вспомогательных запросов (ближайшая больница, точки спавна, поиск по адресу). Для дорог, пересекающих несколько ячеек, добавить таблицу связи `object_cells(object_id, cell_x, cell_z)`.

### 6.5. gRPC API map-service (итоговый)

| Метод | Назначение |
|-------|-----------|
| `GetMapInfo` | `project_info` (origin, cell_size, bounds, map_version) |
| `GetChunks(cells[])` | Статические объекты чанков в формате §5.2 |
| `GetRoute`, `GetNearestNode`, `GetJunctionInfo` | Как сейчас (+ фиксы §2.2) |
| `ExportGraph` (server-stream) | Выгрузка графа в `simulation-service`: узлы, рёбра с геометрией, светофоры, переходы, `map_version` |
| `GetSpawnPoints(area, kind)` | Допустимые точки появления машин/спецтранспорта; станции скорой/пожарных/полиции и больницы |
| ~~`UpdateEdgeWeight`~~ | Deprecated (см. §3.4) |

---

## 7. simulation-service: движок симуляции

### 7.1. Устройство цикла

```text
 ┌────────────────────── tick-loop (одна горутина — «писатель») ──────────────────────┐
 │ 1. drain command queue            (команды из Kafka → применить на границе тика)   │
 │ 2. clock.advance(wall_dt × speed) → N шагов фиксированного dt                      │
 │ 3. для каждого шага dt:                                                            │
 │      signals.update(dt)          (фазы светофоров, преэмпшн для спецтранспорта)    │
 │      spawner.update(dt)          (потоки/запланированные спавны)                   │
 │      vehicles.update(dt)         (IDM, перекрёстки, смена рёбер, прибытие)         │
 │ 4. раз в 100 мс wall-clock: собрать WorldSnapshot → publisher (асинхронно, Kafka)  │
 │ 5. по изменению: события и фазы светофоров → Kafka                                 │
 └────────────────────────────────────────────────────────────────────────────────────┘
```

**Время:**

- `dt` фиксирован (рекомендуется **0.05 с** = 20 Гц симуляции; не больше 0.1 с для устойчивости IDM).
- `speed_factor` ∈ {0 (пауза), 0.1 … 50}. За один кадр wall-clock выполняется `n = floor(accumulator / dt)` шагов, где `accumulator += wall_dt × speed_factor`.
- Ограничитель шагов за кадр (бюджет, например 50 мс процессорного времени): если не успеваем — снижаем реально достигнутую скорость и публикуем `effective_speed` (иначе «спираль смерти»).
- Детерминизм: один `rand.Rand` с `seed` на симуляцию, обход агентов в стабильном порядке — позволяет воспроизводить сценарии и тестировать.
- **Команды никогда не меняют состояние напрямую из других горутин** — только через канал; применяются в начале тика.

### 7.2. Модель мира внутри симуляции

После `ExportGraph` строится компактная структура (не `map[int64]*Node`, а массивы по индексам):

```go
type SimGraph struct {
    Nodes    []SimNode      // x, z, type, signalID
    Segments []Segment      // цепочка рёбер между перекрёстками: polyline, length, lanes, speed_limit, from, to
    Out      [][]SegmentID  // исходящие сегменты из узла (CSR-подобно)
    // индекс: сегмент → список машин, отсортированный по позиции s
}
```

- «Промежуточные» узлы формы дороги сворачиваются в полилинию сегмента — меньше объектов, быстрее поиск лидера.
- Позиция машины: `(segmentID, laneIdx, s)` (расстояние от начала сегмента), мировые `x,z,heading` считаются по полилинии при публикации.

### 7.3. Движение машин

**Фаза 2 (минимум):** одна полоса на направление, **IDM (Intelligent Driver Model)**:

`a = a_max · [1 − (v/v0)^4 − (s*/s)^2]`, `s* = s0 + v·T + v·Δv / (2·√(a_max·b))`

где лидер — ближайшая машина впереди на сегменте (или на следующем сегменте по маршруту в пределах горизонта ~100 м), либо **виртуальная стоящая «стена»** на стоп-линии красного светофора / перед занятым перекрёстком.

Параметры типов (старт, потом калибруются):

| Тип | длина, м | a_max, м/с² | b (комф. торможение), м/с² | T, с | s0, м | v0 множитель |
|-----|----------|-------------|---------------------------|------|-------|--------------|
| car | 4.5 | 1.5 | 2.0 | 1.4 | 2.0 | 1.0 |
| truck | 12 | 1.0 | 1.5 | 1.8 | 3.0 | 0.85 |
| bus | 12 | 1.1 | 1.6 | 1.6 | 3.0 | 0.9 |
| ambulance | 6 | 2.0 | 3.0 | 1.0 | 2.0 | 1.0 (с сиреной 1.3) |

Дальше (Фаза 6): многополосность и перестроения (MOBIL), повороты и ограничения (`type=restriction`), стоянки/остановки автобусов.

### 7.4. Перекрёстки и светофоры

- **Светофор** = контроллер перекрёстка (`SignalController`) с фазами; группы подходов (approach) формируются по направлению (противоположные подходы — одна фаза). Стартовая программа: `зелёный 25 с → жёлтый 3 с → all-red 2 с`, затем следующая фаза; фазовый сдвиг (`offset`) для «зелёной волны».
- Для каждой **головы** (`light_n<node>_<i>`) контроллер выдаёт состояние; изменения пишутся в `sim.lights` (+ `next_change_sim_time`).
- Машина у светофора: если красный/жёлтый и успеть остановиться с комфортным торможением → «стена» на стоп-линии; если жёлтый загорелся слишком близко — проезжает (правило дилеммной зоны).
- **Нерегулируемые перекрёстки (старт):** резервирование зоны перекрёстка — в зону входит одна машина из конфликтующих направлений (FIFO + приоритет по классу дороги). Позже — настоящие конфликтные точки и «уступи дорогу справа».
- **Пешеходные переходы:** узлы `NodeCrossing` — пока только замедление/вероятностная остановка; полноценная модель — Фаза 7.
- **Программы светофоров** можно менять командой `SetSignalProgram` (длительности фаз, режим «мигающий жёлтый»).

### 7.5. Спавн, маршруты, удаление

- **Спавн** (команда `SpawnVehicles{count, type, area, mode}`): выбор точек из `GetSpawnPoints`, случайный пункт назначения, маршрут — A* по `SimGraph` с живыми весами (`вес = длина / (v_средняя на сегменте)`; пересчитывать периодически, не на каждый тик). Пулы воркеров для расчёта маршрутов, чтобы спавн 1000 машин не вешал тик-луп (маршруты считаются асинхронно, машина появляется, когда маршрут готов).
- **Непрерывный поток** (позже): `Spawner` с интенсивностью (машин/час) по зонам и времени суток.
- **Прибытие**: в конце маршрута — события `VehicleRemoved{reason: arrived}` или новый случайный маршрут (режим «вечного трафика»).
- **Удаление** (`RemoveVehicles{ids|area|all}`): помечаем на удаление, физически убираем в начале следующего тика; отправляем `VehicleRemoved` в `sim.events`.
- **Защита от дедлоков** (важно в симуляциях): если группа машин стоит дольше N секунд в «замкнутом цикле» — телепорт одной из машин/удаление + событие `stuck`. Метрика `stuck_vehicles`.

### 7.6. Публикация состояния

- `WorldSnapshot` — раз в 100 мс **wall-clock** (не sim-time). Содержит `sim_time` и `tick`, чтобы клиент интерполировал по симуляционному времени (корректно при любом `speed_factor`).
- Публикация **асинхронная** (буфер продюсера); если Kafka тормозит — отбрасываем устаревший снимок, а не накапливаем.
- Позже: дельта-кодирование (слать только изменившееся), квантование (`int16` в см), разбиение по ячейкам.
- Оценка нагрузки: 5 000 машин × ~24 байта × 10 Гц ≈ 1.2 МБ/с на gateway до фильтрации по видимым ячейкам. Это приблизительная оценка — измерить в Фазе 2.

### 7.7. Типы команд и их эффект

| Команда | Эффект |
|---------|--------|
| `SetRunState{running/paused}` | Пауза/старт часов. В паузе тик-луп продолжает слушать команды и публиковать статус |
| `SetSpeed{factor}` | Меняет `speed_factor` (0.1–50, ограничивается `max_speed_factor` по бюджету CPU) |
| `SpawnVehicles` | См. §7.5 |
| `RemoveVehicles` | См. §7.5 |
| `CloseRoad{edge_ids,closed}` | Вес ребра = ∞, машины на нём доезжают/перестраивают маршрут |
| `CreateIncident{x,z,kind}` | Авария/блокировка полосы; запускает диспетчеризацию (§9) |
| `SetSignalProgram` | Меняет программу контроллера |
| `Reset{seed}` | Очистка мира; новая симуляция с seed |

---

## 8. api-gateway: REST и WebSocket

### 8.1. REST

| Метод и путь | Описание |
|--------------|----------|
| `GET /api/v1/map/info` | `project_info` |
| `GET /api/v1/map/chunks/{cx}/{cz}` и `?cells=` | Чанки (§5.4) |
| `POST /api/v1/map/route` | Route preview (как сейчас) |
| `GET /api/v1/sim/state` | Текущий `SimStatus` (из кэша gateway) |
| `POST /api/v1/sim/state` `{run: true|false}` | Старт/пауза → `202 {command_id}` |
| `POST /api/v1/sim/speed` `{factor}` | Скорость → `202` |
| `POST /api/v1/sim/vehicles` `{count,type,area,...}` | Спавн → `202` |
| `DELETE /api/v1/sim/vehicles` `?ids=…|area=…|all=true` | Удаление → `202` |
| `GET /api/v1/sim/vehicles/{id}` | Детали машины (gRPC в simulation) |
| `POST /api/v1/sim/incidents` | Создать инцидент |
| `POST /api/v1/sim/roads/{id}/closure` | Закрыть/открыть дорогу |
| `PUT /api/v1/sim/traffic-lights/{id}/program` | Сменить программу |

Все изменяющие эндпоинты **асинхронные**: `202 Accepted` + `command_id`; итог приходит по WebSocket (`ack`) — это естественно ложится на Kafka-команды.

### 8.2. WebSocket `/ws`

Клиент → сервер (JSON, редкие сообщения):

```json
{ "op": "subscribe", "cells": [[0,0],[0,1],[1,0]] }
{ "op": "unsubscribe", "cells": [[0,0]] }
{ "op": "follow", "vehicle_id": 4217 }
```

Сервер → клиент (бинарный protobuf `ServerFrame`, поле `oneof`):

- `Snapshot{sim_time, tick, vehicles[]}` — только машины в подписанных ячейках, ~10 раз/с;
- `LightStates[]` — при изменении (+ полный набор при подключении из compacted-кэша);
- `Status` — при изменении часов/скорости;
- `Events[]` — spawn/despawn/siren/incident;
- `Ack{command_id, ok, error}`.

Реализация gateway:

- `transport/kafka` (вместо пустого `transport/nats`): consumer'ы → внутренний `Hub`.
- `Hub` хранит: последний `WorldSnapshot`, `map[lightID]LightState`, последний `SimStatus`; индекс машин по ячейкам.
- Для каждого клиента — своя горутина записи с **буфером на 1–2 кадра и политикой «выбросить старое»** (медленный клиент не должен тормозить остальных).
- Новому клиенту сразу отправляется «слепок»: статус + все светофоры + текущий снимок.
- Библиотека: `github.com/gofiber/contrib/websocket` (вы уже на Fiber).

### 8.3. Рекомендации фронту (контракт)

- Грузить чанки по ячейкам вокруг камеры (радиус 1–2 км), кэшировать по `map_version`.
- Статические светофоры связывать с `LightStates` по `id`.
- Держать буфер из 2 снимков и рендерить с задержкой ≈ 150 мс, интерполируя позицию/угол по `sim_time`; машина, не пришедшая в 1.0 с, удаляется (TTL), либо удаляется по `VehicleRemoved`.
- При смене подписки (камера двигается) — отправлять `subscribe/unsubscribe` не чаще 2 раз/с.

---

## 9. Спецтранспорт

Типы: `AMBULANCE`, `FIRE_TRUCK`, `POLICE` с флагом `SIREN` (сирена/мигалки включены или нет). Скорая **без** сирены — обычная машина с другими размерами.

**Поведение при включённой сирене:**

1. **Скорость**: `v0 × 1.3`, допускается превышение лимита сегмента.
2. **Проезд на красный**: подъехать к стоп-линии, снизиться до ~10 км/ч, если зона перекрёстка свободна/уступают — проезжать.
3. **Преэмпшн светофоров (EVP)**: когда спецмашина в радиусе ~150 м от светофора и едет к нему, контроллер перекрёстка досрочно завершает текущую фазу (через жёлтый) и включает зелёный для её направления; после проезда — возврат к программе.
4. **Уступание дороги другими**: машины в радиусе ~60 м впереди спецмашины на том же сегменте снижают скорость и смещаются к краю (на однополосной дороге — боковое смещение `lateral_offset`; в многополосной модели — перестроение вправо). Спецмашине разрешён обгон по встречной полосе, если она свободна.
5. **События**: `SirenChanged`, `EmergencyDispatched`, `EmergencyArrived` в `sim.events`.

**Диспетчеризация (сценарий «вызов»):**

`CreateIncident(x,z,kind)` → ближайшая станция нужного типа (из `GetSpawnPoints`) → спавн/назначение машины с сиреной → маршрут `станция → инцидент` → пребывание на месте (X сек) → `инцидент → больница` (для скорой) → возврат на станцию, сирена выключается.

---

## 10. Пешеходы и подземные переходы — как заложить сейчас

Чтобы потом не переписывать ingest и движок:

1. **Ingest сразу парсит пешеходные данные** и кладёт их в `static_objects` (тип `footway`, `crossing`, `underpass`, `steps`): `highway=footway|path|pedestrian|steps|crossing`, `footway=sidewalk|crossing`, `tunnel=yes`, `bridge=yes`, `layer`.
2. **Отдельный пешеходный граф** (`PedGraph`) строится рядом с дорожным; узлы `crossing` — места «склейки» двух графов. Подземный переход — рёбра с `layer=-1` (`y = −5` в геометрии), соединяющие два входа (`highway=steps` / `entrance`).
3. **В движке — интерфейс агента**, а не «машина» как особый случай:

```go
type Agent interface {
    ID() uint32
    Kind() AgentKind          // Vehicle | Pedestrian
    Update(dt float64, w *World)
    Pose() (x, z, heading float32)
}
```

   Снимок содержит `vehicles[]` и (позже) `pedestrians[]` отдельным полем — пешеходов может быть на порядок больше, формат компактнее (позиция + heading + скорость).
4. **Светофоры** изначально имеют `pedestrian_phase` (зелёный для пешеходов, когда у машин красный); сейчас не используется, но структура фаз его предусматривает.
5. **Пешеходная модель (Фаза 7)**: движение по рёбрам PedGraph со скоростью ~1.4 м/с, остановка на красный, правила на зебре (машины уступают, если пешеход на переходе или у его края), опционально social-force для плотных потоков.
6. **Масштабирование**: если пешеходов станет много, выносится в отдельный процесс с Kafka-синхронизацией на переходах (топик `sim.crossings`), но начинать следует в одном процессе.

---

## 11. Миграция с NATS на Kafka — чеклист

| Что | Где | Действие |
|-----|-----|----------|
| Брокер | `docker-compose.infra.yaml`, `docker-compose.yaml` | Удалить `nats`; добавить `kafka`, `kafka-init`, `kafka-ui` (§4.5) |
| Переменные | `.env`, `.env.example` | Убрать `NATS_PORT`, `NATS_MONITOR_PORT`; добавить `KAFKA_PORT=9092`, `KAFKA_BROKERS=kafka:19092` |
| Библиотека | `go.mod` | Убрать `github.com/nats-io/nats.go` (+ `nkeys`, `nuid`); добавить `github.com/twmb/franz-go`, `github.com/twmb/franz-go/plugin/kotel` |
| Пакет | `pkg/natslog` | Заменить на `pkg/kafkax` (клиент, producer/consumer helper, трейсинг) |
| Скрипты | `deployments/scripts/nats/monitor.sh`, `Makefile: monitor-nats` | Удалить; `make monitor-kafka` (Kafka UI / `kcat`) |
| Демо | `temp/pub`, `temp/sub`, `services/hi-service` | Удалить (и `api/proto/test`, `HelloClient` в gateway) |
| Gateway | `internal/transport/nats` (пусто) | Создать `internal/transport/kafka` + `internal/transport/websocket` |
| Тема | `city.updates`, `greet.hello` | Больше не используются; единая схема имён — `sim.*` (§4.1) |

---

## 12. Наблюдаемость и тестирование

**Метрики (OTel → Prometheus):**

| Метрика | Сервис | Зачем |
|---------|--------|-------|
| `sim_tick_duration_seconds` (histogram) | simulation | Основной показатель здоровья; алерт, если p99 > `dt` |
| `sim_effective_speed`, `sim_requested_speed` | simulation | Видно, когда «не успеваем» |
| `sim_vehicles{type}` | simulation | Количество агентов |
| `sim_stuck_vehicles` | simulation | Дедлоки |
| `kafka_produce_latency`, `kafka_consumer_lag{topic,group}` | все | Задержки потока (franz-go + kotel) |
| `snapshot_size_bytes` | simulation | Рост трафика |
| `ws_clients`, `ws_dropped_frames` | gateway | Медленные клиенты |
| `chunk_request_duration`, `chunk_cache_hit_ratio` | map/gateway | Скорость выдачи карты |

Trace-контекст проходит через Kafka-заголовки: REST-запрос → команда → применение в тике → ответ (видно в Tempo).

**Тесты:**

- **Юнит**: IDM (стационарные сценарии: разгон, торможение перед стеной, следование за лидером), конвертер координат (`LocalGrid`: round-trip WGS84↔локальные ↔ ячейки), сшивка мультиполигонов, обход контуров и инверсия оси z.
- **Property/сценарные**: машины не проезжают сквозь друг друга (минимальный зазор > 0); нет NaN; закон сохранения числа агентов (`spawned − removed = alive`).
- **Детерминизм**: одинаковый `seed` + одинаковые команды → идентичный хэш состояния через N тиков.
- **Интеграционные**: `testcontainers` (Kafka/Redpanda + PostGIS): команда → результат → снимок; ingest `test.pbf` → чанки.
- **Нагрузочные**: 1k / 5k / 20k машин, замер `sim_tick_duration`, размер снимка, число WS-клиентов.
- Контрактные тесты формата геометрии (JSON Schema для `features`), прогон на `temp/down syndrome.json` как golden-файле (рекомендуется переименовать в `golden/city_sample.json`).

---

## 13. Структура репозитория

```text
synthcity/
├── api/
│   ├── proto/{common,map,sim,vehicle}/v1      # traffic/test удалить; sim — новый
│   └── gen/…                                   # генерация (buf generate)
├── services/
│   ├── api-gateway/
│   │   └── internal/{app,config,service,transport/{grpc,kafka,websocket,rest},hub,middleware}
│   ├── map-service/
│   │   ├── cmd/{server,ingest}
│   │   └── internal/{app,config,domain,ingest/{pass1,pass2,pass3,geometry},infrastructure/{osm,postgres},service/{route,chunk},transport/grpc}
│   └── simulation-service/                     # НОВЫЙ
│       ├── cmd/main.go
│       └── internal/
│           ├── app, config
│           ├── engine/        # tick-loop, clock, command queue
│           ├── world/         # SimGraph, сегменты, индекс машин
│           ├── agents/vehicle # IDM, типы, маршрут
│           ├── signals/       # контроллеры, фазы, preemption
│           ├── spawner/
│           ├── dispatch/      # инциденты, спецтранспорт
│           ├── publisher/     # снимки/события → Kafka
│           ├── commands/      # consumer + обработчики
│           └── transport/grpc # GetVehicle, ListVehicles
├── pkg/
│   ├── logger, telemetry                      # (исправить MetricsInterceptor)
│   ├── kafkax/                                # НОВЫЙ (вместо natslog)
│   ├── geoconverter/                          # перенести из map-service (общий)
│   ├── roadgraph/                             # перенести RoadGraph/Node/Edge из map-service/internal
│   └── routing/                               # A* для map и simulation
├── deployments/{docker-compose,postgres/migrations,configs,scripts}
└── golden/city_sample.json
```

> `internal/` в Go нельзя импортировать из другого сервиса — поэтому общие вещи (`geoconverter`, граф, A*) переезжают в `pkg/`.

---

## 14. Дорожная карта по фазам

Оценки ориентировочные, для одного разработчика; важен порядок и критерий готовности.

### Фаза 0. Фундамент и уборка (≈ 1 неделя)

- [ ] Исправить пункты §2.2 (опечатка `higway`, `Node.Type`, порты/env, импорты в `map.proto`, `MetricsInterceptor`).
- [ ] Удалить NATS/hi-service/temp pub-sub (§11); поднять Kafka + UI + создание топиков.
- [ ] Создать `pkg/kafkax`; `sim/v1` proto; `buf generate` проходит.
- [ ] Вынести `geoconverter`, граф и A* в `pkg/`.
- [ ] Миграции БД; в compose добавлены `map-service`.

**Готово, когда:** `docker compose up` поднимает всю инфраструктуру; тестовый продюсер/консьюмер на `sim.events` виден в Kafka UI и в трейсах Tempo.

### Фаза 1. Статический мир → фронт (≈ 2 недели)

- [ ] CLI `ingest`: 3 прохода, здания (Polygon + multipolygon), парки, вода, светофоры, переходы, дороги.
- [ ] Конвертация в локальные метры, расчёт высот, ячеек, ID (§5).
- [ ] Таблицы PostGIS, `COPY`-загрузка.
- [ ] gRPC/REST: `GetMapInfo`, `GetChunks`; ETag/кэш/сжатие.
- [ ] Фронт-прототип: загрузка чанков вокруг камеры и отрисовка зданий и дорог.

**Готово, когда:** на фронте виден город из `test.pbf`; здания стоят на своих местах относительно дорог; JSON проходит схему и совпадает по структуре с эталоном; чанк отдаётся < 50 мс (из кэша < 5 мс).

### Фаза 2. Ядро симуляции + стриминг (≈ 2–3 недели)

- [ ] `simulation-service`: `ExportGraph`, `SimGraph`, часы (play/pause/speed), tick-loop.
- [ ] Машины: спавн по команде, маршрут A*, IDM на одной полосе, прибытие/удаление.
- [ ] Публикация `WorldSnapshot` → Kafka; gateway `Hub` → WS; фронт интерполирует.
- [ ] Команды `SetRunState`, `SetSpeed`, `SpawnVehicles`, `RemoveVehicles` через Kafka + `CommandResult`.

**Готово, когда:** 1 000 машин едут по городу плавно; пауза и ускорение ×1/×5/×10 работают без рывков; метрики `sim_tick_duration` и `effective_speed` видны в Grafana.

### Фаза 3. Светофоры и перекрёстки (≈ 2 недели)

- [ ] `SignalController`, фазы, события в `sim.lights`; фронт подсвечивает статические светофоры по `id`.
- [ ] Остановка на красный, правило жёлтого, резервирование нерегулируемых перекрёстков.
- [ ] Детектор дедлоков + метрика `stuck_vehicles`.

**Готово, когда:** при 2 000 машин нет проездов на красный и нет взаимных блокировок > 60 с в 30-минутном прогоне; новый клиент сразу видит правильные цвета светофоров.

### Фаза 4. Управление и сценарии (≈ 1–2 недели)

- [ ] UI/REST: добавление/удаление машин по области, закрытие дорог, смена программы светофоров.
- [ ] Статус `effective_speed`, ограничение `max_speed_factor`; `Reset{seed}`.
- [ ] Запись команд (audit) и воспроизводимые сценарии (seed + список команд).

### Фаза 5. Спецтранспорт (≈ 2 недели)

- [ ] Типы машин, флаг сирены, повышенная скорость.
- [ ] Преэмпшн светофоров; уступание дороги.
- [ ] Инциденты и диспетчеризация станция → место → больница.

**Готово, когда:** скорая с сиреной проезжает город заметно быстрее обычной машины, не создавая заторов/аварий; события видны во фронте.

### Фаза 6. Реализм дорожного движения (≈ 3 недели)

- [ ] Многополосность, MOBIL-перестроения, повороты и `restriction`.
- [ ] Остановки автобусов, парковка, разные классы водителей.
- [ ] Калибровка параметров IDM; динамические веса маршрутов (пробки).

### Фаза 7. Пешеходы и подземные переходы (≈ 4+ недели)

- [ ] `PedGraph`, пешеходные агенты, фазы светофоров для пешеходов.
- [ ] Зебры: взаимодействие с машинами; подземные/надземные переходы (`layer`).
- [ ] Формат снимка для пешеходов; фильтрация по ячейкам.

### Фаза 8. Масштабирование и эксплуатация (по мере необходимости)

- [ ] Шардирование симуляции по районам (разные партиции Kafka, обмен на границах).
- [ ] `recorder` + реплей (ClickHouse/файлы), перемотка.
- [ ] Дельта-снимки, квантование, ячеечные партиции снимков.
- [ ] Авторизация, многопользовательские сессии (`sim_id` на пользователя/комнату).
- [ ] CI: линт + buf breaking + интеграционные тесты; образы и деплой.

---

## 15. Риски и открытые вопросы

| Риск | Митигация |
|------|-----------|
| Рост числа агентов упрётся в один процессор | Измерять с Фазы 2; профилирование (`pprof`), параллельный `Update` по независимым сегментам, затем шардирование (Фаза 8) |
| Дедлоки на перекрёстках | Метрика `stuck`, таймаут + телепорт/удаление, упрощённая модель резервирования на старте |
| Некорректные OSM-данные (разрывы, нет `oneway`, незамкнутые контуры) | Валидация и отчёт при ingest; пропуск «битых» объектов с логом |
| Задержка Kafka на горячем пути снимков | `linger.ms=5`, `acks=1`, мониторинг lag; запасной вариант — прямой gRPC-стрим simulation → gateway для снимков (Kafka оставить для команд/событий) |
| Расхождение систем координат (машины мимо дорог) | Единая функция `GeoToWorld`, golden-тест: узел графа и точка дороги на фронте совпадают с точностью до сантиметров |
| Размер ответов карты | Чанки + сжатие + LOD + кэш по `map_version` |

**Вопросы, ответы на которые уточнят план:**

1. Целевой размер города и число машин (1k, 10k, 100k)? От этого зависят шардирование и формат снимка.
2. Какой фронт (Three.js, Unity, Cesium, Deck.gl)? Влияет на выбор системы осей (§5.1) и на то, нужны ли готовые треугольники.
3. Одна общая симуляция или отдельная на каждого пользователя (`sim_id`)? Это определяет схему групп/партиций Kafka.
4. Нужна ли детерминированная повторяемость и реплей (научные эксперименты) — или достаточно «живой» картинки?
5. Какой реальный город используется — Оренбург (по данным в проекте) или Гётеборг (по примеру JSON)? `origin_gps` должен соответствовать PBF.
