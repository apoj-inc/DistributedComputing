# REST API протокол

## 1. Общие положения
- Базовый URL: `/api/v1`
- Формат: `application/json; charset=utf-8`
- Время: ISO 8601 в UTC (пример: `2025-01-04T08:57:00Z`)
- Все ответы ошибок используют единый формат `error`
- Версионирование через префикс пути (`/api/v1`)

Обязательные заголовки:
- `Content-Type: application/json` для запросов с телом
- `Accept: application/json`

## 2. Диаграммы взаимодействия

Регистрация и heartbeat:
```
Worker -> Master: PUT /agents/{agent_id}
Master -> Worker: 200 {heartbeat_interval_sec}
Worker -> Master: POST /agents/{agent_id}/heartbeat
```

Получение и выполнение задач:
```
Worker -> Master: POST /agents/{agent_id}/tasks:poll
Master -> Worker: 200 {tasks[]}
Worker -> OS: запуск процесса
Worker -> Master: POST /tasks/{task_id}/status
```

Управление из CLI:
```
CLI -> Master: POST /tasks
CLI -> Master: GET /tasks/{task_id}
CLI -> Master: GET /tasks/{task_id}/logs?stream=stdout
```

## 3. Модели данных

### 3.1 Agent
- `agent_id` (string, required)
- `os` (string, required; ожидаемые значения: `linux`, `windows`, но сервер не валидирует)
- `version` (string, required)
- `resources` (object, required)
  - `cpu_cores` (int)
  - `ram_mb` (int)
  - `slots` (int)
- `status` (string; `idle`, `busy`, `offline`)
- `last_heartbeat` (string, ISO 8601)

### 3.2 Task
- `task_id` (int64, required)
- `state` (string; `queued`, `running`, `succeeded`, `failed`, `canceled`)
- `command` (string, required)
- `args` (array[string])
- `env` (object string->string)
- `timeout_sec` (int)
- `constraints` (object)
  - `os` (string, optional; сопоставляется с `agent.os` без валидации)
  - `cpu_cores` (int, optional)
  - `ram_mb` (int, optional)
  - `labels` (array[string], optional)
- `assigned_agent` (string, optional)
- `created_at` (string, ISO 8601)
- `started_at` (string, ISO 8601, optional)
- `finished_at` (string, ISO 8601, optional)
- `exit_code` (int, optional)
- `error_message` (string, optional)

## 4. API агента (Worker -> Master)

### 4.1 Регистрация агента
`PUT /api/v1/agents/{agent_id}`

Тело запроса:
```
{
  "os": "linux",
  "version": "1.0.0",
  "resources": { "cpu_cores": 4, "ram_mb": 8192, "slots": 2 }
}
```

Ответ `200`:
```
{
  "status": "ok",
  "heartbeat_interval_sec": 10
}
```

### 4.2 Heartbeat
`POST /api/v1/agents/{agent_id}/heartbeat`

Тело запроса:
```
{
  "status": "idle"
}
```
Дополнительные поля допустимы и игнорируются сервером. Разрешённые значения `status`:
`idle`, `busy`, `offline`.

Ответ `200`:
```
{ "status": "ok" }
```

### 4.3 Получение задач
`POST /api/v1/agents/{agent_id}/tasks:poll`

Тело запроса:
```
{ "free_slots": 2 }
```

Ответ `200`:
```
{
  "tasks": [
    {
      "task_id": 123,
      "command": "solver.exe",
      "args": ["--size", "1000"],
      "env": { "OMP_NUM_THREADS": "4" },
      "timeout_sec": 3600,
      "constraints": { "os": "windows" }
    }
  ]
}
```

### 4.4 Отчет о выполнении задачи
`POST /api/v1/tasks/{task_id}/status`

Тело запроса:
```
{
  "state": "succeeded",
  "exit_code": 0,
  "started_at": "2025-01-04T09:00:00Z",
  "finished_at": "2025-01-04T09:10:00Z",
  "error_message": ""
}
```
Допустимые значения `state`: `queued`, `running`, `succeeded`, `failed`, `canceled`.

Ответ `200`:
```
{ "status": "ok" }
```

## 5. API задач (CLI -> Master)

### 5.1 Создание задачи
`POST /api/v1/tasks`

Тело запроса:
```
{
  "command": "solver",
  "args": ["--size", "1000"],
  "env": { "OMP_NUM_THREADS": "4" },
  "timeout_sec": 3600,
  "constraints": { "os": "linux", "ram_mb": 2048 }
}
```

Ответ `201`:
```
{ "task_id": 123 }
```

### 5.2 Список задач
`GET /api/v1/tasks?state=running&agent_id=agent-1&limit=50&offset=0`

Ответ `200`:
```
{ "tasks": [ { "task_id": 123, "state": "running" } ] }
```

### 5.3 Детали задачи
`GET /api/v1/tasks/{task_id}`

Ответ `200`:
```
{ "task": { "task_id": 123, "state": "running" } }
```

### 5.4 Отмена задачи
`POST /api/v1/tasks/{task_id}/cancel`

Ответ `200`:
```
{ "status": "ok" }
```

## 6. API агентов (CLI -> Master)

### 6.1 Список агентов
`GET /api/v1/agents?status=idle&limit=50&offset=0`

Ответ `200`:
```
{ "agents": [ { "agent_id": "agent-1", "status": "idle" } ] }
```

### 6.2 Детали агента
`GET /api/v1/agents/{agent_id}`

Ответ `200`:
```
{ "agent": { "agent_id": "agent-1", "status": "busy" } }
```

## 7. Логи задач

### 7.1 Получение логов
`GET /api/v1/tasks/{task_id}/logs?stream=stdout`

Ответ `200` (Content-Type: text/plain):
```
<текст логов>
```
Параметр `stream` опционален (по умолчанию `stdout`).

### 7.2 Потоковый вывод логов
`GET /api/v1/tasks/{task_id}/logs:tail?stream=stderr&from=0`

Ответ `200` (chunked):
- Порции текста с постепенным обновлением.
- Параметр `stream` опционален (по умолчанию `stdout`).
- Параметр `from` используется как смещение в байтах (по умолчанию `0`).
- Заголовок ответа `X-Log-Size` содержит текущий размер лога в байтах.

### 7.3 Загрузка логов агентом
`POST /api/v1/tasks/{task_id}/logs:upload`

Тело запроса:
```
{ "stream": "stdout", "data": "<текст лога>" }
```

Ответ `200`:
```
{ "status": "ok", "size_bytes": 123 }
```

Ограничения:
- `stream` — `stdout` (по умолчанию) или `stderr`.
- Размер `data` ограничен `MAX_LOG_UPLOAD_BYTES` на мастере (по умолчанию 10 MB); при превышении вернётся `413 PAYLOAD_TOO_LARGE`.

## 8. Ошибки

Формат ошибки (любой 4xx/5xx):
```
{
  "error": {
    "code": "TASK_NOT_FOUND",
    "message": "Task does not exist",
    "details": { "task_id": 123 }
  }
}
```

Рекомендуемые HTTP коды:
- `400` неверный запрос
- `404` объект не найден
- `409` конфликт состояния
- `422` несовместимые требования
- `500` внутренняя ошибка
