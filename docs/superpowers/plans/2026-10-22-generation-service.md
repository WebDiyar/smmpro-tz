# Сервис генерации: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Сервис принимает заказ на генерацию, списывает токены, ведёт задачу по цепочке ног провайдеров (`alpha`/`beta`) с переходом при отказах и тайм-аутах и возвращает токены, если отказали все ноги. Каждое движение денег (начисление, списание, возврат) происходит ровно один раз при 4 процессах, повторных и одновременных вебхуках и падениях процесса.

**Architecture:** FastAPI + asyncpg поверх PostgreSQL. Всё состояние хранится в БД. «Ровно один раз» держится на уникальных ограничениях журнала и на условных переходах статусов под блокировкой строки задачи. Баланс меняет только триггер на вставке в журнал. В каждом из 4 процессов работает сторож, который забирает просроченные попытки через `FOR UPDATE SKIP LOCKED`. Фейковые провайдеры написаны одним FastAPI-приложением и запускаются двумя контейнерами, а режимы для тестов переключаются через admin-эндпоинт.

**Tech Stack:** Python 3.12, FastAPI, uvicorn (`--workers 4`), asyncpg, httpx, PostgreSQL 16, pytest + pytest-asyncio, docker compose.

**Spec:** `/Users/diyar/Desktop/smmpro/ТЗ.txt`

## Global Constraints

- Python 3.11+, FastAPI или другой асинхронный фреймворк, PostgreSQL. ORM любой или сырой SQL (выбран сырой SQL через asyncpg).
- Сервис работает минимум в 4 процессах (`uvicorn --workers 4`). Состояние в памяти процесса не годится.
- Если сторож запущен в каждом процессе, работа не должна задваиваться.
- Баланс пользователя всегда равен сумме его журнала. Операций без строки в журнале нет.
- Каталог: `image-fast` стоит 2 токена, ноги `alpha` (до 4 референсов) → `beta` (до 10). `video-long` стоит 25, ноги `beta` (до 2) → `alpha` (до 1, `skip_on_moderation`).
- Провайдер: `POST /tasks` сразу отвечает `202 {"external_id": "..."}`, затем через 2–20 с присылает вебхук `completed` со ссылкой на файл или `failed` с типом `technical` | `moderation` | `payload`.
- `X-Signature` = HMAC-SHA256 от сырого тела с общим секретом. Та же подпись стоит на `/webhooks/payments`.
- `PROVIDER_TIMEOUT` настраивается, для тестов 60 секунд.
- Схема создаётся миграциями. В README показано, как их применять и как добавить следующую.
- `docker compose up` поднимает базу, сервис в 4 воркера и обоих фейковых провайдеров.
- Сообщения коммитов объясняют, почему сделано именно так.
- Не нужны: регистрация и авторизация, UI, файловое хранилище (результат хранится строкой-ссылкой), настоящие деньги.

## Review Focus

1. Вебхук провайдера приходит раньше, чем диспетчер сохранил `external_id`, потому что задержка в тестах может быть 0. Результат не должен потеряться. Задача 5: `test_webhook_before_dispatch_recorded`.
2. Процесс упал между коммитом попытки и HTTP-отправкой, и попытка осталась в `dispatching`. Задача не должна зависнуть: её подберёт сторож. Задача 6: `test_orphaned_dispatching_attempt_recovered`.
3. Повтор оплаты с тем же `payment_id`, но другими `tokens`/`user_id`. Выигрывает первая доставка, второго начисления нет. Задача 2: `test_same_payment_id_different_body_credits_once`.
4. Вебхук с чужим `reference` или неизвестным провайдером, либо `reference` принадлежит другому провайдеру. Ответ 404, состояние не меняется. Задача 5: `test_webhook_wrong_provider_or_reference_404`.
5. `completed` от устаревшей попытки, пока задача ещё идёт на следующей ноге. Задача завершается с этим результатом, и возврата нет никогда. Задача 6: `test_superseded_attempt_completed_wins_while_running`.

---

## Проектные решения (анализ ТЗ)

### Д1. Деньги «ровно один раз»

**Журнал — единственный способ изменить баланс.** Таблица `ledger(user_id, amount, reason, payment_id, task_id)` устроена так:
- `reason IN ('payment','charge','refund')`;
- `CHECK` на знак: `payment > 0`, `charge < 0`, `refund > 0`;
- `CHECK ((reason='payment') = (payment_id IS NOT NULL))` и `CHECK ((reason<>'payment') = (task_id IS NOT NULL))`;
- `UNIQUE (payment_id)`: одна оплата даёт одно начисление. NULL-значения различны, поэтому строки списаний и возвратов не конфликтуют;
- `UNIQUE (task_id, reason)`: у задачи максимум одно списание и один возврат.

Триггер `AFTER INSERT ON ledger` выполняет `UPDATE users SET balance = balance + NEW.amount WHERE id = NEW.user_id`. Триггер `BEFORE UPDATE OR DELETE ON ledger` бросает исключение, то есть журнал только дописывается. `users.balance CHECK (balance >= 0)` служит последней защитой от ухода в минус. Почему триггер, а не код приложения: правило 8 («операций без строки в журнале нет») становится гарантией БД, а не договорённостью, которую каждый новый путь кода должен помнить. В приложении нет ни одного `UPDATE users SET balance`.

**Начисление** (`/webhooks/payments`, status=`succeeded`), одна транзакция:
```sql
INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING;
INSERT INTO ledger (user_id, amount, reason, payment_id) VALUES ($1, $2, 'payment', $3)
  ON CONFLICT (payment_id) DO NOTHING RETURNING id;     -- NULL => дубль, 200 {"credited": false}
```
20 одновременных доставок: вторая и следующие вставки ждут на уникальном индексе, пока первая закоммитится, после чего срабатывает `DO NOTHING`. Баланс при этом не трогается, потому что триггер не срабатывает.

**Списание** (`/generate`): вставка `charge` в той же транзакции, что и создание задачи, после проверок под блокировкой строки пользователя (Д2).

**Возврат**: выполняется только внутри перехода `running → failed`, под блокировкой строки задачи:
```sql
UPDATE tasks SET status='failed', finished_at=now() WHERE id=$1 AND status='running' RETURNING id;
-- только если строка вернулась:
INSERT INTO ledger (user_id, amount, reason, task_id)
  SELECT user_id, -amount, 'refund', task_id FROM ledger WHERE task_id=$1 AND reason='charge'
  ON CONFLICT (task_id, reason) DO NOTHING;
```
Сумма возврата берётся из строки списания, поэтому смена цены в каталоге не ломает возврат. Защиты две: условный переход статуса (сработает один раз) и уникальный индекс (даже ошибка в коде не даст второй возврат).

**Падение процесса посреди обработки вебхука.** Обработка любого вебхука укладывается в одну транзакцию: смена статусов, строка журнала и создание следующей попытки. При падении до `COMMIT` PostgreSQL откатывает всё. Повторная доставка обрабатывается с нуля, а если её не будет, задачу дотянет сторож по дедлайну попытки. При падении после `COMMIT`, но до ответа 200 провайдер повторит доставку и получит no-op через условные переходы и `ON CONFLICT`. При падении после `COMMIT`, но до HTTP-отправки следующей ноги попытка остаётся в `dispatching` со своим `deadline_at`. Через `PROVIDER_TIMEOUT` сторож засчитает её как неудачу и поведёт задачу дальше. Отсюда инвариант: у каждой `running`-задачи есть текущая незавершённая попытка с дедлайном в БД, поэтому задача не может застрять навсегда.

**Проверка инварианта** в тестах: autouse-фикстура после каждого e2e-теста выполняет
`SELECT count(*) FROM users u WHERE u.balance <> (SELECT coalesce(sum(amount),0) FROM ledger l WHERE l.user_id=u.id)` и ожидает 0.

### Д2. Конкурентность `/generate` (429/402 без гонки)

Строка пользователя блокируется так: `SELECT balance FROM users WHERE id=$1 FOR NO KEY UPDATE`. Под этой блокировкой выполняются подсчёт активных задач, проверка баланса, `INSERT tasks`, `INSERT ledger(charge)` и `INSERT attempts`, затем `COMMIT`. Все `/generate` одного пользователя (на любом из 4 процессов) так выстраиваются в очередь, и второй запрос видит закоммиченные задачу и списание первого.
- Почему не SERIALIZABLE: пришлось бы писать цикл повторов на serialization failure, а ошибки было бы сложнее объяснить.
- Почему не advisory lock: делает то же самое, но невидим в схеме. Строку пользователя всё равно трогает списание.
- Почему `NO KEY UPDATE`, а не `UPDATE`: этот режим не конфликтует с `KEY SHARE`, который ставят FK-проверки чужих вставок в `ledger`/`tasks`. Тогда ожидание идёт только в одну сторону. Порядок блокировок везде одинаковый: задача → пользователь (возврат) или только пользователь (`/generate`, оплата), то есть циклов нет.
- Порядок проверок: 422 (форма) → 404 (модель) → 400 (нет подходящей ноги) → 404 (пользователь) → 429 → 402. Ответ 400 выдаётся до любой блокировки и списания. Ответ 429 идёт раньше 402: пользователю с тремя активными задачами пополнение не поможет.
- Активной считается задача в статусе `running`. `MAX_ACTIVE_TASKS = 3` задан константой, а не настройкой.

### Д3. Машина состояний

**Задача:** `running → completed | failed`. Оба конечных состояния терминальные. `tasks.current_attempt_id` указывает на текущую ногу.

**Попытка:** `dispatching` (создана, HTTP ещё не отправлен) → `pending` (провайдер ответил 202) → `completed` | `failed` | `timed_out`. Дополнительно:
- `abandoned`: задачу завершила другая попытка, а эта ещё висела;
- `late_completed`: `completed` пришёл, когда задача уже терминальна (правило 6, ссылка сохраняется в попытке для аудита).

Частичный уникальный индекс `attempts(task_id) WHERE status IN ('dispatching','pending')` гарантирует на уровне БД, что у задачи не больше одной «живой» попытки.

**`error_type` попытки:** `technical` | `moderation` | `payload` приходят из вебхука, `submit_error` означает синхронный 5xx, тайм-аут или сетевую ошибку при `POST /tasks`, `timeout` ставит сторож. Для маршрутизации `submit_error` и `timeout` равносильны `technical`.

**Привязка вебхука к попытке.** В `POST /tasks` провайдеру уходит `reference = attempt.id` (UUID создаётся нами до отправки), и провайдер возвращает его в вебхуке. Поиск идёт по `attempts WHERE id=reference AND provider=name`. Это решает две проблемы:
- вебхук мог прийти раньше, чем мы сохранили `external_id` из ответа 202 (Review Focus 1);
- устаревший вебхук отличается однозначно: `reference <> tasks.current_attempt_id`.

`external_id` сохраняется для истории, если он ещё не записан. Подлинность гарантирует подпись, поэтому проверка совпадения `external_id` не нужна.

**`fail_attempt(conn, attempt_id, error_type)`** — единая функция для `failed`-вебхука, синхронной ошибки и сторожа (по ТЗ все три «ведут туда же»). Вызывающий держит транзакцию:
1. `SELECT t.* FROM tasks t JOIN attempts a ON a.task_id=t.id WHERE a.id=$1 FOR UPDATE OF t`.
2. Если `t.status<>'running'` или `t.current_attempt_id<>$1`, вернуть `None`: попытка устаревшая или дубль.
3. `UPDATE attempts SET status = CASE WHEN $2='timeout' THEN 'timed_out' ELSE 'failed' END, error_type=$2, finished_at=now() WHERE id=$1 AND status IN ('dispatching','pending')`. Если строка не обновилась, вернуть `None`.
4. При `error_type='payload'` выполнить `finish_failed` (правило 2: любая нога отвергнет запрос) и вернуть `None`.
5. Выбрать следующую ногу:
   ```sql
   SELECT l.* FROM legs l JOIN tasks t ON t.model_slug = l.model_slug
   WHERE t.id = $task AND l.priority > $current_priority
     AND l.max_ref_images >= cardinality(t.ref_images)
     AND NOT (l.skip_on_moderation AND EXISTS (
           SELECT 1 FROM attempts WHERE task_id = $task AND error_type = 'moderation'))
   ORDER BY l.priority LIMIT 1;
   ```
6. Если ноги нет, выполнить `finish_failed` (правило 4) и вернуть `None`. Если нога есть: `INSERT attempts (n+1, leg, status 'dispatching', deadline_at = now() + make_interval(secs => PROVIDER_TIMEOUT))`, `UPDATE tasks SET current_attempt_id`, вернуть id новой попытки. Вызывающий отправляет её после `COMMIT`: держать блокировку во время сетевого вызова нельзя.

**`complete_attempt(conn, attempt_id, result_url)`**:
1. Та же блокировка задачи `FOR UPDATE OF t`.
2. Если `t.status='running'`: `UPDATE tasks SET status='completed', result_url, finished_at`. Попытка получает `completed`. Остальные живые попытки задачи получают `abandoned`. Денег не трогаем. Устаревшая попытка (тайм-аут сторожа или тайм-аут отправки) тоже выигрывает, если задача ещё идёт (см. Д5).
3. Если задача уже терминальна: `UPDATE attempts SET status='late_completed', result_url=$2 WHERE id=$1 AND status NOT IN ('completed','late_completed')`. Задачу и деньги не трогаем.

**Отправка** `dispatch(pool, http, attempt_id)` работает в цикле:
- выбирает попытку `WHERE status='dispatching'` (если не нашлась, выход) и делает `POST {url}/tasks` с тайм-аутом `PROVIDER_HTTP_TIMEOUT`;
- при ответе 202: `UPDATE attempts SET status='pending', external_id = coalesce(external_id, $x) WHERE id=$1 AND status='dispatching'`. Если вебхук или сторож успели раньше, затронуто 0 строк, и это нормально;
- при любом другом коде или `httpx.TransportError`/`TimeoutException`: транзакция, затем `fail_attempt(conn, id, 'submit_error')`. Если вернулся новый id, цикл продолжается с ним (правило 3).

`PROVIDER_HTTP_TIMEOUT` (5 с) должен быть меньше `PROVIDER_TIMEOUT`. Если сторож всё же сработает раньше ответа, условный `UPDATE` ничего не затронет.

### Д4. Сторож

Сторож работает в каждом процессе: это `asyncio`-задача из lifespan, которая каждые `WATCHDOG_INTERVAL` секунд выполняет `tick()`. Один шаг — одна транзакция, до 100 шагов за тик:
```sql
SELECT a.id FROM tasks t JOIN attempts a ON a.id = t.current_attempt_id
WHERE t.status = 'running' AND a.status IN ('dispatching','pending') AND a.deadline_at < now()
ORDER BY a.deadline_at LIMIT 1
FOR UPDATE OF t SKIP LOCKED;
-- затем fail_attempt(conn, id, 'timeout') в той же транзакции, COMMIT, dispatch(new_id) после COMMIT
```
- Задвоения нет. `SKIP LOCKED` раздаёт разным процессам разные задачи. Всё равно после блокировки `fail_attempt` перепроверяет `status`/`current_attempt_id`/статус попытки: если другой сторож или вебхук уже продвинул задачу, получается no-op.
- Залипания нет. Дедлайн хранится в БД и сравнивается с `now()` БД, поэтому часы процессов не участвуют. Упавший процесс снимает блокировки откатом, и задачу подберёт любой другой сторож. Лидер не выбирается, значит и умереть ему некому.
- Гонка «completed одновременно со сторожем» снимается блокировкой строки задачи, порядок любой. Если сторож успел первым, задача уходит на следующую ногу (тогда `completed` выигрывает по Д3) или в `failed` с возвратом (тогда `completed` превращается в `late_completed`). Если первым был `completed`, сторож не находит задачу: `status <> 'running'`. В обоих исходах результат и возврат одновременно невозможны.

### Д5. Правило 6 — выбранный вариант

«Сторож сдался» в ТЗ может значить два разных случая, решение принято для каждого.

| Случай | Решение |
|---|---|
| Сторож снял последнюю ногу, задача `failed`, возврат сделан, затем пришёл `completed` | **Игнорировать результат, возврат оставить.** Задача остаётся `failed`, `result_url = NULL`. Попытка получает `late_completed` и хранит ссылку для аудита. Ответ 200, чтобы провайдер не повторял доставку. |
| Сторож перевёл задачу на следующую ногу, она ещё `running`, и пришёл `completed` от прошлой ноги | **Принять результат.** Задача `completed`, текущая попытка получает `abandoned`, поздние вебхуки от неё уходят в `late_completed`. Возврата не было и уже не будет: задача терминальна. |

Почему не «отдать результат и откатить возврат». Потребовалось бы третье движение денег по задаче (повторное списание), а пользователь мог уже потратить возвращённые токены. Повторное списание тогда либо уводит баланс в минус (нарушает `CHECK`), либо проваливается, и мы отдаём результат бесплатно. Кроме того, статус `failed → completed` ломает обещание, что терминальный статус окончателен. Вариант «игнорировать» сводит деньги к простому правилу: на задачу одно списание и не больше одного возврата, никогда не оба исхода сразу.

Почему принимаем результат, пока задача `running`. Это бесплатно для денег, ведь списание уже было, а возврата нет. Пользователь быстрее получает результат. И это единственное согласованное поведение для тайм-аута `POST /tasks` (правило 3): провайдер мог принять задачу, хотя мы считаем отправку неудачной.

`failed`-вебхук от устаревшей попытки всегда игнорируется: он не может продвинуть задачу второй раз.

### Д6. Подпись

`sign(body, secret) = hmac.new(secret, body, sha256).hexdigest()`. Подпись проверяется по `await request.body()` **до** парсинга JSON, сравнение через `hmac.compare_digest`. Если подписи нет или она неверна, ответ `401 {"detail": "invalid signature"}` без чтения тела и без изменения состояния, плюс предупреждение в лог. Неверная подпись означает ошибку конфигурации или подделку, а не повод для повторной доставки. Один общий секрет `WEBHOOK_SECRET` на провайдеров и платежи («с той же подписью»).

### Д7. Фейковые провайдеры

`fakeprovider/main.py` — одно FastAPI-приложение, имя берётся из `PROVIDER_NAME`. Compose запускает два контейнера (`alpha`, `beta`), **по одному воркеру**: конфиг режима хранится в памяти, и для тестового двойника это допустимо.
- `POST /tasks {reference, prompt, ref_images, callback_url}` возвращает `202 {"external_id": uuid4().hex}` и планирует доставку через `asyncio.create_task`. Ссылки на задачи хранятся в `deliveries: set[asyncio.Task]`.
- Доставка: `sleep(uniform(delay_min, delay_max))`. Исход: с вероятностью `fail_rate` получается `failed` с `fail_type`, иначе `completed` с `result_url = f"https://files.example/{external_id}.png"`. Тело подписывается и отправляется на `callback_url`.
- Конфиг `FakeConfig`: `fail_rate: float=0`, `fail_type: technical|moderation|payload = technical`, `submit_error: none|503|timeout = none`, `submit_sleep: float = 30` (сон перед ответом в режиме `timeout`), `webhook: once|twice_sequential|twice_concurrent|never = once`, `delay_min: float = 2`, `delay_max: float = 20`. Поздний `completed` получается как `delay_min > PROVIDER_TIMEOUT`. Молчание провайдера — это `webhook=never`.
- `PUT /_admin/config` (частичное обновление) и `POST /_admin/reset`. Значения по умолчанию берутся из env `FAKE_CONFIG` (JSON).
- Вебхук: `{"reference", "external_id", "status": "completed"|"failed", "result_url"?, "error_type"?}`.

### Д8. Миграции

Миграции — чистые SQL-файлы `migrations/NNNN_name.sql` и раннер на ~25 строк `python -m app.migrate`. Раннер:
- берёт `pg_advisory_lock`, чтобы параллельные запуски не мешали друг другу;
- создаёт таблицу `schema_migrations(version)`;
- применяет каждый новый файл по порядку в своей транзакции.

Почему не Alembic: доступ к БД построен на asyncpg без SQLAlchemy, а Alembic принёс бы SQLAlchemy, env.py и autogenerate, которые здесь бесполезны. Миграции всё равно были бы `op.execute(sql)`. Следующая миграция добавляется так: создать `migrations/0003_<что>.sql` и выполнить `docker compose run --rm migrate`. Каталог моделей и ног лежит в `0002_catalog.sql`.

### Д9. Почему asyncpg, а не SQLAlchemy async

Все критичные места — это конкретный SQL: `FOR NO KEY UPDATE`, `FOR UPDATE OF t SKIP LOCKED`, `ON CONFLICT ... DO NOTHING RETURNING`, условные `UPDATE ... RETURNING`. В SQLAlchemy их всё равно пришлось бы писать через `text()`, а слой сессий и unit-of-work скрывает момент, когда реально уходит запрос и берётся блокировка. С asyncpg видна каждая транзакция (`async with conn.transaction()`) и каждый запрос. Пул на процесс создаётся в lifespan, `min_size=2, max_size=20`.

### Д10. API

| Запрос | Тело / ответ | Коды |
|---|---|---|
| `POST /generate` | `{user_id: int, model_slug: str, prompt: str (непустой), ref_images: list[str] = []}` → `{"task_id": uuid}` | 202; 400 (нет подходящей ноги или неизвестная модель); 402; 404 (неизвестный пользователь); 422; 429 |
| `POST /webhooks/providers/{name}` | вебхук из Д7, заголовок `X-Signature` → `{"ok": true}` | 200 (в том числе дубль, устаревший или поздний вебхук); 401; 404 (неизвестный `name`, или `reference` не найден у этого провайдера); 422 |
| `POST /webhooks/payments` | `{payment_id: str, user_id: int, tokens: int > 0, status: str}`, `X-Signature` → `{"credited": bool}` | 200 (в том числе дубль и не-`succeeded`); 401; 422 |
| `GET /tasks/{id}` | `{id, status, model_slug, result_url, current_leg: {provider, priority, attempt_id}, attempts: [{id, n, provider, status, error_type, external_id, created_at, finished_at}]}` | 200; 404; 422 |
| `GET /users/{id}/balance` | `{user_id, balance, ledger: [{id, amount, reason, payment_id, task_id, created_at}]}` (последние 20, новые первыми) | 200; 404 |

Неизвестная модель возвращает 400, а не 404: это ошибка содержимого запроса, такая же, как «нет ноги под 11 референсов».

---

## Неоднозначности и допущения

| # | Место в ТЗ | Проблема | Решение |
|---|---|---|---|
| 1 | Правило 1: «принимает столько референсов, сколько прислал» | Буквально это `==`, но поле называется `max_ref_images` («до 4») | Нога подходит, если `max_ref_images >= len(ref_images)` |
| 2 | Домен: `skip_on_moderation` — «если предыдущая отказала по модерации» | «Предыдущая» может значить «непосредственно предыдущая попытка» или «любая из прошлых» | Решение «липкое»: нога пропускается, если **любая** прошлая попытка задачи отказала по модерации. Модерация — свойство содержимого, а оно не меняется. Для текущего каталога (2 ноги) оба прочтения совпадают. Пропуск означает «перейти к следующей ноге», а не остановиться |
| 3 | «3 активные задачи — 429» | Не определено, что такое «активная», и неясно, при каком по счёту запросе 429 | Активная = `running`. Четвёртая одновременная получает 429 |
| 4 | 402 vs 429 одновременно | Порядок не задан | Сначала 429 (пополнение не поможет), потом 402 |
| 5 | `/webhooks/payments`: поле `status` | Значения не перечислены | Начисляет только `succeeded`. Любое другое значение даёт 200 `{"credited": false}` без строки журнала. Chargeback вне объёма ТЗ |
| 6 | Повтор оплаты с другим телом | Не описано | Выигрывает первая доставка `payment_id`, остальные доставки — no-op |
| 7 | Откуда берутся пользователи | Регистрации нет | Пользователь создаётся при первой оплате (`INSERT ... ON CONFLICT DO NOTHING`). `/generate` и `/balance` для неизвестного пользователя дают 404 |
| 8 | «с той же подписью» | Тот же секрет или та же схема? | Та же схема и тот же `WEBHOOK_SECRET` |
| 9 | Формат вебхука провайдера и его связь с попыткой | Не задан | Формат из Д7: мы передаём `reference = attempt_id`, провайдер возвращает его в вебхуке |
| 10 | Правило 3: только «5xx, таймаут» | 4xx и сетевые ошибки не упомянуты | Любой ответ, кроме 202, и любая транспортная ошибка дают `submit_error`, это аналог `technical` |
| 11 | Тайм-аут `POST /tasks` | Провайдер мог принять задачу и позже прислать вебхук | `completed` от такой попытки принимается, пока задача `running`. `failed` от неё игнорируется (Д5) |
| 12 | Правило 6: «сторож сдался» | Сдался на попытке или на задаче? | Оба случая разобраны в Д5 |
| 13 | `PROVIDER_TIMEOUT` «для тестов 60 с» | Прод-значение не задано | Значение по умолчанию и в compose — 60. Отсчёт идёт от создания попытки |
| 14 | Провайдер «2–20 секунд» | 20 с замедляют тесты | Задержка настраивается (`delay_min/max`), в тестах 0–0.5 с |
| 15 | Идемпотентность `POST /generate` | Не указана | **Не делается:** каждый вызов — новый заказ и новое списание. Путь расширения: заголовок `Idempotency-Key` и `UNIQUE (user_id, idempotency_key)` в `tasks`. Записано в README |
| 16 | «Текущая нога» у терминальной задачи | Не определено | `current_leg` показывает ногу последней текущей попытки |
| 17 | Приоритеты в каталоге | Числа не даны, есть только порядок | Приоритет 1, 2 в порядке таблицы. Меньшее значение пробуется раньше |
| 18 | Код ответа `/generate` | Не задан | 202: задача асинхронная |
| 19 | Что получает пользователь при `late_completed` | Не задано | Результата нет (`result_url = NULL`), ссылка остаётся только в истории попытки. Пользователь получил возврат |
| 20 | Повторная отправка той же ноги после падения процесса до HTTP | Не задано | Не делается. Попытка в `dispatching` после дедлайна засчитывается как неудача, правило 3 и Д4 |

---

## File Structure

```
pyproject.toml                 зависимости, настройки pytest (asyncio_mode=auto)
Dockerfile                     один образ для api, migrate, alpha/beta, tests
docker-compose.yml             db, migrate (одноразовый), api (4 воркера), alpha, beta, tests (profile test)
.gitignore
README.md                      запуск, тесты, «ровно один раз», падение процесса, правило 6, миграции
migrations/0001_schema.sql     таблицы, ограничения, триггеры журнала
migrations/0002_catalog.sql    модели и ноги
app/__init__.py
app/config.py                  Settings из env (frozen dataclass)
app/migrate.py                 раннер SQL-миграций
app/signing.py                 sign / verify (HMAC-SHA256)
app/money.py                   credit_payment, refund_task, get_balance (журнал)
app/orchestrator.py            create_task, dispatch, fail_attempt, complete_attempt, apply_provider_webhook, get_task
app/watchdog.py                tick, run
app/main.py                    FastAPI: lifespan (пул, httpx, сторож), маршруты, pydantic-схемы
fakeprovider/__init__.py
fakeprovider/main.py           фейковый alpha/beta
tests/test_fakeprovider.py     фейк in-process, без БД
tests/e2e/conftest.py          фикстуры: api, db, сброс фейков, инвариант баланса, хелперы
tests/e2e/test_schema.py       миграции, каталог, триггеры и уникальности журнала
tests/e2e/test_payments.py     начисление, подпись, баланс
tests/e2e/test_generate.py     приём заказа: 400/402/404/429, списание, выбор ноги
tests/e2e/test_failover.py     вебхуки провайдеров, переходы между ногами, возврат
tests/e2e/test_watchdog.py     сторож, правило 6, гонки
```

Тесты в `tests/e2e/` работают с поднятым compose через HTTP (`API_URL`) и напрямую с БД (`DATABASE_URL`) для проверок и подготовки данных. Так тесты проверяют настоящие 4 процесса и настоящие блокировки PostgreSQL. Подписанные вебхуки тесты отправляют сами, когда нужен детерминизм. Фейки во время e2e-тестов по умолчанию молчат (`webhook=never`), и тест явно включает нужный режим.

Цикл исполнителя после правки кода: `docker compose up -d --build --wait`, затем `docker compose run --rm tests pytest <путь> -v`. Сервис `tests` монтирует `.:/srv`, поэтому тесты пересобирать не нужно.

---

### Task 1: Каркас, схема, миграции, журнал с триггером

**Files:**
- Create: `pyproject.toml`, `Dockerfile`, `docker-compose.yml` (сервисы `db`, `migrate`, `tests`), `.gitignore`, `app/__init__.py`, `app/config.py`, `app/migrate.py`, `migrations/0001_schema.sql`, `migrations/0002_catalog.sql`
- Test: `tests/e2e/conftest.py`, `tests/e2e/test_schema.py`

**Interfaces:**
- Produces:
  - `app.config.Settings` (frozen dataclass), `app.config.settings = Settings.from_env()`. Поля: `database_url: str`, `webhook_secret: str`, `provider_timeout: float = 60`, `provider_http_timeout: float = 5`, `watchdog_interval: float = 1`, `provider_urls: dict[str, str]` (из `ALPHA_URL`, `BETA_URL`), `public_url: str`.
  - `app.migrate.migrate(dsn: str, directory: Path = Path("migrations")) -> list[str]` возвращает версии, применённые в этом запуске. `python -m app.migrate` применяет к `settings.database_url`.
  - Таблицы `users`, `models`, `legs`, `tasks`, `attempts`, `ledger`, `schema_migrations` с ограничениями из Д1/Д3. `tasks.ref_images text[]`, `tasks.id`/`attempts.id` типа `uuid DEFAULT gen_random_uuid()`, `attempts.provider text` (копия из ноги для удобства запросов), `attempts.n int`, `UNIQUE (task_id, n)`, `legs UNIQUE (model_slug, priority)`. Индексы: `tasks(user_id) WHERE status='running'`, `attempts(deadline_at) WHERE status IN ('dispatching','pending')`, частичный уникальный `attempts(task_id) WHERE status IN ('dispatching','pending')`.
  - Фикстура `db` (asyncpg-соединение). Autouse-фикстура `balance_invariant` (Д1) в `tests/e2e/conftest.py`.

- [ ] **Step 1: `git init`, создать `pyproject.toml`, `Dockerfile` (python:3.12-slim, `pip install .[dev]`, `WORKDIR /srv`) и `docker-compose.yml`**
  - `db`: `postgres:16`, healthcheck `pg_isready`, порт 5432.
  - `migrate`: `command: python -m app.migrate`, `depends_on: db: service_healthy`.
  - `tests`: `profiles: [test]`, `volumes: [.:/srv]`, env `DATABASE_URL`, `API_URL=http://api:8000`, `ALPHA_URL`, `BETA_URL`, `WEBHOOK_SECRET`, `PROVIDER_TIMEOUT`.

  Зависимости: `fastapi`, `uvicorn[standard]`, `asyncpg`, `httpx`; dev: `pytest`, `pytest-asyncio`.

- [ ] **Step 2: Написать падающие тесты `tests/e2e/test_schema.py`**
  - `test_migrate_is_idempotent`: `await migrate(DATABASE_URL) == []` (compose уже применил миграции).
  - `test_catalog_seeded`: цены `{"image-fast": 2, "video-long": 25}`. Ноги `image-fast` = `[("alpha",1,4,False),("beta",2,10,False)]`, `video-long` = `[("beta",1,2,False),("alpha",2,1,True)]` (provider, priority, max_ref_images, skip_on_moderation).
  - `test_ledger_insert_moves_balance`: новый пользователь, вставка `payment` на 10 → `balance == 10`.
  - `test_balance_cannot_go_negative`: вставка `charge` на −11 при балансе 10 → `asyncpg.CheckViolationError`.
  - `test_ledger_is_append_only`: `UPDATE ledger` и `DELETE FROM ledger` → `asyncpg.RaiseError`.
  - `test_payment_id_unique` и `test_one_refund_per_task` (задача вставляется напрямую SQL) → `asyncpg.UniqueViolationError` на второй строке.
  - `test_one_live_attempt_per_task`: вторая попытка в `pending` у той же задачи → `UniqueViolationError`.

  Каждый тест использует свежий `user_id = random.randint(10**9, 2**62)`.

- [ ] **Step 3: Запустить и убедиться, что тесты падают**
  Run: `docker compose up -d --build --wait db && docker compose run --rm tests pytest tests/e2e/test_schema.py -v`
  Expected: FAIL / ERROR (модуля `app.migrate` и таблиц ещё нет).

- [ ] **Step 4: Реализовать `migrations/0001_schema.sql`, `migrations/0002_catalog.sql`, `app/config.py`, `app/migrate.py`**
  Схема берётся из Д1/Д3. Триггеры — две функции PL/pgSQL (`ledger_apply`, `ledger_append_only`). Раннер по Д8: `pg_advisory_lock(727001)`, каждый файл в своей транзакции, затем `INSERT INTO schema_migrations`.

- [ ] **Step 5: Запустить тесты**
  Run: `docker compose up -d --build --wait db && docker compose run --rm migrate && docker compose run --rm tests pytest tests/e2e/test_schema.py -v`
  Expected: PASS (8 тестов).

- [ ] **Step 6: Commit**
  ```bash
  git add -A
  git commit -m "feat(db): schema with append-only ledger that drives balance via trigger

  Balance == sum(ledger) is enforced by the database itself: the only way to
  change users.balance is an INSERT into ledger, so no future code path can
  move money without a journal row. UNIQUE(payment_id) and UNIQUE(task_id,
  reason) make double credit/refund impossible even if app logic is wrong.
  Plain SQL migrations + 25-line runner instead of Alembic: we use asyncpg
  without SQLAlchemy, so Alembic would add a stack only to run raw SQL."
  ```

---

### Task 2: Подпись, платёжный вебхук, баланс

**Files:**
- Create: `app/signing.py`, `app/money.py`, `app/main.py`
- Modify: `docker-compose.yml` (сервис `api`: `uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4`, `depends_on: migrate: service_completed_successfully`, healthcheck через `urllib` на `/docs`, порт 8000)
- Modify: `tests/e2e/conftest.py`
- Test: `tests/e2e/test_payments.py`

**Interfaces:**
- Consumes: `settings`, таблицы из Task 1.
- Produces:
  - `app.signing.sign(body: bytes, secret: str) -> str` (hex) и `app.signing.verify(body: bytes, signature: str | None, secret: str) -> bool`.
  - `app.money.credit_payment(pool, payment_id: str, user_id: int, tokens: int) -> bool`.
  - `app.money.refund_task(conn, task_id: UUID) -> bool` выполняет SQL-вставку возврата из Д1. Вызывается только внутри транзакции перехода `running → failed`, используется в Task 5.
  - `app.money.get_balance(pool, user_id: int, limit: int = 20) -> dict | None`.
  - `app.main.app`, lifespan кладёт `app.state.pool`. Зависимость `async def signed_body(request) -> bytes` возвращает 401 при неверной подписи.
  - Хелперы conftest: фикстура `api` (`httpx.AsyncClient(base_url=API_URL, timeout=30)`), `signed(payload: dict) -> tuple[bytes, dict]`, `async def new_user(api, tokens: int) -> int` (оплата с уникальным `payment_id`), `async def ledger_rows(db, user_id, reason=None) -> list`.

- [ ] **Step 1: Написать падающие тесты `tests/e2e/test_payments.py`**
  - `test_twenty_concurrent_payment_webhooks_credit_once` (**обязательный №1**). Одно и то же подписанное тело отправляется 20 раз через `asyncio.gather(*[api.post("/webhooks/payments", content=raw, headers=h) for _ in range(20)])`. Конкурентность: 20 параллельных соединений httpx, ядро раздаёт их между 4 воркерами uvicorn, и вставки встречаются на уникальном индексе PostgreSQL. Ожидание: все ответы 200, ровно один `credited: true`, `balance == 100`, одна строка `payment` с этим `payment_id`.
  - `test_same_payment_id_different_body_credits_once` (Review Focus 3): второе тело с `tokens=999` → `credited: false`, баланс 100.
  - `test_non_succeeded_payment_does_not_credit`: `status="failed"` → 200, `credited: false`, строк в журнале нет.
  - `test_invalid_or_missing_signature_401`: неверная подпись и отсутствующий заголовок → 401, баланс не изменился (`/users/{id}/balance` → 404 для нового id).
  - `test_signature_is_over_raw_body`: тело `b'{ "payment_id" : "...", ... }'` с лишними пробелами и правильной подписью этих байтов → 200 `credited: true`.
  - `test_balance_endpoint_shows_ledger`: `balance == 100`, `ledger[0]` = `{amount: 100, reason: "payment", payment_id: ...}`. Неизвестный пользователь → 404.
  - `test_tokens_must_be_positive`: `tokens=0` → 422.

- [ ] **Step 2: Запустить, убедиться, что падают**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e/test_payments.py -v`
  Expected: FAIL (сервиса `api` или маршрутов ещё нет).

- [ ] **Step 3: Реализовать `app/signing.py`, `app/money.py`, `app/main.py`**
  `credit_payment` — две SQL-команды из Д1 в одной транзакции, возвращает `bool(row)`. Подпись проверяется до `model_validate_json(raw)` по Д6. `PaymentWebhook` (pydantic): `payment_id: str = Field(min_length=1, max_length=200)`, `user_id: int = Field(gt=0)`, `tokens: int = Field(gt=0)`, `status: str`.

- [ ] **Step 4: Запустить тесты**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e -v`
  Expected: PASS (schema + payments), фикстура инварианта зелёная.

- [ ] **Step 5: Commit**
  ```bash
  git add -A
  git commit -m "feat(payments): idempotent signed payment webhook

  Credit is a single INSERT ... ON CONFLICT (payment_id) DO NOTHING; duplicate
  and concurrent deliveries wait on the unique index and become no-ops, so
  exactly-once does not depend on in-process state across the 4 workers.
  Signature is checked on the raw bytes with compare_digest before parsing,
  because re-serialised JSON would not match what the sender signed."
  ```

---

### Task 3: Фейковые провайдеры alpha/beta

**Files:**
- Create: `fakeprovider/__init__.py`, `fakeprovider/main.py`
- Modify: `docker-compose.yml`. Сервисы `alpha` и `beta`: `uvicorn fakeprovider.main:app --host 0.0.0.0 --port 8000` (1 воркер), env `PROVIDER_NAME`, `WEBHOOK_SECRET`, порты 8001/8002, healthcheck на `/docs`. В `api` добавить `ALPHA_URL=http://alpha:8000`, `BETA_URL=http://beta:8000`, `PUBLIC_URL=http://api:8000`.
- Test: `tests/test_fakeprovider.py`

**Interfaces:**
- Consumes: `app.signing.sign`, `app.signing.verify`.
- Produces:
  - `fakeprovider.main.app`, `FakeConfig` (Д7), `config: FakeConfig` (модульное состояние), `deliveries: set[asyncio.Task]`, `client: httpx.AsyncClient` (тесты подменяют его на клиент с `httpx.MockTransport`).
  - HTTP: `POST /tasks`, `PUT /_admin/config`, `POST /_admin/reset`.
  - Хелпер e2e-conftest `async def configure(provider: str, **cfg) -> None` (PUT в `ALPHA_URL`/`BETA_URL`). Autouse-фикстура `quiet_fakes`: перед каждым e2e-тестом `reset` и `configure(p, webhook="never")` для обоих провайдеров.

- [ ] **Step 1: Написать падающие тесты `tests/test_fakeprovider.py`**
  Тесты in-process: `httpx.AsyncClient(transport=ASGITransport(fakeprovider.main.app))`, `client` подменён на `MockTransport`, который записывает запросы. После `POST /tasks` выполняется `await asyncio.gather(*deliveries)`.
  - `test_accepts_task_202_with_external_id`.
  - `test_completed_webhook_is_signed`: `delay_min=delay_max=0`. Ровно один запрос на `callback_url`, `verify(body, headers["X-Signature"], secret)`, `status=="completed"`, `reference` совпадает, есть `result_url`.
  - `test_fail_rate_one_sends_failed_with_type`: `fail_rate=1, fail_type="moderation"` → `error_type=="moderation"`.
  - `test_submit_503` → 503, вебхуков нет. `test_submit_timeout_sleeps`: `submit_sleep=0.2`, клиент с тайм-аутом 0.05 → `httpx.ReadTimeout`.
  - `test_twice_concurrent_sends_two_identical`: 2 запроса с одинаковыми телами.
  - `test_never_sends_nothing`.

- [ ] **Step 2: Запустить, убедиться, что падают**
  Run: `docker compose run --rm tests pytest tests/test_fakeprovider.py -v`
  Expected: FAIL (`ModuleNotFoundError: fakeprovider`).

- [ ] **Step 3: Реализовать `fakeprovider/main.py` по Д7**
  `twice_concurrent` реализуется как `asyncio.gather(post(), post())`. Ошибки отправки вебхука глотаются с логом, потому что фейк не повторяет доставку.

- [ ] **Step 4: Запустить тесты фейка и проверить compose**
  Run: `docker compose run --rm tests pytest tests/test_fakeprovider.py -v && docker compose up -d --build --wait`
  Expected: PASS, `alpha` и `beta` в статусе healthy.

- [ ] **Step 5: Commit**
  ```bash
  git add -A
  git commit -m "test: fake alpha/beta providers switchable at runtime

  One app, two containers: the providers differ only by name. Modes (fail
  rate/type, 503/timeout on submit, duplicate or concurrent webhook, silence,
  configurable delay for late completed) are set via an admin endpoint so
  each e2e test enables exactly the failure it exercises. Single worker on
  purpose: the mode lives in memory, acceptable for a test double only."
  ```

---

### Task 4: `POST /generate`, отправка на первую ногу, `GET /tasks/{id}`

**Files:**
- Create: `app/orchestrator.py`
- Modify: `app/main.py` (маршруты, `app.state.http = httpx.AsyncClient()`)
- Modify: `tests/e2e/conftest.py` (`wait_task`)
- Test: `tests/e2e/test_generate.py`

**Interfaces:**
- Consumes: `settings`, `app.state.pool`, `app.state.http`.
- Produces:
  - `class AdmissionError(Exception)` с полями `status_code: int` и `detail: str`.
  - `create_task(pool, user_id: int, model_slug: str, prompt: str, ref_images: list[str]) -> tuple[UUID, UUID]` возвращает `(task_id, attempt_id)` и бросает `AdmissionError(400|402|404|429)`.
  - `dispatch(pool, http: httpx.AsyncClient, attempt_id: UUID) -> None`. В этой задаче реализуется только ветка 202. Ветку ошибки добавит Task 5.
  - `get_task(pool, task_id: UUID) -> dict | None` (форма из Д10).
  - Константа `MAX_ACTIVE_TASKS = 3`. Маршрут добавляет `BackgroundTasks.add_task(dispatch, pool, http, attempt_id)`.
  - Хелпер conftest: `async def wait_task(api, task_id, until: Callable[[dict], bool], timeout: float = 15) -> dict` опрашивает каждые 0.2 с и падает с последним состоянием при тайм-ауте.

- [ ] **Step 1: Написать падающие тесты `tests/e2e/test_generate.py`** (фейки молчат, задачи остаются `running`)
  - `test_generate_charges_and_dispatches_first_leg`: 202 с `task_id`. Баланс 10 − 2 = 8. Ровно одна строка `charge` с `amount=-2` и этим `task_id`. `wait_task(... attempts[0]["status"] == "pending")`: provider `alpha`, `external_id` не пустой, `current_leg.provider == "alpha"`.
  - `test_ref_count_picks_first_eligible_leg`: `image-fast` с 5 референсами → первая попытка `beta`.
  - `test_no_eligible_leg_400_before_charge`: `image-fast` с 11 референсами и `video-long` с 3 → 400. Баланс не изменился, строк `charge` нет, задач у пользователя нет.
  - `test_unknown_model_400_unknown_user_404`.
  - `test_concurrent_generate_respects_active_limit`: баланс 100, `asyncio.gather` из 6 `/generate` → ровно 3 × 202 и 3 × 429. Строк `charge` 3, баланс 94. Конкурентность: 6 параллельных HTTP-запросов, которые попадают на разные воркеры и сериализуются только блокировкой строки пользователя.
  - `test_concurrent_generate_respects_balance`: баланс 3, `gather` из 2 × `image-fast` → ровно один 202 и один 402. Баланс 1, никогда не меньше 0.
  - `test_get_task_404`: случайный uuid → 404.

- [ ] **Step 2: Запустить, убедиться, что падают**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e/test_generate.py -v`
  Expected: FAIL (404 на `/generate`).

- [ ] **Step 3: Реализовать `create_task`, `dispatch` (ветка 202), `get_task` и маршруты**
  Порядок проверок и блокировка — по Д2. Ноги выбираются одним запросом `SELECT ... FROM legs WHERE model_slug=$1 ORDER BY priority` до транзакции. Первая попытка: `n=1`, `status='dispatching'`, `deadline_at = now() + make_interval(secs => $timeout)`. Тело запроса к провайдеру: `{reference, prompt, ref_images, callback_url: f"{public_url}/webhooks/providers/{provider}"}`.

- [ ] **Step 4: Запустить тесты**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e -v`
  Expected: PASS.

- [ ] **Step 5: Commit**
  ```bash
  git add -A
  git commit -m "feat(generate): admit, charge and dispatch under the user row lock

  Limit (429) and balance (402) checks, task insert and charge run in one
  transaction holding SELECT ... FOR NO KEY UPDATE on the user, so parallel
  requests on different workers serialise per user and cannot both pass.
  NO KEY UPDATE rather than UPDATE: it does not conflict with FK key-share
  locks from ledger inserts, keeping lock waits one-directional. The 400
  for no eligible leg is decided before any lock or charge. Provider call
  happens after COMMIT: never hold a row lock across the network."
  ```

---

### Task 5: Вебхуки провайдеров и переходы между ногами (правила 1–4)

**Files:**
- Modify: `app/orchestrator.py` (`fail_attempt`, `complete_attempt`, `finish_failed`, `apply_provider_webhook`, ветка ошибки в `dispatch`)
- Modify: `app/main.py` (маршрут `POST /webhooks/providers/{name}`)
- Modify: `tests/e2e/conftest.py` (`provider_webhook`)
- Test: `tests/e2e/test_failover.py`

**Interfaces:**
- Consumes: `refund_task(conn, task_id)` из Task 2, `dispatch` и `create_task` из Task 4.
- Produces:
  - `fail_attempt(conn, attempt_id: UUID, error_type: str) -> UUID | None`, вызывается внутри транзакции (Д3).
  - `complete_attempt(conn, attempt_id: UUID, result_url: str) -> None`, внутри транзакции (Д3).
  - `finish_failed(conn, task_id: UUID) -> None`: условный `UPDATE tasks ... status='failed'`, затем `refund_task` только если строка вернулась.
  - `apply_provider_webhook(pool, provider: str, payload: ProviderWebhook) -> UUID | None` бросает `LookupError`, что даёт 404. Возвращает id новой попытки, маршрут ставит её в `BackgroundTasks`.
  - `ProviderWebhook` (pydantic): `reference: UUID`, `external_id: str`, `status: Literal["completed","failed"]`, `result_url: str | None`, `error_type: Literal["technical","moderation","payload"] | None`. Валидатор: при `completed` обязателен `result_url`, при `failed` обязателен `error_type`, иначе 422.
  - Хелпер conftest: `async def provider_webhook(api, name: str, **payload) -> httpx.Response` (подписывает сам).

- [ ] **Step 1: Написать падающие тесты `tests/e2e/test_failover.py`**
  Для «быстрого» режима используется `configure(p, webhook="once", delay_min=0, delay_max=0.5)`.
  - `test_first_leg_fails_second_completes` (**обязательный №3**). `alpha`: `fail_rate=1, fail_type="technical"`, `beta` отвечает успехом. `image-fast` без референсов → `wait_task(status=="completed")`. В `attempts` ровно 2 записи: `[alpha, failed, technical]` и `[beta, completed]`. `result_url` не пустой. Баланс 10 − 2, строк `refund` нет.
  - `test_ten_concurrent_failed_on_last_leg_refund_once` (**обязательный №2**). `image-fast` с 5 референсами, поэтому единственная (а значит последняя) нога — `beta`, и она молчит. Берётся `attempt_id = attempts[0].id`. `asyncio.gather` из 10 одинаковых подписанных `failed/technical` с этим `reference`; все запросы идут параллельно на 4 воркера и встречаются на `FOR UPDATE` строки задачи. Ожидание: все ответы 200, задача `failed`, ровно одна строка `refund` на 2, баланс равен исходному.
  - `test_payload_fails_immediately_no_next_leg`: `alpha` с `fail_type="payload"` → `failed`, 1 попытка, возврат сделан, `beta` не пробовался.
  - `test_moderation_skips_flagged_leg`: `video-long` без референсов, `beta` с `fail_type="moderation"` → `failed`, 1 попытка (`alpha` со `skip_on_moderation` пропущена), возврат 25.
  - `test_technical_does_not_skip_flagged_leg`: `video-long`, `beta` с `technical`, `alpha` отвечает успехом → `completed`, 2 попытки.
  - `test_ref_count_limits_chain`: `video-long` с 2 референсами, `beta` с `technical` → `failed` с одной попыткой (`alpha` принимает только 1 референс) и возвратом.
  - `test_all_legs_fail_refund_once`: обе ноги `image-fast` с `technical` → `failed`, 2 попытки, одна строка `refund`.
  - `test_sync_503_moves_to_next_leg`: `alpha` с `submit_error="503"` → `completed` на `beta`, `attempts[0].error_type == "submit_error"`.
  - `test_sync_timeout_moves_to_next_leg`: `alpha` с `submit_error="timeout", submit_sleep=10, webhook="never"` → `completed` на `beta` за время меньше 15 с, `attempts[0].error_type == "submit_error"`.
  - `test_provider_duplicate_concurrent_webhooks`: `alpha` с `webhook="twice_concurrent"` → `completed`, 1 попытка, состояние стабильно: через 1 с всё то же.
  - `test_webhook_before_dispatch_recorded` (Review Focus 1): `alpha` с `delay_min=delay_max=0` → `completed`, `attempts[0].status == "completed"`, `external_id` не пустой.
  - `test_webhook_wrong_provider_or_reference_404` (Review Focus 4): `reference` попытки `alpha`, отправленный на `/webhooks/providers/beta`, даёт 404. Случайный uuid даёт 404, `/webhooks/providers/gamma` даёт 404. Задача остаётся `running`, попытка не изменилась.
  - `test_provider_webhook_bad_signature_401`.

- [ ] **Step 2: Запустить, убедиться, что падают**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e/test_failover.py -v`
  Expected: FAIL.

- [ ] **Step 3: Реализовать `fail_attempt`, `complete_attempt`, `finish_failed` по алгоритмам Д3**
  Блокировка задачи `FOR UPDATE OF t`. Перепроверки (`status`, `current_attempt_id`, статус попытки) выполняются после блокировки. Запрос выбора следующей ноги — дословно из Д3.

- [ ] **Step 4: Реализовать `apply_provider_webhook`, маршрут и ветку ошибки `dispatch`**
  Маршрут: подпись (Д6), затем `name in settings.provider_urls` (иначе 404), затем транзакция. В ней: `SELECT ... FROM attempts WHERE id=$ref AND provider=$name` (иначе `LookupError`), `UPDATE attempts SET external_id = coalesce(external_id, $ext)`, затем `complete_attempt` или `fail_attempt`. После коммита новая попытка уходит в `BackgroundTasks`. В `dispatch` при ответе не 202 или исключении httpx: транзакция `fail_attempt(conn, id, "submit_error")`, затем цикл с новой попыткой.

- [ ] **Step 5: Запустить тесты**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e -v`
  Expected: PASS.

- [ ] **Step 6: Commit**
  ```bash
  git add -A
  git commit -m "feat(tasks): leg failover with a single locked transition per task

  Webhook failure, sync submit error and (next) watchdog timeout all go
  through fail_attempt, as the spec says they lead to the same place. It
  locks the task row and re-checks that the attempt is still current, so
  duplicate or stale webhooks are no-ops and a refund happens only on the
  one running->failed transition (UNIQUE(task_id, reason) as backstop).
  Webhooks find the attempt by our own reference, not external_id, so a
  callback that beats the 202 response is not lost."
  ```

---

### Task 6: Сторож и правило 6

**Files:**
- Create: `app/watchdog.py`
- Modify: `app/main.py` (lifespan: `task = asyncio.create_task(watchdog.run(pool, http, settings.watchdog_interval))`, при остановке — отмена)
- Modify: `docker-compose.yml` (у `api`: `PROVIDER_TIMEOUT=60`, `WATCHDOG_INTERVAL=1`)
- Test: `tests/e2e/test_watchdog.py`

**Interfaces:**
- Consumes: `fail_attempt`, `complete_attempt`, `dispatch`.
- Produces:
  - `watchdog.tick(pool) -> list[UUID]` возвращает id новых попыток для отправки.
  - `watchdog.run(pool, http, interval: float) -> None` — бесконечный цикл: `tick`, затем `dispatch` каждой новой попытки, затем `sleep(interval)`. Исключения логируются, цикл не умирает.
  - Фикстура conftest `pool` (`asyncpg.create_pool(DATABASE_URL)`) для прямых вызовов `tick` из теста гонки.

- [ ] **Step 1: Написать падающие тесты `tests/e2e/test_watchdog.py`**
  Хелпер `nudge(db, attempt_id)`: `UPDATE attempts SET deadline_at = now() WHERE id=$1`. Он нужен, чтобы не ждать 60 с там, где проверяется не сам тайм-аут.
  - `test_late_completed_after_watchdog_gave_up` (**обязательный №4**, честное ожидание `PROVIDER_TIMEOUT`). `image-fast` с 5 референсами, `beta` молчит. `wait_task(status=="failed", timeout=PROVIDER_TIMEOUT + 15)`: `attempts[0].status == "timed_out"`, `error_type == "timeout"`, одна строка `refund`. Затем подписанный `completed` с этим `reference` → 200. Задача по-прежнему `failed`, `result_url is None`, `attempts[0].status == "late_completed"`, ссылка лежит в попытке. Одна строка `charge`, одна `refund`, баланс равен исходному.
  - `test_watchdog_moves_silent_attempt_to_next_leg`: использует хелпер `advance_to_second_leg(api, db) -> dict`: `image-fast` без референсов, обе ноги молчат, `nudge` первой попытки, затем `wait_task` до двух попыток. Ожидание: `[alpha, timed_out]`, `[beta, pending]`, задача `running`, возврата нет.
  - `test_superseded_attempt_completed_wins_while_running` (Review Focus 5, правило 6 для `running`): свой вызов `advance_to_second_leg`, затем `completed` с `reference` первой (alpha) попытки → задача `completed` с этим `result_url`. Попытка beta получает `abandoned`, строк `refund` 0. Затем `failed` от beta → 200, ничего не меняется.
  - `test_completed_races_watchdog_never_both`: 8 задач (`image-fast`, 5 референсов, `beta` молчит, одна нога) с `deadline_at = now() - interval '1 s'`, проставленным одним `UPDATE`. Затем `asyncio.gather` запускает 8 подписанных `completed` через HTTP и 4 прямых вызова `watchdog.tick(pool)` с собственным пулом теста. Сторожа сервиса тоже участвуют, поэтому за строку задачи реально гонятся и сторож, и вебхук. Для каждой задачи ровно один исход: (`completed` и 0 `refund`) или (`failed`, 1 `refund`, попытка `late_completed`). Распределение исходов не проверяется, проверяется только «никогда не оба».
  - `test_orphaned_dispatching_attempt_recovered` (Review Focus 2, падение после коммита до отправки): `UPDATE attempts SET status='dispatching', deadline_at=now()` у текущей попытки → в течение 5 с задача переходит на следующую ногу (`image-fast` без референсов, `attempts[1].provider == "beta"`).
  - `test_watchdog_does_not_double_advance`: 5 задач, `nudge` всех одновременно. Через 5 с у каждой ровно 2 попытки (не 3), что проверяет отсутствие задвоения между 4 процессами.

- [ ] **Step 2: Запустить, убедиться, что падают**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest tests/e2e/test_watchdog.py -v`
  Expected: FAIL (тайм-ауты `wait_task`: сторожа нет). `complete_attempt` из Task 5 уже обрабатывает `late_completed`, поэтому падает в первую очередь часть про сторожа.

- [ ] **Step 3: Реализовать `app/watchdog.py` по Д4 и подключить в lifespan**

- [ ] **Step 4: Запустить весь набор**
  Run: `docker compose up -d --build --wait && docker compose run --rm tests pytest -v`
  Expected: PASS. Набор занимает примерно 90 с, из них около 65 с — честное ожидание в обязательном тесте №4.

- [ ] **Step 5: Commit**
  ```bash
  git add -A
  git commit -m "feat(watchdog): per-process timeout sweeper with SKIP LOCKED

  Every worker runs the sweeper; FOR UPDATE OF task SKIP LOCKED hands
  different overdue tasks to different processes, and fail_attempt re-checks
  state under the lock, so work is neither duplicated nor dependent on a
  leader that could die. Deadlines live in the DB and are compared with the
  DB clock. Rule 6: a late 'completed' after the task failed is recorded on
  the attempt but ignored - re-charging after a refund could hit an already
  spent balance; while the task still runs, a late result simply wins."
  ```

---

### Task 7: README и финальная проверка

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: всё выше. Новых интерфейсов нет.

- [ ] **Step 1: Написать `README.md`.** Разделы, обязательные по ТЗ, отмечены (*):
  1. (*) Запуск: `docker compose up --build`. Порты: api 8000, alpha 8001, beta 8002, db 5432. Пример `curl`: оплата (с вычислением подписи однострочником `python -c`), `/generate`, `/tasks/{id}`.
  2. (*) Тесты: `docker compose up -d --build --wait && docker compose --profile test run --rm tests pytest -v`. Набор идёт около 90 с из-за честного `PROVIDER_TIMEOUT=60`. Локальный вариант: `pip install -e '.[dev]'` и `pytest` с env на `localhost`.
  3. Миграции: как применяются (`migrate` в compose до старта `api`, `python -m app.migrate`) и как добавить следующую (`migrations/0003_*.sql` + `docker compose run --rm migrate`).
  4. (*) «Ровно один раз» для начисления и возврата: выжимка Д1 (уникальности, условный переход, триггер, инвариант и его проверка в тестах).
  5. (*) Падение процесса посреди обработки вебхука: выжимка из Д1 «Падение процесса» и инвариант «у running-задачи всегда есть попытка с дедлайном».
  6. (*) Правило 6: таблица и обоснование из Д5, включая гонку со сторожем.
  7. Конкурентность и сторож: Д2, Д4 в 5–8 строк.
  8. Допущения: таблица «Неоднозначности и допущения» из этого плана, включая неидемпотентный `/generate` и путь расширения.

- [ ] **Step 2: Проверить README на чистом окружении**
  Run: `docker compose down -v && docker compose up -d --build --wait && docker compose --profile test run --rm tests pytest -v`
  Expected: все тесты PASS. Команды из README выполняются дословно.

- [ ] **Step 3: Commit**
  ```bash
  git add README.md
  git commit -m "docs: README with run/test steps and the money guarantees

  The spec asks reviewers to judge exactly-once, crash-mid-webhook and rule 6
  from the README, so each is explained by the concrete DB mechanism (unique
  constraint, locked conditional transition, trigger) rather than prose."
  ```

---

## Self-Review

1. **Покрытие ТЗ.**
   - Домен и каталог: Task 1.
   - Фейки (5 режимов): Task 3.
   - `/generate` с 400/402/429: Task 4.
   - Вебхуки провайдеров и правила 1–4: Task 5.
   - Правило 5 и правило 6: Task 6.
   - Правило 7 (4 процесса, сторож без задвоения): compose в Task 2 и Task 6, тест `test_watchdog_does_not_double_advance`.
   - Правило 8: Task 1 (триггер) и фикстура `balance_invariant`.
   - Подпись: Task 2 и Task 5.
   - `/webhooks/payments`: Task 2. `GET /tasks`: Task 4. `GET /balance`: Task 2.
   - Миграции (применение и добавление): Task 1 и README.
   - `docker compose up`: Task 1–3, 6.
   - Обязательные тесты: №1 в Task 2, №2 и №3 в Task 5, №4 в Task 6.
   - Коммиты с «почему»: все задачи.
   - README (4 обязательных пункта): Task 7.

   Пробелов нет.
2. **Шаги.** Каждый шаг кода называет файл, сигнатуру и SQL-ядро там, где тест его не определяет (списание, возврат, выбор ноги, сторож, `fail_attempt`/`complete_attempt`). Тела функций, которые следуют из сигнатуры и тестов (подпись, раннер миграций, фейк), не расписаны. Строк вида «TBD» или «обработать ошибки» нет.
3. **Согласованность имён.** `fail_attempt(conn, attempt_id, error_type)`, `complete_attempt(conn, attempt_id, result_url)`, `refund_task(conn, task_id)`, `dispatch(pool, http, attempt_id)` везде совпадают. Статусы попытки (`dispatching|pending|completed|failed|timed_out|abandoned|late_completed`) и `error_type` (`technical|moderation|payload|submit_error|timeout`) одинаковы в Д3, Task 1 (CHECK) и тестах.
4. **Review Focus.** У каждой из 5 строк есть тест в задаче-владельце: RF1 → Task 5, RF2 → Task 6, RF3 → Task 2, RF4 → Task 5, RF5 → Task 6.
5. **Пропорции.** План заметно длиннее ТЗ (71 строка), потому что заказчик просил разобрать конкурентность и деньги. Кода в плане почти нет: только ключевые SQL-запросы и команды. Отклонение от порядка этапов, заданного в задаче: фейковые провайдеры (Task 3) идут раньше оркестрации, потому что без ответа 202 нельзя проверить отправку на ногу. Тесты не вынесены в отдельный этап: они пишутся внутри каждой задачи (TDD).
