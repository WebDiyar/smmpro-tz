-- Журнал (ledger) - единственный способ изменить баланс: триггер ledger_apply
-- прибавляет amount к users.balance. UPDATE/DELETE журнала запрещены.

CREATE TABLE users (
    id         bigint PRIMARY KEY,
    balance    bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),  -- последняя защита от минуса
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE models (
    slug  text PRIMARY KEY,
    price int NOT NULL CHECK (price > 0)
);

CREATE TABLE legs (
    id                 serial PRIMARY KEY,
    model_slug         text NOT NULL REFERENCES models (slug),
    provider           text NOT NULL,
    priority           int  NOT NULL CHECK (priority > 0),  -- меньше = пробуется раньше
    max_ref_images     int  NOT NULL CHECK (max_ref_images >= 0),
    skip_on_moderation boolean NOT NULL DEFAULT false,
    UNIQUE (model_slug, priority)
);

CREATE TABLE tasks (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id            bigint NOT NULL REFERENCES users (id),
    model_slug         text   NOT NULL REFERENCES models (slug),
    prompt             text   NOT NULL CHECK (prompt <> ''),
    ref_images         text[] NOT NULL DEFAULT '{}',
    status             text   NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    result_url         text,
    current_attempt_id uuid,  -- FK добавлен ниже: attempts ссылается на tasks
    created_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz
);

CREATE TABLE attempts (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id     uuid NOT NULL REFERENCES tasks (id),
    n           int  NOT NULL CHECK (n > 0),
    leg_id      int  NOT NULL REFERENCES legs (id),
    provider    text NOT NULL,  -- копия legs.provider для удобства запросов
    status      text NOT NULL DEFAULT 'dispatching' CHECK (status IN (
                    'dispatching', 'pending', 'completed', 'failed', 'timed_out', 'abandoned', 'late_completed')),
    error_type  text CHECK (error_type IN ('technical', 'moderation', 'payload', 'submit_error', 'timeout')),
    external_id text,
    result_url  text,
    deadline_at timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (task_id, n)
);

ALTER TABLE tasks ADD FOREIGN KEY (current_attempt_id) REFERENCES attempts (id);

CREATE INDEX tasks_running_by_user ON tasks (user_id) WHERE status = 'running';
CREATE INDEX attempts_live_deadline ON attempts (deadline_at) WHERE status IN ('dispatching', 'pending');
-- не больше одной «живой» попытки у задачи
CREATE UNIQUE INDEX attempts_one_live_per_task ON attempts (task_id) WHERE status IN ('dispatching', 'pending');

CREATE TABLE ledger (
    id         bigserial PRIMARY KEY,
    user_id    bigint NOT NULL REFERENCES users (id),
    amount     bigint NOT NULL,
    reason     text   NOT NULL CHECK (reason IN ('payment', 'charge', 'refund')),
    payment_id text,
    task_id    uuid REFERENCES tasks (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((reason = 'payment' AND amount > 0) OR (reason = 'charge' AND amount < 0) OR (reason = 'refund' AND amount > 0)),
    CHECK ((reason = 'payment') = (payment_id IS NOT NULL)),
    CHECK ((reason <> 'payment') = (task_id IS NOT NULL)),
    UNIQUE (payment_id),       -- одна оплата - одно начисление (NULL-ы различны)
    UNIQUE (task_id, reason)   -- у задачи одно списание и не больше одного возврата
);

CREATE FUNCTION ledger_apply() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE users SET balance = balance + NEW.amount WHERE id = NEW.user_id;
    RETURN NULL;
END $$;

CREATE TRIGGER ledger_apply AFTER INSERT ON ledger
    FOR EACH ROW EXECUTE FUNCTION ledger_apply();

CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger is append-only';
END $$;

CREATE TRIGGER ledger_append_only BEFORE UPDATE OR DELETE ON ledger
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
