CREATE TABLE IF NOT EXISTS balance (
    account_id      INT PRIMARY KEY,
    current_balance NUMERIC(10, 2) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inbox_events (
    id             BIGSERIAL PRIMARY KEY,
    account_id     INT      NOT NULL,
    transaction_id BIGINT   NOT NULL,
    payload        TEXT     NOT NULL,
    processed      BOOLEAN  NOT NULL DEFAULT false,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'inbox_events_transaction_id_key'
    ) THEN
        ALTER TABLE inbox_events
            ADD CONSTRAINT inbox_events_transaction_id_key
            UNIQUE (transaction_id);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION apply_inbox_event() RETURNS trigger AS $$
DECLARE
    v_amount NUMERIC(10, 2);
    v_payload_json JSONB;
BEGIN
    v_payload_json := NEW.payload::jsonb;
    v_amount   := (v_payload_json->>'amount')::numeric;

    INSERT INTO balance (account_id, current_balance, updated_at)
    VALUES (NEW.account_id, v_amount, NOW())
    ON CONFLICT (account_id)
    DO UPDATE
      SET current_balance = balance.current_balance + EXCLUDED.current_balance,
          updated_at = NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_apply_inbox_event'
    ) THEN
        DROP TRIGGER trg_apply_inbox_event ON inbox_events;
    END IF;

    CREATE TRIGGER trg_apply_inbox_event
    AFTER INSERT ON inbox_events
    FOR EACH ROW
    EXECUTE FUNCTION apply_inbox_event();
END;
$$;
