use lazy_static::lazy_static;
#[derive(Clone)]
pub struct Migration {
    pub from: i32,
    pub to: i32,
    pub script: &'static str,
}

lazy_static! {
    pub static ref MIGRATIONS: Vec<Migration> = vec![
        Migration {
            from: 0,
            to: 1,
            script: r#"
                CREATE TABLE IF NOT EXISTS configuration (
                    id SERIAL PRIMARY KEY,
                    h4 INTEGER NOT NULL,
                    percent FLOAT4 NOT NULL,
                    is_activated BOOL
                );
                INSERT INTO configuration (h4, percent, is_activated) VALUES (1000, 90.0, true);
                CREATE TABLE IF NOT EXISTS date_calculate_logs (
                    id SERIAL PRIMARY KEY,
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP NOT NULL,
                    configuration_id INTEGER NOT NULL REFERENCES configuration(id) ON DELETE SET NULL
                );
                DO $$
                BEGIN
                    CREATE TYPE candle_timeframe AS ENUM ('h1', 'h4');
                EXCEPTION
                    WHEN duplicate_object THEN null;
                END $$;

                CREATE OR REPLACE FUNCTION add_date_going(p_day TIMESTAMP)
                RETURNS VOID AS $$
                DECLARE
                    last_record RECORD;
                BEGIN
                    -- Получаем последнюю запись
                    SELECT * INTO last_record FROM date_going
                    ORDER BY id DESC LIMIT 1
                    FOR UPDATE;

                    -- Если запись существует и count не равен 20, увеличиваем count и обновляем дату

                    UPDATE date_going
                    SET count = last_record.count + 1, day = p_day
                    WHERE id = last_record.id;

                END;
                $$ LANGUAGE plpgsql SECURITY DEFINER;
                CREATE TABLE IF NOT EXISTS date_going (
                    id SERIAL PRIMARY KEY,
                    day TIMESTAMP NOT NULL,
                    count INTEGER NOT NULL,
                    log_id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pairs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    is_synthetic BOOLEAN NOT NULL,
                    UNIQUE(name)
                );
                CREATE TABLE pair_requests (
                    id SERIAL PRIMARY KEY,
                    pair_id INTEGER NOT NULL REFERENCES pairs(id) ON DELETE CASCADE,
                    data_configuration_id INTEGER NOT NULL,
                    username VARCHAR NOT NULL,
                    UNIQUE (pair_id, data_configuration_id, username)
                );

                CREATE OR REPLACE FUNCTION request_pair(p_pair_id INTEGER, p_data_configuration_id INTEGER, user_name VARCHAR)
                RETURNS VOID AS $$BEGIN
                    -- Add new user request to pair_requests table with the new schema
                    INSERT INTO pair_requests (pair_id, data_configuration_id, username)
                    VALUES (p_pair_id, p_data_configuration_id, user_name)
                    ON CONFLICT (pair_id, data_configuration_id, username) DO NOTHING;


                END;$$ LANGUAGE plpgsql SECURITY DEFINER;


                CREATE OR REPLACE FUNCTION cancel_pair_request(p_pair_id INTEGER, p_data_configuration_id INTEGER, user_name VARCHAR)
                RETURNS VOID AS $$BEGIN
                    -- Remove user request from pair_requests table
                    DELETE FROM pair_requests
                    WHERE pair_id = p_pair_id
                      AND data_configuration_id = p_data_configuration_id
                      AND username = user_name;


                END;$$ LANGUAGE plpgsql SECURITY DEFINER;

                    CREATE TABLE IF NOT EXISTS stationarity_pairs (
                        id SERIAL PRIMARY KEY,
                        pair_id INTEGER NOT NULL REFERENCES pairs(id) ON DELETE CASCADE,
                        log_id INTEGER NOT NULL REFERENCES date_calculate_logs(id) ON DELETE SET NULL,
                        stationarity FLOAT4 NOT NULL
                    );

                CREATE TABLE IF NOT EXISTS pairs_info (
                    id SERIAL PRIMARY KEY,
                    candles_h4 FLOAT4[] NOT NULL,
                    zscores_h4 FLOAT4[] NOT NULL,
                    dates_minutes TIMESTAMP[] NOT NULL,
                    dates_h4 TIMESTAMP[] NOT NULL,
                    candles_minutes FLOAT4[] NOT NULL,
                    zscores_minutes FLOAT4[] NOT NULL,
                    pair_id INTEGER NOT NULL REFERENCES pairs(id) ON DELETE CASCADE,
                    candles_timeframe candle_timeframe NOT NULL,
                    configuration_id INTEGER NOT NULL REFERENCES configuration(id) ON DELETE SET NULL,
                    UNIQUE (pair_id, candles_timeframe, configuration_id)
                );

                -- Create trading_user if not exists
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'trading_user') THEN
                        CREATE USER trading_user WITH PASSWORD '123';
                    END IF;
                END
                $$;

                -- Grant read access to specified tables
                GRANT SELECT ON pairs_info TO trading_user;
                GRANT SELECT ON stationarity_pairs TO trading_user;
                GRANT SELECT ON pair_requests TO trading_user;
                GRANT SELECT ON pairs TO trading_user;
                GRANT SELECT ON date_going TO trading_user;
                GRANT SELECT ON date_calculate_logs TO trading_user;
                GRANT SELECT ON configuration TO trading_user;
                GRANT EXECUTE ON FUNCTION request_pair(INTEGER,INTEGER,VARCHAR) TO trading_user;
                GRANT EXECUTE ON FUNCTION cancel_pair_request(INTEGER,INTEGER,VARCHAR) TO trading_user;
                GRANT EXECUTE ON FUNCTION add_date_going(TIMESTAMP) TO trading_user;
            "#,
        },


    ];
}