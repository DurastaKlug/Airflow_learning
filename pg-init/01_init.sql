-- ==========================================
-- 1. Создание схем для разделения слоев
-- ==========================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS audit;

-- ==========================================
-- 2. Слой RAW (Сырые данные, "мусор", неструктурированное)
-- ==========================================
CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount NUMERIC(10,2),
    status VARCHAR(50),
    raw_json_data JSONB, -- Для тестов парсинга JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    registration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 3. Слой STAGING (Очищенные данные)
-- ==========================================
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    amount NUMERIC(10,2) CHECK (amount > 0), -- Добавим constraint для теста Data Quality
    status VARCHAR(50),
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id INT PRIMARY KEY,
    full_name VARCHAR(200),
    email VARCHAR(150),
    registration_date DATE,
    is_active BOOLEAN,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 4. Слой DWH (Витрины данных, Star Schema)
-- ==========================================

-- Таблица дат (обязательна для аналитики)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    quarter INT
);

-- Измерение: Клиенты (с поддержкой SCD Type 2 - медленно меняющиеся измерения)
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    full_name VARCHAR(200),
    email VARCHAR(150),
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Измерение: Статусы заказов
CREATE TABLE IF NOT EXISTS dwh.dim_order_statuses (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL
);

-- Факт: Заказы
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id INT PRIMARY KEY,
    customer_key INT REFERENCES dwh.dim_customers(customer_key),
    date_id DATE REFERENCES dwh.dim_date(date_id),
    status_id INT REFERENCES dwh.dim_order_statuses(status_id),
    amount NUMERIC(10,2),
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 5. Слой AUDIT (Логирование работы DAG'ов)
-- ==========================================
CREATE TABLE IF NOT EXISTS audit.dag_execution_log (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(50), -- SUCCESS, FAILED, SKIPPED
    rows_processed INT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 6. Наполнение тестовыми данными
-- ==========================================

-- Статусы
INSERT INTO dwh.dim_order_statuses (status_name) VALUES 
('Pending'), ('Processing'), ('Shipped'), ('Delivered'), ('Cancelled')
ON CONFLICT DO NOTHING;

-- Генерация дат на 1 год вперед (упрощенно)
INSERT INTO dwh.dim_date (date_id, year, month, day, day_of_week, quarter)
SELECT 
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d),
    EXTRACT(QUARTER FROM d)
FROM generate_series('2023-01-01'::date, '2024-12-31'::date, '1 day'::interval) d;

-- Сырые клиенты (хорошие данные)
INSERT INTO raw.raw_customers (first_name, last_name, email, registration_date) VALUES
('Иван', 'Иванов', 'ivanov@test.com', '2023-01-15'),
('Петр', 'Петров', 'petrov@test.com', '2023-02-20'),
('Мария', 'Сидорова', 'sidorova@test.com', '2023-03-10'),
('Олег', 'Смирнов', 'smirnov@test.com', '2023-05-05'),
('Елена', 'Кузнецова', 'kuznetsova@test.com', '2023-07-12');

-- Сырые заказы (включая "грязные" данные для тестов)
INSERT INTO raw.raw_orders (customer_id, order_date, amount, status, raw_json_data) VALUES
-- Нормальные заказы
(1, '2023-10-01', 150.00, 'Delivered', '{"source": "web", "promo": null}'),
(2, '2023-10-05', 250.50, 'Shipped', '{"source": "mobile", "promo": "SUMMER20"}'),
(3, '2023-11-12', 99.99, 'Processing', '{"source": "web", "promo": null}'),
(1, '2023-11-20', 300.00, 'Pending', '{"source": "api", "promo": "VIP10"}'),
-- "Грязные" данные (отрицательная сумма - упадет constraint в staging)
(2, '2023-12-01', -50.00, 'Cancelled', '{"source": "web", "error": "refund_failed"}'),
-- "Грязные" данные (NULL в обязательных полях)
(NULL, '2023-12-05', 100.00, 'Pending', '{"source": "mobile"}'),
-- Заказ из будущего (для тестов инкрементальной загрузки)
(4, CURRENT_DATE, 500.00, 'Pending', '{"source": "web"}');

-- Обновим updated_at у одного клиента, чтобы потом тестировать инкрементальную загрузку / SCD2
UPDATE raw.raw_customers SET email = 'ivanov_new@test.com', updated_at = CURRENT_TIMESTAMP WHERE customer_id = 1;