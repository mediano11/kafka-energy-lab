-- Лабораторна робота 3: Оптимізація схем даних в Apache Cassandra
-- Варіант 8: Розподілені енергетичні ресурси (DER)
-- Підваріант А: орієнтація на зберігання

-- Створення keyspace
CREATE KEYSPACE IF NOT EXISTS der_energy_lab 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

USE der_energy_lab;

-- СХЕМА 1: Simple Wide Row (Baseline - антипатерн)
-- Демонстрація проблеми з нескінченним зростанням партицій

CREATE TABLE IF NOT EXISTS der_simple (
    device_id TEXT,
    timestamp TIMESTAMP,
    net_power DOUBLE,           -- чиста потужність (кВт)
    battery_level DOUBLE,       -- рівень заряду акумулятора (%)
    installation_type TEXT,     -- тип установки (residential, commercial, industrial)
    voltage DOUBLE,             -- напруга (В)
    current DOUBLE,             -- струм (А)
    temperature DOUBLE,         -- температура (°C)
    PRIMARY KEY (device_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- СХЕМА 2: Hourly Bucketing (Рекомендована)
-- Оптимальна схема для систем з 1 записом/хвилину

CREATE TABLE IF NOT EXISTS der_hourly (
    device_id TEXT,
    bucket_hour TIMESTAMP,      -- час початку години (округлений)
    timestamp TIMESTAMP,
    net_power DOUBLE,
    battery_level DOUBLE,
    installation_type TEXT,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    PRIMARY KEY ((device_id, bucket_hour), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- СХЕМА 3: Daily Bucketing + Pre-aggregation
-- Схема з денним групуванням та попередньою агрегацією

-- Таблиця сирих даних (денне bucketing)
CREATE TABLE IF NOT EXISTS der_daily_raw (
    device_id TEXT,
    bucket_date DATE,           -- дата (без часу)
    timestamp TIMESTAMP,
    net_power DOUBLE,
    battery_level DOUBLE,
    installation_type TEXT,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    PRIMARY KEY ((device_id, bucket_date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Таблиця агрегованих даних (погодинні агрегати)
CREATE TABLE IF NOT EXISTS der_daily_aggregates (
    device_id TEXT,
    hour TIMESTAMP,             -- час початку години
    installation_type TEXT,
    avg_net_power DOUBLE,       -- середня чиста потужність
    max_net_power DOUBLE,       -- максимальна потужність
    min_net_power DOUBLE,       -- мінімальна потужність
    avg_battery_level DOUBLE,   -- середній рівень заряду
    max_battery_level DOUBLE,   -- максимальний рівень заряду
    min_battery_level DOUBLE,   -- мінімальний рівень заряду
    avg_voltage DOUBLE,         -- середня напруга
    avg_current DOUBLE,         -- середній струм
    avg_temperature DOUBLE,     -- середня температура
    sample_count INT,           -- кількість зразків
    PRIMARY KEY (device_id, hour)
) WITH CLUSTERING ORDER BY (hour DESC);

-- MATERIALIZED VIEWS для оптимізації запитів


-- MV1: High Power (потужність > 2.5 кВт) - для виявлення піків
-- Примітка: Materialized Views з WHERE умовами не підтримуються в цій версії Cassandra
-- Використовуємо окремі таблиці замість MV

CREATE TABLE IF NOT EXISTS der_high_power (
    device_id TEXT,
    bucket_hour TIMESTAMP,
    timestamp TIMESTAMP,
    net_power DOUBLE,
    battery_level DOUBLE,
    installation_type TEXT,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    PRIMARY KEY ((device_id, bucket_hour), net_power, timestamp)
) WITH CLUSTERING ORDER BY (net_power DESC, timestamp DESC);

-- MV2: Low Battery (рівень заряду < 20%) - для контролю акумуляторів
CREATE TABLE IF NOT EXISTS der_low_battery (
    device_id TEXT,
    bucket_hour TIMESTAMP,
    timestamp TIMESTAMP,
    net_power DOUBLE,
    battery_level DOUBLE,
    installation_type TEXT,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    PRIMARY KEY ((device_id, bucket_hour), battery_level, timestamp)
) WITH CLUSTERING ORDER BY (battery_level ASC, timestamp DESC);

-- MV3: Industrial Installations - для фільтрації промислових установок
CREATE TABLE IF NOT EXISTS der_industrial (
    device_id TEXT,
    bucket_hour TIMESTAMP,
    timestamp TIMESTAMP,
    net_power DOUBLE,
    battery_level DOUBLE,
    installation_type TEXT,
    voltage DOUBLE,
    current DOUBLE,
    temperature DOUBLE,
    PRIMARY KEY ((device_id, bucket_hour), timestamp)
);

-- ІНДЕКСИ для додаткової оптимізації


-- Індекс для фільтрації за типом установки
CREATE INDEX IF NOT EXISTS idx_installation_type ON der_hourly (installation_type);


-- НАЛАШТУВАННЯ TTL та СТИСНЕННЯ

-- Оновлення таблиць з TTL та стисненням
ALTER TABLE der_simple WITH 
    compression = {'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 864000;  -- 10 днів

ALTER TABLE der_hourly WITH 
    compression = {'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 864000;

ALTER TABLE der_daily_raw WITH 
    compression = {'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 864000;

ALTER TABLE der_daily_aggregates WITH 
    compression = {'class': 'LZ4Compressor'}
    AND gc_grace_seconds = 864000;

-- ПОЛІТИКА TTL для автоматичного видалення старих даних


-- Для сирих даних: зберігати 1 рік
-- Для агрегованих даних: зберігати 5 років