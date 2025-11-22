# Лабораторна робота №4.

## Потокова обробка енергетичних даних з транзакційною семантикою в Apache Kafka Streams

**Варіант 8:** Розподілені енергетичні ресурси (DER/VPP)  
**Підваріант А:** Tumbling Windows (5 хв) + Event Sourcing  
**Дата виконання:** 21-11-2025

## Мета

Розробити систему потокової обробки енергетичних даних з використанням
Apache Kafka Streams, що забезпечує транзакційну семантику exactly-once та
інтеграцію з Apache Cassandra для збереження результатів обробки.

## Завдання

1. Створити Kafka Streams додаток для обробки енергетичних телеметричних даних
2. Імплементувати stateful операції: агрегації, windowing, join операції
3. Налаштувати exactly-once семантику для гарантії транзакційної консистентності
4. Розробити механізм виявлення аномалій в реальному часі
5. Інтегрувати з Cassandra для збереження агрегованих даних та виявлених аномалій
6. Протестувати відмовостійкість системи при збоях
7. Порівняти продуктивність з at-least-once та at-most-once семантиками
8. Розробити dashboard для візуалізації результатів обробки

## Особливості варіанту 8

### Об'єкт дослідження

- **Кількість активів:** 1000 DER (solar, wind, battery, diesel)
- **Частота оновлення:** Кожні 60 секунд
- **Throughput:** ~16.7 msg/sec
- **Дані:** asset_id, asset_type, power_output, available_capacity, soc (для батарей), fuel_level (для генераторів), forecasted_output

### Підваріант А: Tumbling Windows + Event Sourcing

- 5-хвилинні вікна для агрегації VPP portfolio
- Розрахунок: total_capacity, available_capacity, dispatch_margin
- Event Store для energy market bidding audit
- Replay для аналізу dispatch decisions
- Події: ASSET_DISPATCHED, BID_SUBMITTED, CAPACITY_CHANGED
- State для forecasting accuracy tracking
- API для market operator queries

## Структура проекту

```
.
├── config.py                 # Конфігурація системи
├── models.py                 # Моделі даних (Pydantic)
├── producer.py               # Producer для генерації тестових даних
├── streams_app.py            # Kafka Streams додаток
├── cassandra_integration.py  # Інтеграція з Cassandra
├── cassandra_sink.py         # Sink для запису в Cassandra
├── api.py                    # REST API для market operator
├── dashboard.py              # Dashboard для візуалізації
├── requirements.txt          # Залежності Python
├── tests/                    # Тести
│   ├── test_resilience.py
│   └── test_semantics_comparison.py
└── README.md                 # Документація
```

## Встановлення та налаштування

### Вимоги до системи

1. **Python 3.10+**
2. **Apache Kafka** (локально або Docker)
3. **Apache Cassandra** (локально або Docker)

### Встановлення залежностей

```bash
# Створення віртуального середовища
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# або
venv\Scripts\activate  # Windows

# Встановлення залежностей
pip install -r requirements.txt
```

### Налаштування Kafka, Zookeeper, Cassandra

```bash
# Запуск Kafka через Docker
docker-compose up -d
```

## Запуск системи

### 1. Запуск Producer (генерація тестових даних)

```bash
python producer.py
```

Producer генерує телеметричні дані для 1000 активів кожні 60 секунд та відправляє їх в Kafka topic `der-raw-data`.

### 2. Запуск Kafka Streams додатку

```bash
python streams_app.py
```

Streams додаток:

- Читає дані з `der-raw-data`
- Агрегує дані в 5-хвилинні tumbling windows
- Виявляє аномалії
- Створює події для Event Sourcing
- Записує результати в topics: `der-events`, `der-aggregates`, `der-anomalies`

### 3. Запуск Cassandra Sink

```bash
python cassandra_sink.py
```

Sink читає дані з Kafka topics та записує їх в Cassandra:

- `der_event_log` - Event Store
- `portfolio_state` - Агреговані стани portfolio
- `der_anomalies` - Виявлені аномалії

### 4. Запуск API сервера

```bash
python api.py
```

API доступне на `http://localhost:5000` з endpoints:

- `GET /api/portfolio/latest` - Останній стан portfolio
- `GET /api/portfolio/window?window_start=...` - Стан за вікно
- `GET /api/events/asset/<asset_id>` - Події по активу
- `GET /api/events/type/<event_type>` - Події по типу
- `GET /api/anomalies/asset/<asset_id>` - Аномалії по активу
- `POST /api/replay/<asset_id>` - Replay подій для аналізу

### 5. Запуск Dashboard

```bash
python dashboard.py
```

Dashboard доступний на `http://localhost:8050` з візуалізацією:

- Стан portfolio (total_capacity, available_capacity, dispatch_margin)
- Breakdown по типах активів
- Виявлені аномалії
- Останні події

## Архітектура системи

```
┌─────────────┐
│  Producer   │ ──> der-raw-data (Kafka)
│ (1000 DER)  │     (генерація кожні 60 сек)
└─────────────┘
       │
       v
┌─────────────┐
│Streams App  │ ──> der-events (Event Sourcing)
│             │     der-aggregates (5-хв вікна)
│             │     der-anomalies (виявлення)
└─────────────┘
       │
       v
┌─────────────┐
│Cassandra    │ ──> der_event_log
│   Sink      │     portfolio_state
│             │     der_anomalies
└─────────────┘
       │
       v
┌─────────────┐
│     API     │ <── Dashboard (http://localhost:8050)
│ (REST)      │     (http://localhost:5000)
└─────────────┘
```

### 2. Основні компоненти

#### Producer (`producer.py`)

- Генерує телеметричні дані для 1000 активів
- Частота оновлення: кожні 60 секунд
- Throughput: ~16.7 msg/sec
- Типи активів: solar (250), wind (250), battery (250), diesel (250)

#### Kafka Streams App (`streams_app.py`)

- **Tumbling Windows**: 5-хвилинні вікна для агрегації
- **Event Sourcing**: Створення подій (ASSET_DISPATCHED, BID_SUBMITTED, CAPACITY_CHANGED)
- **Anomaly Detection**: Виявлення аномалій в реальному часі
- **Exactly-once семантика**: Гарантія транзакційної консистентності

#### Cassandra Integration (`cassandra_integration.py`)

- Зберігання подій в `der_event_log`
- Зберігання агрегованих станів в `portfolio_state`
- Зберігання аномалій в `der_anomalies`

#### REST API (`api.py`)

- Endpoints для отримання portfolio state
- Endpoints для запиту подій та аномалій
- Replay функціональність для аналізу

#### Dashboard (`dashboard.py`)

- Візуалізація portfolio state
- Відображення аномалій
- Список останніх подій

### 3. Реалізовані функції

- **Tumbling Windows (5 хв)** - агрегація даних за фіксовані 5-хвилинні вікна
- **Event Sourcing** - збереження всіх подій для audit trail та replay
- **Anomaly Detection** - виявлення аномалій на основі статистичних методів
- **Exactly-once семантика** - гарантія обробки кожного повідомлення рівно один раз
- **State Management** - підтримка стану для forecasting accuracy tracking
- **REST API** - API для market operator queries
- **Dashboard** - веб-інтерфейс для візуалізації

### Event Sourcing

Система реалізує Event Sourcing з подіями:

- **ASSET_DISPATCHED** - Диспетчеризація активу
- **BID_SUBMITTED** - Подача заявки на ринок
- **CAPACITY_CHANGED** - Зміна доступної потужності

### Exactly-once vs інші семантики

- **At-most-once:** Найшвидша, але може втратити дані
- **At-least-once:** Середня швидкість, можливі дублікати
- **Exactly-once:** На 20-30% повільніша, але гарантує консистентність

## АНАЛІЗ ОТРИМАНИХ результатів

### 1. Приклад відповіді API Portfolio State (Latest):

```json
{
  "data": {
    "asset_count": 1000,
    "asset_type_breakdown": {
      "battery": 250,
      "diesel": 250,
      "solar": 250,
      "wind": 250
    },
    "available_capacity": 810668.25,
    "dispatch_margin": 355074.75,
    "total_capacity": 861060.38,
    "total_output": 455593.5,
    "window_end": "2025-11-22T00:05:00",
    "window_start": "2025-11-22T00:00:00"
  },
  "success": true
}
```

#### Пояснення полів:

- **`window_start` / `window_end`**: Часове вікно агрегації (5 хвилин)
  - В даному випадку: з 00:00:00 до 00:05:00
- **`asset_count`**: Загальна кількість активів в портфелі
  - Значення: 1000 активів
- **`asset_type_breakdown`**: Розподіл активів по типах
  - `battery`: 250 батарей
  - `diesel`: 250 дизельних генераторів
  - `solar`: 250 сонячних панелей
  - `wind`: 250 вітрових турбін
- **`total_capacity`**: Загальна максимальна потужність всіх активів (кВт)
  - Значення: 861,060.38 кВт
  - Це сума максимальних потужностей всіх 1000 активів
- **`available_capacity`**: Доступна потужність для використання (кВт)
  - Значення: 810,668.25 кВт
  - Враховує:
    - SOC (State of Charge) для батарей
    - Рівень палива для дизельних генераторів
    - Поточні умови для сонячних та вітрових установок
- **`total_output`**: Поточна вихідна потужність всіх активів (кВт)
  - Значення: 455,593.5 кВт
  - Фактична потужність, яку виробляють активи зараз
- **`dispatch_margin`**: Маржа для диспетчеризації (кВт)
  - Значення: 355,074.75 кВт
  - Розрахунок: `available_capacity - total_output`
  - Показує, скільки додаткової потужності можна використати для диспетчеризації

#### Що це означає:

1. **Потенціал системи**: Загальна потужність 861 МВт
2. **Доступність**: 810 МВт доступно для використання (94% від загальної)
3. **Поточне виробництво**: 455 МВт активно виробляється (56% від доступної)
4. **Резерв**: 355 МВт доступно для диспетчеризації (можна збільшити виробництво)

### 2. Recent Events

#### Приклад подій:

```
BID_SUBMITTED - portfolio - 2025-11-22 00:10:14
BID_SUBMITTED - portfolio - 2025-11-22 00:05:13
BID_SUBMITTED - portfolio - 2025-11-22 00:00:13
ASSET_DISPATCHED - diesel_0999 - 2025-11-21 23:49:20
ASSET_DISPATCHED - diesel_0991 - 2025-11-21 23:49:20
```

#### Типи подій:

1. **`BID_SUBMITTED`**: Подача заявки на енергетичний ринок
   - Створюється кожні 5 хвилин при закритті вікна
   - `asset_id`: "portfolio" (заявка від всього портфелю)
   - Використовується для energy market bidding audit
2. **`ASSET_DISPATCHED`**: Диспетчеризація конкретного активу
   - Створюється коли актив починає/припиняє виробництво
   - `asset_id`: ID конкретного активу (наприклад, "diesel_0999")
   - Вказує на активне використання активу
3. **`CAPACITY_CHANGED`**: Зміна доступної потужності
   - Створюється при зміні доступної потужності більше ніж на 10%
   - Може вказувати на:
     - Зміну SOC батареї
     - Зменшення рівня палива
     - Зміну умов для ВДЕ

#### Event Sourcing переваги:

- **Audit Trail**: Повна історія всіх подій
- **Replay**: Можливість відтворити стан системи на будь-який момент
- **Аналіз**: Можливість проаналізувати dispatch decisions

### 3. Anomalies (Аномалії)

#### Статистика:

- **Total Anomalies**: 13
- **Types**: forecast_accuracy (100%)

**Примітка**: 100% аномалій типу `forecast_accuracy` є нормальним для тестових даних, оскільки:

1. Producer генерує дані з випадковими змінами, що створює природні розбіжності між прогнозом та фактом
2. Поріг виявлення (30%) спрацьовує часто через волатильність тестових даних
3. Інші типи аномалій (low_soc, low_fuel, negative_capacity) не виникають, бо:
   - Батареї не досягають критично низького рівня заряду (<5%)
   - Генератори не досягають критично низького рівня палива (<10%)
   - Валідація моделей запобігає негативним значенням

В production системі розподіл типів аномалій буде більш рівномірним.

#### Приклад аномалії:

```json
{
  "anomaly_id": "28ef5668-80b2-43b4-a6d4-8d453cc6d07b",
  "anomaly_type": "forecast_accuracy",
  "asset_id": "solar_0001",
  "description": "Велика розбіжність між прогнозом та фактичним значенням: 88.8%",
  "expected_range": [126.567, 235.053],
  "severity": "high",
  "timestamp": "2025-11-21T23:59:13.049000",
  "value": 20.34,
  "z_score": null
}
```

#### Пояснення:

- **`anomaly_type`**: Тип аномалії
  - `forecast_accuracy`: Розбіжність між прогнозом та фактичним значенням
- **`asset_id`**: ID активу, де виявлено аномалію
  - В даному випадку: "solar_0001" (сонячна панель)
- **`severity`**: Серйозність аномалії
  - `low`, `medium`, `high`, `critical`
  - В даному випадку: `high`
- **`description`**: Опис проблеми
  - "Велика розбіжність між прогнозом та фактичним значенням: 88.8%"
  - Це означає, що прогнозована потужність відрізняється від фактичної на 88.8%
- **`value`**: Фактичне значення
  - 20.34 кВт (фактична вихідна потужність)
- **`expected_range`**: Очікуваний діапазон значень
  - [126.567, 235.053] кВт
  - Фактичне значення (20.34 кВт) значно нижче очікуваного мінімуму
- **`timestamp`**: Час виявлення аномалії

## ВИСНОВКИ

### Було розроблено та впроваджено такі ключові можливості системи:

1. Система успішно обробляє 1000 активів в реальному часі
2. Агрегація в 5-хвилинні вікна працює коректно
3. Event Sourcing забезпечує повну історію подій
4. Виявлення аномалій працює та виявляє проблеми з прогнозуванням
5. Exactly-once семантика забезпечує консистентність даних

### Області для покращення:

1. **Аномалії**: Відрегулювати налаштування для виялвення аномалій (зараз переважно одна)
2. **Прогнозування**: Деякі активи мають високу розбіжність між прогнозом та фактом
3. **Моніторинг**: Можна додати більше метрик для детального аналізу
4. **Алерти**: Автоматичні сповіщення при критичних аномаліях
