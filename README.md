# VPP Management System - Варіант 8

Система моніторингу та управління Virtual Power Plant (VPP) з 1000 розподілених енергетичних ресурсів (DER).

## Опис

Цей проект реалізує систему моніторингу для VPP, яка управляє:

- 350 сонячних панелей (solar)
- 250 вітрових турбін (wind)
- 250 батарейних систем (battery)
- 150 навантажень (load)

Дані генеруються кожну хвилину та відправляються в Kafka, з одночасним експортом метрик в Prometheus.

## Вимоги

- Python 3.8+
- Apache Kafka (запущений на localhost:9092)
- Prometheus (встановлений в ~)
- Grafana (встановлений в ~)

## Швидкий старт

### Автоматичне встановлення та запуск

1. Надайте права виконання скриптам:

```bash
chmod +x *.sh
```

2. Встановіть та налаштуйте всі сервіси:

```bash
./install_and_setup.sh
```

Цей скрипт:

- Встановить Python залежності
- Завантажить та встановить Kafka (якщо не встановлено)
- Завантажить та встановить Prometheus (якщо не встановлено)
- Скопіює конфігурації

3. Запустіть всі сервіси:

```bash
./start_services.sh
```

Цей скрипт запустить:

- Zookeeper
- Kafka
- Prometheus
- Створить необхідні Kafka топики

4. Запустіть DER Producer:

```bash
./start_producer.sh
```

Або вручну:

```bash
python3 der_producer.py
```

### Ручне встановлення

Якщо ви хочете встановити все вручну:

1. Встановіть Python залежності:

```bash
pip install -r requirements.txt --break-system-packages
```

2. Встановіть Kafka (якщо не встановлено):

```bash
cd ~
wget https://downloads.apache.org/kafka/2.13-3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
export KAFKA_HOME=~/kafka_2.13-3.6.1
export PATH="$KAFKA_HOME/bin:$PATH"
```

3. Встановіть Prometheus (якщо не встановлено):

```bash
cd ~
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar -xzf prometheus-2.45.0.linux-amd64.tar.gz
export PROMETHEUS_HOME=~/prometheus-2.45.0.linux-amd64
```

4. Скопіюйте конфігурації:

```bash
cp prometheus.yml "$PROMETHEUS_HOME/"
cp alerts.yml "$PROMETHEUS_HOME/"
```

5. Запустіть Zookeeper та Kafka:

```bash
# В одному терміналі
cd "$KAFKA_HOME"
bin/zookeeper-server-start.sh config/zookeeper.properties

# В іншому терміналі
cd "$KAFKA_HOME"
bin/kafka-server-start.sh config/server.properties
```

6. Створіть Kafka топики:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" --create --topic der_telemetry --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --create --topic vpp_aggregated --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

7. Запустіть Prometheus:

```bash
cd "$PROMETHEUS_HOME"
./prometheus --config.file=prometheus.yml
```

## Корисні скрипти

- `install_and_setup.sh` - автоматичне встановлення всіх залежностей
- `start_services.sh` - запуск всіх сервісів (Zookeeper, Kafka, Prometheus)
- `start_producer.sh` - запуск DER Producer
- `stop_services.sh` - зупинка всіх сервісів
- `check_system.sh` - перевірка стану системи
- `setup_kafka_topics.sh` - створення Kafka топиків

## Перевірка стану

Після запуску всіх сервісів перевірте їх стан:

```bash
./check_system.sh
```

Або вручну:

- Prometheus: http://localhost:9090
- Prometheus метрики: http://localhost:8000/metrics
- Grafana: http://localhost:3000

### 3. Налаштування Grafana

1. Відкрийте Grafana: http://localhost:3000
2. Додайте Prometheus як Data Source:

   - Configuration → Data Sources → Add data source
   - Вибрати Prometheus
   - URL: http://localhost:9090
   - Save & Test

3. Імпортуйте Dashboard:
   - - → Import
   - Завантажити файл `vpp_dashboard.json`
   - Або скопіювати JSON вручну

## Структура метрик

### Базові метрики DER

- `der_net_power_kw` - Чиста потужність DER (кВт)
- `der_battery_soc_percent` - State of Charge батареї (%)
- `der_available_flexibility_kw` - Доступна гнучкість (кВт)
- `der_status` - Статус (1=online, 0=offline, -1=maintenance)

### VPP агреговані метрики

- `vpp_aggregated_generation_kw` - Агрегована генерація (кВт)
- `vpp_aggregated_consumption_kw` - Агреговане споживання (кВт)
- `vpp_net_power_kw` - Чиста потужність VPP (кВт)
- `vpp_total_flexibility_kw` - Загальна доступна гнучкість (кВт)
- `vpp_battery_fleet_soc_percent` - Розподіл SOC батарейного флоту (histogram)

### Dispatch та Compliance

- `vpp_setpoint_kw` - Setpoint для VPP (кВт)
- `vpp_actual_power_kw` - Фактична потужність VPP (кВт)
- `vpp_setpoint_deviation_kw` - Відхилення від setpoint (кВт)
- `vpp_compliance_percent` - Compliance з setpoint (%)
- `vpp_dispatch_instruction_compliance` - Compliance з dispatch instruction

### Аналітичні метрики

- `vpp_efficiency_percent` - Ефективність VPP (%)
- `vpp_response_time_seconds` - Response time (секунди)
- `vpp_flexibility_utilization_percent` - Utilization flexibility (%)
- `vpp_market_participation_success` - Market participation success

## Алерти

Система налаштована з наступними алертами:

1. **Недостатня гнучкість**: <100 кВт протягом 2 хвилин
2. **Відхилення від setpoint**: >50 кВт протягом 5 хвилин
3. **Багато DER offline**: >10% протягом 3 хвилин
4. **Не виконано dispatch instruction**: протягом 2 хвилин
5. **Низька ефективність VPP**: <60% протягом 10 хвилин
6. **Високий response time**: >300 секунд протягом 5 хвилин
7. **Низька utilization flexibility**: <30% протягом 15 хвилин
8. **Невдала market participation**: протягом 10 хвилин

Всі алерти налаштовані в файлі `alerts.yml` та автоматично завантажуються Prometheus.

## Dashboard панелі

Grafana dashboard містить:

1. **Агрегована потужність VPP** (Time Series) - показує генерацію, споживання, чисту потужність та setpoint
2. **Доступна гнучкість VPP** (Gauge) - поточне значення з порогами
3. **SOC батарейного флоту** (Histogram) - розподіл State of Charge батарей
4. **Compliance з setpoint** (Stat) - відсоток відповідності setpoint
5. **Відхилення від setpoint** (Time Series) - графік відхилень
6. **Статус DER** (Stat) - відсоток online/offline DER
7. **Dispatch Instruction Compliance** (Gauge) - виконання dispatch інструкцій
8. **Ефективність VPP** (Time Series)
9. **Response Time VPP** (Time Series)
10. **Utilization Flexibility** (Time Series)
11. **Market Participation Success** (Stat)
12. **Розподіл DER за типами** (Table)
13. **Потужність по типах DER** (Time Series)
14. **Гнучкість по типах DER** (Time Series)

## Аналіз

Система збирає наступні аналітичні метрики:

- **Ефективність VPP**: відношення фактичної до потенційної потужності
- **Response time**: час досягнення setpoint після його зміни
- **Utilization flexibility**: використання доступної гнучкості
- **Market participation success**: успішність участі на ринку (compliance > 80%)

## Корисні команди

### Перевірка метрик

```bash
curl http://localhost:8000/metrics | grep vpp
```

### Перевірка Prometheus targets

```bash
curl http://localhost:9090/api/v1/targets
```

### Перевірка алертів

Відкрийте: http://localhost:9090/alerts

### PromQL запити для тестування

Середня потужність VPP:

```promql
avg(vpp_net_power_kw)
```

Загальна гнучкість:

```promql
sum(vpp_total_flexibility_kw)
```

Відсоток online DER:

```promql
count(der_status == 1) / count(der_status) * 100
```

Compliance з setpoint:

```promql
vpp_compliance_percent
```

## Перегляд алертів та експорт даних

### Перегляд алертів:

Дивіться детальні інструкції в **`VIEW_ALERTS.md`**

Основні способи:

- Prometheus UI: http://localhost:9090 → вкладка "Alerts"
- Grafana: Alerting → Alert rules
- Командний рядок: `curl http://localhost:9090/api/v1/alerts`

### Експорт даних для аналізу:

Дивіться детальні інструкції в **`GET_DATA_FOR_ANALYSIS.md`**

Швидкий експорт:

```bash
./export_data.sh
```

Це створить файли в `exported_data/`:

- `metrics_export_*.json` - всі метрики
- `report_*.txt` - текстовий звіт зі статистикою

## Структура файлів

```
.
├── der_producer.py          # Головний producer для генерації даних DER
├── prometheus.yml           # Конфігурація Prometheus
├── alerts.yml               # Правила алертування
├── vpp_dashboard.json       # Grafana dashboard
├── requirements.txt         # Python залежності
├── install_and_setup.sh     # Автоматичне встановлення
├── start_services.sh        # Запуск сервісів
├── start_producer.sh        # Запуск producer
├── stop_services.sh         # Зупинка сервісів
├── check_system.sh          # Перевірка стану
├── setup_kafka_topics.sh    # Створення топиків
├── export_data.sh           # Експорт даних для аналізу
├── VIEW_ALERTS.md           # Інструкції по перегляду алертів
├── GET_DATA_FOR_ANALYSIS.md # Інструкції по експорту даних
├── QUICKSTART.md            # Швидкий старт
└── README.md               # Цей файл
```

## Автор

Створено для лабораторної роботи No5 - Моніторинг та алертинг енергетичних систем
Варіант 8: Розподілені енергетичні ресурси (DER) - VPP управління
