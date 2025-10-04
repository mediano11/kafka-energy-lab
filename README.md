# Лабораторна робота №1

### Проектування систем з розподіленими базами даних в енергетиці

**Тема:** Дослідження продуктивності Apache Kafka для енергетичних
IoT-потоків\
**Варіант:** 8 Розподілені енергетичні ресурси (DER)
**Підваріант:** A (оптимізація для real-time критичних алертів)
**Виконав:** студент групи ТМ-52 Пестенков Дмітрій
**Перевірив:** [ПІБ викладача]

---

## 🎯 Мета роботи

Дослідити продуктивність Apache Kafka для потоків даних для розподілені енергетичних ресурсів.\

- Кількість пристроїв: 1000 дрібних генераторів (сонячні дахи, малі вітряки, батареї)
- Частота передачі: Кожен пристрій відправляє дані кожні 60 секунд
- Географія: Приватні домогосподарства по всій Україні
- Особливості: Peer-to-peer energy trading, virtual power plant aggregation

Додаткові завдання:

- Тестувати batch.size до 8KB для ultra-low latency
- Фокус на 50th та 95th percentile латентності
- Аналіз критичних алертів для [специфічний параметр]
- Рекомендації для SCADA інтеграції
  Унікальний аналіз:
- Детальний аналіз впливу linger.ms=1,2,3,5ms
- Consumer lag моніторинг
- Network jitter вплив на latency

Стандартні параметри тестування:

- Batch sizes: 16384, 65536, 262144 байт (16KB, 64KB, 256KB)
- Linger values: 0, 10, 50 мілісекунд
- Compression types: none, snappy, lz4, zstd
- Partition counts: 3, 6, 12 партицій

---

## ⚙️ Тестове середовище

- **OS:** Ubuntu 22.04\
- **Java:** 11\
- **Kafka:** 3.7.1 (single-broker), Zookeeper 3.8\
- **Python:** 3.10.12\
- **Hardware:** 4 vCores, 4 GB RAM, SSD

### Тестові дані

- Тип: Розподілені енергетичні ресурси приватних домогосподарств
- Формат: 256 байт
- Кількість: 2000
- Ключові параметри:

---

## 📊 4.Основні результати

# 4.1 Batch.size та linger.ms тестування

## 🧪 Таблиця 1 – Ключові результати (9 тестів)

| Конфігурація | Records/sec | Avg Latency (ms) | P95 Latency (ms) | Success Rate (%) | Використання |
| ------------ | ----------- | ---------------- | ---------------- | ---------------- | ------------ |
| 16KB_0ms     | 403.66      | 2.46             | 3.5              | 100.0            | Real-time    |
| 16KB_10ms    | 75.93       | 13.15            | 13.95            | 100.0            | Balanced     |
| 16KB_50ms    | 18.95       | 52.76            | 54.29            | 100.0            | Batch        |
| 64KB_0ms     | 450.35      | 2.21             | 3.28             | 100.0            | Real-time    |
| 64KB_10ms    | 75.55       | 13.22            | 14.6             | 100.0            | Balanced     |
| 64KB_50ms    | 18.86       | 52.99            | 53.79            | 100.0            | Batch        |
| 256KB_0ms    | 555.73      | 1.78             | 2.46             | 100.0            | Real-time    |
| 256KB_10ms   | 77.8        | 12.84            | 13.57            | 100.0            | Balanced     |
| 256KB_50ms   | 18.94       | 52.78            | 53.52            | 100.0            | Batch        |

---

## 🏆 Найкращі результати

- **Максимальний throughput:** `256KB_0ms` → **555.73 rec/sec**
- **Мінімальна latency:** `256KB_0ms` → **1.78 ms**
- **Оптимальний баланс:** `256KB_10ms` → для агрегації **1000 DER пристроїв**

---

## 📊 Аналіз по типах використання

### Real-time (`linger_ms = 0`)

- **Середній throughput:** `469.9 rec/sec`
- **Середня latency:** `2.1 ms`

### Balanced (`linger_ms = 10`)

- **Середній throughput:** `76.4 rec/sec`
- **Середня latency:** `13.1 ms`

### Batch (`linger_ms = 50`)

- **Середній throughput:** `18.9 rec/sec`
- **Середня latency:** `52.8 ms`

---

## 💡 Рекомендації для DER системи

### Для Virtual Power Plant aggregation з 1000 пристроїв:

- ⚠️ **Цільовий throughput `1000+ rec/sec` не досягнуто**
- ✅ **Досягнута цільова latency `≤ 100 ms`**

---

## 🎯 Оптимальні налаштування

- **Максимальний throughput:**  
  `batch_size=262144`, `linger_ms=0`

- **Мінімальна latency:**  
  `batch_size=262144`, `linger_ms=0`

## 📂 Додатки

### A. Генератор даних (Python)

```python
import random, json
N = 12

def generate_powerplant_data():
    return {
        "device_id": f"TESC_{random.randint(1, N):03d}",
        "fuel_flow": round(random.uniform(120.0, 250.0), 2),
        "steam_pressure": round(random.uniform(14.0, 23.0), 2),
        "temperature": round(random.uniform(480.0, 560.0), 1),
        "timestamp": "2025-09-27T12:00:00Z"
    }

for _ in range(3):
    print(json.dumps(generate_powerplant_data(), ensure_ascii=False))
```

### B. Тестові команди Kafka

```bash
kafka-producer-perf-test.sh   --topic test_powerplant   --num-records 100000   --record-size 256   --throughput -1   --producer-props bootstrap.servers=localhost:9092 batch.size=16384 linger.ms=0 compression.type=none
```
