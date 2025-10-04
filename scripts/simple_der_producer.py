#!/usr/bin/env python3
"""
Простий Producer для тестування DER даних
Використовує базові налаштування для сумісності з Kafka 2.5.0
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleDERProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація простого Kafka Producer"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.setup_producer()
    
    def setup_producer(self):
        """Налаштування простого Producer з базовими параметрами"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Базові налаштування без складних оптимізацій
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Прості налаштування надійності
                acks='all',  # Підтвердження від всіх реплік
                retries=3,
                # Базові налаштування продуктивності
                batch_size=8192,
                linger_ms=5,
                # Налаштування для Kafka 3.7.1
                api_version=(3, 7, 1),
                security_protocol='PLAINTEXT',
            )
            logger.info("Простий Kafka Producer успішно налаштований для Kafka 3.7.1")
        except Exception as e:
            logger.error(f"Помилка налаштування Producer: {e}")
            raise
    
    def send_single_record(self, topic_name: str, record: dict):
        """Відправка одного запису"""
        try:
            device_id = record.get('device_id', 'unknown')
            future = self.producer.send(topic_name, key=device_id, value=record)
            # Чекаємо підтвердження
            record_metadata = future.get(timeout=10)
            logger.info(f"Запис відправлено до партиції {record_metadata.partition} з offset {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Помилка відправки запису {device_id}: {e}")
            return False
    
    def send_batch_records(self, topic_name: str, records: list):
        """Відправка батчу записів"""
        logger.info(f"Відправка {len(records)} записів до топіку {topic_name}")
        
        sent_count = 0
        failed_count = 0
        
        for record in records:
            if self.send_single_record(topic_name, record):
                sent_count += 1
            else:
                failed_count += 1
        
        logger.info(f"Відправка завершена. Успішно: {sent_count}, Помилок: {failed_count}")
        return sent_count, failed_count
    
    def generate_test_record(self, device_num: int) -> dict:
        """Генерує тестовий запис для пристрою"""
        device_types = ["solar_roof", "micro_wind", "battery", "combined"]
        statuses = ["generating", "consuming", "idle", "maintenance"]
        
        device_type = random.choice(device_types)
        
        # Генеруємо потужність відповідно до типу
        if device_type == "battery":
            power_output = round(random.uniform(-5.0, 10.0), 2)
        elif device_type == "solar_roof":
            power_output = round(random.uniform(0.0, 10.0), 2)
        elif device_type == "micro_wind":
            power_output = round(random.uniform(0.0, 8.0), 2)
        else:  # combined
            power_output = round(random.uniform(-3.0, 10.0), 2)
        
        record = {
            "device_id": f"DER_{device_num:04d}",
            "power_output": power_output,
            "efficiency": round(random.uniform(80.0, 96.0), 1),
            "temperature": round(random.uniform(-20.0, 50.0), 1),
            "voltage": round(random.uniform(220.0, 240.0), 1),
            "current": round(random.uniform(5.0, 45.0), 1),
            "status": random.choice(statuses),
            "location": {
                "lat": round(random.uniform(45.0, 52.0), 4),
                "lon": round(random.uniform(22.0, 40.0), 4)
            },
            "maintenance_hours": random.randint(1000, 8000),
            "net_power": round(power_output + random.uniform(-0.5, 0.5), 2),
            "battery_soc": round(random.uniform(0.0, 100.0), 1) if device_type in ["battery", "combined"] else 0.0,
            "unit_type": device_type,
            "timestamp": datetime.now().isoformat()
        }
        
        return record
    
    def send_test_data(self, topic_name: str, num_records: int = 10):
        """Відправка тестових даних"""
        logger.info(f"Генерація та відправка {num_records} тестових записів")
        
        records = []
        for i in range(1, num_records + 1):
            record = self.generate_test_record(i)
            records.append(record)
        
        return self.send_batch_records(topic_name, records)
    
    def close(self):
        """Закриття Producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer закрито")

def main():
    """Основна функція для тестування"""
    print("=== Простий DER Kafka Producer ===")
    print("Тестування сумісності з Kafka 3.7.1 та OpenJDK 11")
    print()
    
    producer = SimpleDERProducer()
    
    try:
        # Тест 1: Відправка одного запису
        print("Тест 1: Відправка одного тестового запису")
        test_record = producer.generate_test_record(1)
        print(f"Тестовий запис: {json.dumps(test_record, indent=2, ensure_ascii=False)}")
        
        success = producer.send_single_record("der-main", test_record)
        if success:
            print("✅ Тестовий запис успішно відправлено!")
        else:
            print("❌ Помилка відправки тестового запису")
        
        print("\n" + "="*50 + "\n")
        
        # Тест 2: Відправка батчу записів
        print("Тест 2: Відправка батчу з 10 записів")
        sent, failed = producer.send_test_data("der-main", 10)
        
        if failed == 0:
            print(f"✅ Всі {sent} записів успішно відправлено!")
        else:
            print(f"⚠️ Відправлено {sent}, помилок {failed}")
        
        print("\n" + "="*50 + "\n")
        
        # Тест 3: Відправка до тестового топіку
        print("Тест 3: Відправка до тестового топіку der-batch-test")
        sent, failed = producer.send_test_data("der-batch-test", 5)
        
        if failed == 0:
            print(f"✅ Всі {sent} записів успішно відправлено до тестового топіку!")
        else:
            print(f"⚠️ Відправлено {sent}, помилок {failed}")
        
    except KeyboardInterrupt:
        print("\nПереривання користувачем...")
    except Exception as e:
        logger.error(f"Помилка виконання: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
