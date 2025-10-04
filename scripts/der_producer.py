#!/usr/bin/env python3
"""
Producer для відправки тестових даних DER системи до Kafka
Налаштований для high-volume обробки 1000 пристроїв
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

class DERProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Kafka Producer з оптимізацією для high-volume"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.setup_producer()
    
    def setup_producer(self):
        """Налаштування Producer з параметрами для високої пропускної здатності"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Налаштування для high-volume обробки
                batch_size=16384,  # Розмір батчу в байтах
                linger_ms=10,      # Затримка для збирання батчів (мс)
                compression_type='snappy',  # Стиснення для економії пропускної здатності
                buffer_memory=33554432,  # Буфер пам'яті (32MB)
                max_block_ms=10000,  # Максимальний час очікування
                retries=3,  # Кількість повторних спроб
                retry_backoff_ms=100,  # Затримка між повторними спробами
                # Налаштування серіалізації для Kafka 3.7.1
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Налаштування надійності
                acks='all',  # Підтвердження від всіх реплік для надійності
                request_timeout_ms=30000,
                # Налаштування для оптимізації мережі
                send_buffer_bytes=131072,  # 128KB
                receive_buffer_bytes=32768,  # 32KB
                # Налаштування для Kafka 3.7.1 та OpenJDK 11
                api_version=(3, 7, 1),  # Версія API для Kafka 3.7.1
                security_protocol='PLAINTEXT',
                # Додаткові налаштування для стабільності
                max_in_flight_requests_per_connection=5,  # Контроль паралельних запитів
            )
            logger.info("Kafka Producer успішно налаштований для Kafka 3.7.1")
        except Exception as e:
            logger.error(f"Помилка налаштування Producer: {e}")
            raise
    
    def send_der_data(self, topic_name: str, records: list, batch_size: int = 100):
        """Відправка даних DER до Kafka топіку батчами"""
        logger.info(f"Початок відправки {len(records)} записів до топіку {topic_name}")
        
        sent_count = 0
        failed_count = 0
        
        # Відправляємо дані батчами
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Відправляємо кожен запис у батчі
            for record in batch:
                try:
                    # Використовуємо device_id як ключ для партиціонування
                    device_id = record.get('device_id', 'unknown')
                    
                    future = self.producer.send(
                        topic_name,
                        key=device_id,
                        value=record
                    )
                    
                    # Не чекаємо підтвердження для кожного повідомлення
                    # для підвищення пропускної здатності
                    sent_count += 1
                    
                except Exception as e:
                    logger.error(f"Помилка відправки запису {device_id}: {e}")
                    failed_count += 1
            
            # Невелика пауза між батчами для контролю навантаження
            if i + batch_size < len(records):
                time.sleep(0.01)  # 10мс пауза
        
        # Чекаємо завершення всіх відправлених повідомлень
        logger.info("Очікування завершення відправки всіх повідомлень...")
        self.producer.flush(timeout=30)
        
        logger.info(f"Відправка завершена. Успішно: {sent_count}, Помилок: {failed_count}")
        return sent_count, failed_count
    
    def send_realtime_data(self, topic_name: str, num_devices: int = 1000, duration_seconds: int = 60):
        """Симуляція реального часу відправки даних кожні 60 секунд"""
        logger.info(f"Початок симуляції реального часу для {num_devices} пристроїв")
        logger.info(f"Тривалість: {duration_seconds} секунд")
        
        device_ids = [f"DER_{i:04d}" for i in range(1, num_devices + 1)]
        device_types = ["solar_roof", "micro_wind", "battery", "combined"]
        statuses = ["generating", "consuming", "idle", "maintenance"]
        
        start_time = time.time()
        iteration = 0
        
        while time.time() - start_time < duration_seconds:
            iteration += 1
            logger.info(f"Ітерація {iteration}: відправка даних для всіх пристроїв")
            
            batch_records = []
            
            # Генеруємо дані для всіх пристроїв
            for device_id in device_ids:
                device_type = random.choice(device_types)
                
                # Генеруємо дані відповідно до специфікації
                power_output = self._generate_power_output(device_type)
                
                record = {
                    "device_id": device_id,
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
                    "timestamp": datetime.now().isoformat(),
                    "iteration": iteration
                }
                
                batch_records.append(record)
            
            # Відправляємо батч
            sent_count, failed_count = self.send_der_data(topic_name, batch_records, batch_size=200)
            
            logger.info(f"Ітерація {iteration} завершена. Відправлено: {sent_count}, Помилок: {failed_count}")
            
            # Чекаємо 60 секунд до наступної ітерації
            logger.info("Очікування 60 секунд до наступної ітерації...")
            time.sleep(60)
        
        logger.info("Симуляція реального часу завершена")
    
    def _generate_power_output(self, device_type: str) -> float:
        """Генерує потужність відповідно до типу пристрою"""
        if device_type == "battery":
            return round(random.uniform(-5.0, 10.0), 2)
        elif device_type == "solar_roof":
            return round(random.uniform(0.0, 10.0), 2)
        elif device_type == "micro_wind":
            return round(random.uniform(0.0, 8.0), 2)
        else:  # combined
            return round(random.uniform(-3.0, 10.0), 2)
    
    def close(self):
        """Закриття Producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer закрито")

def load_test_data(filename: str) -> list:
    """Завантаження тестових даних з JSON файлу"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Завантажено {len(data)} записів з файлу {filename}")
        return data
    except Exception as e:
        logger.error(f"Помилка завантаження файлу {filename}: {e}")
        return []

def main():
    """Основна функція"""
    print("=== DER Kafka Producer ===")
    print("Налаштування для high-volume обробки 1000 пристроїв")
    print()
    
    # Ініціалізуємо Producer
    producer = DERProducer()
    
    try:
        # Варіант 1: Відправка тестових даних з файлу
        print("Варіант 1: Відправка тестових даних з файлу")
        test_data = load_test_data("data/der_test_data.json")
        
        if test_data:
            print(f"Відправка {len(test_data)} записів до топіку 'der-main'...")
            sent, failed = producer.send_der_data("der-main", test_data, batch_size=100)
            print(f"Результат: відправлено {sent}, помилок {failed}")
        
        print("\n" + "="*50 + "\n")
        
        # Варіант 2: Симуляція реального часу
        print("Варіант 2: Симуляція реального часу (5 хвилин)")
        print("Кожен пристрій відправляє дані кожні 60 секунд")
        
        # Коротка симуляція на 5 хвилин для демонстрації
        producer.send_realtime_data("der-batch-test", num_devices=1000, duration_seconds=300)
        
    except KeyboardInterrupt:
        print("\nПереривання користувачем...")
    except Exception as e:
        logger.error(f"Помилка виконання: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
