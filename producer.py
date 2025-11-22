"""
Producer для генерації тестових даних DER активів
Генерує телеметричні дані кожні 60 секунд для 1000 активів
"""
import json
import time
import random
import uuid
from datetime import datetime, timezone
from typing import List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_DATA,
    DER_CONFIG,
    KAFKA_PRODUCER_CONFIG
)
from models import DERReading, AssetType


class DERDataProducer:
    """Генератор та відправник телеметричних даних DER"""
    
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
        })
        self.assets = self._generate_assets()
        self.asset_states = {asset_id: self._init_asset_state(asset_type) 
                           for asset_id, asset_type in self.assets.items()}
    
    def _generate_assets(self) -> dict[str, AssetType]:
        """Генерує 1000 активів з різними типами"""
        assets = {}
        asset_types = DER_CONFIG['asset_types']
        total = DER_CONFIG['total_assets']
        
        # Рівномірний розподіл типів
        assets_per_type = total // len(asset_types)
        remainder = total % len(asset_types)
        
        asset_id = 0
        for i, asset_type in enumerate(asset_types):
            count = assets_per_type + (1 if i < remainder else 0)
            for _ in range(count):
                assets[f"{asset_type}_{asset_id:04d}"] = asset_type
                asset_id += 1
        
        return assets
    
    def _init_asset_state(self, asset_type: AssetType) -> dict:
        """Ініціалізує початковий стан активу"""
        if asset_type == 'solar':
            capacity = random.uniform(*DER_CONFIG['solar_capacity_range'])
            return {
                'capacity': capacity,
                'power_output': random.uniform(0, capacity * 0.9),
                'available_capacity': capacity,
                'forecasted_output': capacity * random.uniform(0.3, 0.8)
            }
        elif asset_type == 'wind':
            capacity = random.uniform(*DER_CONFIG['wind_capacity_range'])
            return {
                'capacity': capacity,
                'power_output': random.uniform(0, capacity * 0.85),
                'available_capacity': capacity,
                'forecasted_output': capacity * random.uniform(0.2, 0.75)
            }
        elif asset_type == 'battery':
            capacity = random.uniform(*DER_CONFIG['battery_capacity_range'])
            soc = random.uniform(20, 90)
            return {
                'capacity': capacity,
                'power_output': random.uniform(-capacity * 0.5, capacity * 0.5),  # Може заряжати/розряджати
                'available_capacity': capacity * (soc / 100),
                'soc': soc,
                'forecasted_output': capacity * random.uniform(-0.3, 0.3)
            }
        else:  # diesel
            capacity = random.uniform(*DER_CONFIG['diesel_capacity_range'])
            fuel_level = random.uniform(30, 100)
            return {
                'capacity': capacity,
                'power_output': random.uniform(0, capacity * 0.95),
                'available_capacity': capacity * (fuel_level / 100),
                'fuel_level': fuel_level,
                'forecasted_output': capacity * random.uniform(0.5, 0.9)
            }
    
    def _update_asset_state(self, asset_id: str, asset_type: AssetType) -> dict:
        """Оновлює стан активу з реалістичними змінами"""
        state = self.asset_states[asset_id].copy()
        
        if asset_type == 'solar':
            # Імітація змін сонячної активності
            state['power_output'] = max(0, min(
                state['capacity'],
                state['power_output'] + random.uniform(-50, 50)
            ))
            state['available_capacity'] = state['capacity']
            state['forecasted_output'] = state['capacity'] * random.uniform(0.2, 0.9)
        
        elif asset_type == 'wind':
            # Імітація змін вітру
            state['power_output'] = max(0, min(
                state['capacity'],
                state['power_output'] + random.uniform(-100, 100)
            ))
            state['available_capacity'] = state['capacity']
            state['forecasted_output'] = state['capacity'] * random.uniform(0.1, 0.8)
        
        elif asset_type == 'battery':
            # Імітація зарядки/розрядки
            power_change = random.uniform(-20, 20)
            state['power_output'] = max(-state['capacity'] * 0.5, 
                                       min(state['capacity'] * 0.5, 
                                           state['power_output'] + power_change))
            # Оновлення SOC
            if state['power_output'] > 0:  # Розрядка
                state['soc'] = max(0, state['soc'] - abs(power_change) / state['capacity'] * 2)
            else:  # Зарядка
                state['soc'] = min(100, state['soc'] + abs(power_change) / state['capacity'] * 2)
            state['available_capacity'] = state['capacity'] * (state['soc'] / 100)
            state['forecasted_output'] = state['capacity'] * random.uniform(-0.4, 0.4)
        
        else:  # diesel
            # Імітація роботи генератора
            state['power_output'] = max(0, min(
                state['capacity'],
                state['power_output'] + random.uniform(-30, 30)
            ))
            # Зменшення палива
            state['fuel_level'] = max(0, state['fuel_level'] - random.uniform(0.1, 0.5))
            state['available_capacity'] = state['capacity'] * (state['fuel_level'] / 100)
            state['forecasted_output'] = state['capacity'] * random.uniform(0.4, 0.95)
        
        self.asset_states[asset_id] = state
        return state
    
    def _create_reading(self, asset_id: str, asset_type: AssetType) -> DERReading:
        """Створює об'єкт телеметричного читання"""
        state = self._update_asset_state(asset_id, asset_type)
        
        reading = DERReading(
            asset_id=asset_id,
            asset_type=asset_type,
            timestamp=datetime.now(timezone.utc),
            power_output=round(state['power_output'], 2),
            available_capacity=round(state['available_capacity'], 2),
            forecasted_output=round(state['forecasted_output'], 2)
        )
        
        if asset_type == 'battery':
            reading.soc = round(state['soc'], 2)
        elif asset_type == 'diesel':
            reading.fuel_level = round(state['fuel_level'], 2)
        
        return reading
    
    def _delivery_callback(self, err, msg):
        """Callback для підтвердження доставки"""
        if err:
            print(f'Помилка доставки повідомлення: {err}')
        else:
            print(f'Повідомлення доставлено до {msg.topic()} [{msg.partition()}]')
    
    def create_topic(self):
        """Створює Kafka topic якщо не існує"""
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
        topic_list = [NewTopic(
            KAFKA_TOPIC_RAW_DATA,
            num_partitions=3,
            replication_factor=1
        )]
        
        futures = admin_client.create_topics(topic_list)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic {topic} створено успішно")
            except Exception as e:
                print(f"Помилка створення topic {topic}: {e}")
    
    def generate_batch(self) -> List[DERReading]:
        """Генерує один батч даних для всіх активів"""
        readings = []
        for asset_id, asset_type in self.assets.items():
            reading = self._create_reading(asset_id, asset_type)
            readings.append(reading)
        return readings
    
    def send_readings(self, readings: List[DERReading]):
        """Відправляє читання в Kafka"""
        for reading in readings:
            message = reading.model_dump_json()
            self.producer.produce(
                KAFKA_TOPIC_RAW_DATA,
                key=reading.asset_id.encode('utf-8'),
                value=message.encode('utf-8'),
                callback=self._delivery_callback
            )
        
        # Очікування завершення відправки
        self.producer.flush()
    
    def check_kafka_connection(self):
        """Перевірка підключення до Kafka"""
        import socket
        host, port = KAFKA_BOOTSTRAP_SERVERS.split(':')
        port = int(port)
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
            else:
                return False
        except Exception as e:
            print(f"Помилка перевірки підключення: {e}")
            return False
    
    def run(self, duration_minutes: int = None):
        """Запускає генерацію даних"""
        print(f"Запуск producer для {len(self.assets)} активів")
        print(f"Інтервал оновлення: {DER_CONFIG['update_interval_seconds']} секунд")
        
        # Перевірка підключення до Kafka
        print(f"\nПеревірка підключення до Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
        if not self.check_kafka_connection():
            print(f"❌ Помилка: Не вдалося підключитися до Kafka на {KAFKA_BOOTSTRAP_SERVERS}")
            print("\n💡 Рішення:")
            print("   1. Переконайтеся, що Kafka запущено: docker-compose ps")
            print("   2. Запустіть Kafka: docker-compose up -d kafka")
            print("   3. Дочекайтеся повного запуску (30-60 секунд)")
            print("   4. Перевірте налаштування в config.py")
            print("\n   Для віддаленого сервера переконайтеся, що:")
            print("   - Порт 9092 відкритий")
            print("   - KAFKA_ADVERTISED_LISTENERS налаштовано правильно")
            return
        
        print("✅ Підключення до Kafka успішне")
        
        self.create_topic()
        
        iteration = 0
        start_time = time.time()
        
        try:
            while True:
                iteration += 1
                print(f"\n=== Ітерація {iteration} ===")
                print(f"Генерація даних для {len(self.assets)} активів...")
                
                readings = self.generate_batch()
                self.send_readings(readings)
                
                print(f"Відправлено {len(readings)} повідомлень")
                
                if duration_minutes and (time.time() - start_time) > duration_minutes * 60:
                    break
                
                time.sleep(DER_CONFIG['update_interval_seconds'])
        
        except KeyboardInterrupt:
            print("\nЗупинка producer...")
        finally:
            self.producer.flush()


if __name__ == '__main__':
    producer = DERDataProducer()
    producer.run(duration_minutes=30)  # Генерувати дані 30 хвилин

