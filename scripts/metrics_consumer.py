#!/usr/bin/env python3
"""
Consumer для вимірювання метрик під час тестування batch.size та linger.ms
Аналізує latency та throughput для різних конфігурацій Producer
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
from typing import Dict, List, Any
import statistics

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Consumer для вимірювання метрик"""
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.setup_consumer()
        
        # Метрики для аналізу
        self.test_metrics = defaultdict(lambda: {
            'records': [],
            'latencies': [],
            'start_time': None,
            'end_time': None,
            'test_id': None
        })
        
        # Статистика в реальному часі
        self.realtime_stats = {
            'total_records': 0,
            'current_throughput': 0,
            'avg_latency': 0,
            'last_update': time.time()
        }
    
    def setup_consumer(self):
        """Налаштування Consumer для вимірювання метрик"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                group_id=f'metrics-consumer-{int(time.time())}',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=1000,
                fetch_min_bytes=1024,
                fetch_max_wait_ms=1000,
                # Налаштування для Kafka 3.7.1
                api_version=(3, 7, 1),
                security_protocol='PLAINTEXT',
                # Десеріалізація
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
            )
            logger.info("Metrics Consumer успішно налаштований")
        except Exception as e:
            logger.error(f"Помилка налаштування Consumer: {e}")
            raise
    
    def calculate_message_latency(self, message: Any) -> float:
        """Розраховує latency повідомлення"""
        try:
            # Отримуємо timestamp з повідомлення
            message_timestamp = message.timestamp
            if message_timestamp:
                # Розраховуємо latency як різницю між поточним часом та timestamp повідомлення
                current_time = time.time() * 1000  # Конвертуємо в мілісекунди
                latency_ms = current_time - message_timestamp
                return max(0, latency_ms)  # Негативний latency не має сенсу
            return 0
        except Exception as e:
            logger.error(f"Помилка розрахунку latency: {e}")
            return 0
    
    def process_message(self, message: Any):
        """Обробляє одне повідомлення та збирає метрики"""
        try:
            # Отримуємо дані з повідомлення
            record = message.value
            test_id = record.get('test_id', 'unknown')
            
            # Розраховуємо latency
            latency_ms = self.calculate_message_latency(message)
            
            # Збираємо метрики
            if test_id not in self.test_metrics:
                self.test_metrics[test_id]['start_time'] = time.time()
            
            self.test_metrics[test_id]['records'].append({
                'timestamp': record.get('timestamp'),
                'device_id': record.get('device_id'),
                'test_id': test_id,
                'kafka_timestamp': message.timestamp,
                'partition': message.partition,
                'offset': message.offset,
                'latency_ms': latency_ms
            })
            
            self.test_metrics[test_id]['latencies'].append(latency_ms)
            self.test_metrics[test_id]['end_time'] = time.time()
            
            # Оновлюємо статистику в реальному часі
            self.realtime_stats['total_records'] += 1
            
        except Exception as e:
            logger.error(f"Помилка обробки повідомлення: {e}")
    
    def calculate_test_metrics(self, test_id: str) -> Dict[str, Any]:
        """Розраховує метрики для конкретного тесту"""
        if test_id not in self.test_metrics:
            return {}
        
        test_data = self.test_metrics[test_id]
        records = test_data['records']
        latencies = test_data['latencies']
        
        if not records:
            return {}
        
        # Розраховуємо тривалість тесту
        duration = test_data['end_time'] - test_data['start_time'] if test_data['end_time'] and test_data['start_time'] else 0
        
        # Throughput
        throughput = len(records) / duration if duration > 0 else 0
        
        # Latency статистика
        if latencies:
            avg_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 0 else 0
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) > 0 else 0
        else:
            avg_latency = min_latency = max_latency = p95_latency = p99_latency = 0
        
        # Аналіз по партиціях
        partition_stats = defaultdict(int)
        for record in records:
            partition_stats[record['partition']] += 1
        
        return {
            'test_id': test_id,
            'total_records': len(records),
            'duration_seconds': round(duration, 2),
            'throughput_records_per_sec': round(throughput, 2),
            'avg_latency_ms': round(avg_latency, 2),
            'min_latency_ms': round(min_latency, 2),
            'max_latency_ms': round(max_latency, 2),
            'p95_latency_ms': round(p95_latency, 2),
            'p99_latency_ms': round(p99_latency, 2),
            'partition_distribution': dict(partition_stats),
            'start_time': test_data['start_time'],
            'end_time': test_data['end_time']
        }
    
    def print_realtime_stats(self):
        """Виводить статистику в реальному часі"""
        current_time = time.time()
        time_since_update = current_time - self.realtime_stats['last_update']
        
        if time_since_update >= 5:  # Оновлюємо кожні 5 секунд
            print(f"\n📊 РЕАЛЬНИЙ ЧАС:")
            print(f"   Загальна кількість записів: {self.realtime_stats['total_records']}")
            print(f"   Час останнього оновлення: {datetime.fromtimestamp(self.realtime_stats['last_update']).strftime('%H:%M:%S')}")
            
            # Показуємо статистику по активних тестах
            active_tests = [test_id for test_id, data in self.test_metrics.items() if data['records']]
            if active_tests:
                print(f"   Активні тести: {', '.join(active_tests)}")
            
            self.realtime_stats['last_update'] = current_time
    
    def print_test_summary(self, test_id: str):
        """Виводить підсумок тесту"""
        metrics = self.calculate_test_metrics(test_id)
        
        if not metrics:
            print(f"❌ Немає даних для тесту {test_id}")
            return
        
        print(f"\n📋 ПІДСУМОК ТЕСТУ: {test_id}")
        print(f"   Записів: {metrics['total_records']}")
        print(f"   Тривалість: {metrics['duration_seconds']}с")
        print(f"   Throughput: {metrics['throughput_records_per_sec']} rec/sec")
        print(f"   Avg Latency: {metrics['avg_latency_ms']} ms")
        print(f"   P95 Latency: {metrics['p95_latency_ms']} ms")
        print(f"   P99 Latency: {metrics['p99_latency_ms']} ms")
        
        if metrics['partition_distribution']:
            print(f"   Розподіл по партиціях: {metrics['partition_distribution']}")
    
    def consume_and_analyze(self, topics: List[str], duration_minutes: int = 10):
        """Основна функція споживання та аналізу"""
        logger.info(f"Початок споживання метрик з топіків: {topics}")
        logger.info(f"Тривалість: {duration_minutes} хвилин")
        
        # Підписуємося на топіки
        self.consumer.subscribe(topics)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        print("🚀 Metrics Consumer запущено!")
        print("📡 Збираємо метрики для аналізу batch.size та linger.ms...")
        print("🛑 Натисніть Ctrl+C для зупинки\n")
        
        try:
            while time.time() < end_time:
                # Отримуємо повідомлення батчами
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self.process_message(message)
                
                # Виводимо статистику в реальному часі
                self.print_realtime_stats()
                
                # Перевіряємо завершені тести
                for test_id in list(self.test_metrics.keys()):
                    test_data = self.test_metrics[test_id]
                    if test_data['end_time'] and (time.time() - test_data['end_time']) > 10:
                        # Тест завершився більше 10 секунд тому
                        self.print_test_summary(test_id)
                        # Видаляємо з активних тестів
                        del self.test_metrics[test_id]
        
        except KeyboardInterrupt:
            print(f"\n🛑 Consumer зупинено користувачем")
        
        # Фінальний аналіз всіх тестів
        print(f"\n📊 === ФІНАЛЬНИЙ АНАЛІЗ ВСІХ ТЕСТІВ ===")
        for test_id in self.test_metrics.keys():
            self.print_test_summary(test_id)
    
    def save_metrics(self, filename: str = "consumer_metrics.json"):
        """Зберігає зібрані метрики у файл"""
        try:
            all_metrics = {}
            for test_id in self.test_metrics.keys():
                all_metrics[test_id] = self.calculate_test_metrics(test_id)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_metrics, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Метрики збережено у файл: {filename}")
        except Exception as e:
            logger.error(f"Помилка збереження метрик: {e}")
    
    def close(self):
        """Закриття Consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("Metrics Consumer закрито")

def main():
    """Основна функція"""
    print("=== METRICS CONSUMER ДЛЯ BATCH ТЕСТУВАННЯ ===")
    print("Збір та аналіз метрик latency та throughput")
    print()
    
    consumer = MetricsConsumer()
    
    try:
        # Споживання з тестового топіку
        topics = ['der-batch-test']
        
        # Запускаємо споживання на 15 хвилин (достатньо для всіх тестів)
        consumer.consume_and_analyze(topics, duration_minutes=15)
        
        # Зберігаємо метрики
        consumer.save_metrics()
        
    except KeyboardInterrupt:
        print("\nПереривання користувачем...")
    except Exception as e:
        logger.error(f"Помилка виконання: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
