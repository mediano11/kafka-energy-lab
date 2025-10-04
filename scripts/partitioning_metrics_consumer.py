#!/usr/bin/env python3
"""
Consumer для вимірювання метрик під час тестування партиціонування
Аналізує розподіл по партиціях, latency та throughput для різних стратегій
"""

import json
import time
import statistics
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
from typing import Dict, List, Any

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PartitioningMetricsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Consumer для вимірювання метрик партиціонування"""
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.setup_consumer()
        
        # Метрики для аналізу партиціонування
        self.partitioning_metrics = defaultdict(lambda: {
            'records': [],
            'latencies': [],
            'partition_distribution': defaultdict(int),
            'start_time': None,
            'end_time': None,
            'num_partitions': None,
            'strategy': None
        })
        
        # Статистика в реальному часі
        self.realtime_stats = {
            'total_records': 0,
            'current_throughput': 0,
            'avg_latency': 0,
            'last_update': time.time(),
            'partition_balance': defaultdict(float)
        }
    
    def setup_consumer(self):
        """Налаштування Consumer для вимірювання метрик партиціонування"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                group_id=f'partitioning-metrics-consumer-{int(time.time())}',
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
            logger.info("Partitioning Metrics Consumer успішно налаштований")
        except Exception as e:
            logger.error(f"Помилка налаштування Consumer: {e}")
            raise
    
    def calculate_message_latency(self, message: Any) -> float:
        """Розраховує latency повідомлення"""
        try:
            message_timestamp = message.timestamp
            if message_timestamp:
                current_time = time.time() * 1000
                latency_ms = current_time - message_timestamp
                return max(0, latency_ms)
            return 0
        except Exception as e:
            logger.error(f"Помилка розрахунку latency: {e}")
            return 0
    
    def analyze_partitioning_strategy(self, record: Dict[str, Any], message: Any) -> Dict[str, Any]:
        """Аналізує стратегію партиціонування для повідомлення"""
        analysis = {
            'strategy': record.get('partitioning_test', 'unknown'),
            'partition_id': message.partition,
            'partition_key': message.key.decode('utf-8') if message.key else 'none',
            'unit_type': record.get('unit_type', 'unknown'),
            'location': record.get('location', {}),
            'device_id': record.get('device_id', 'unknown'),
            'partitioning_effectiveness': True
        }
        
        # Аналіз ефективності партиціонування
        strategy = analysis['strategy']
        partition_key = analysis['partition_key']
        
        if strategy == 'unit_type':
            # Перевіряємо чи ключ партиції відповідає unit_type
            unit_type = record.get('unit_type', 'unknown')
            expected_key = f"unit_{unit_type}"
            analysis['partitioning_effectiveness'] = partition_key == expected_key
        
        elif strategy == 'geographic':
            # Перевіряємо чи ключ партиції відповідає географічному регіону
            location = record.get('location', {})
            lat = location.get('lat', 0)
            lon = location.get('lon', 0)
            
            # Визначаємо очікуваний регіон
            expected_region = "default"
            if 50.0 <= lat <= 51.0 and 30.0 <= lon <= 31.0:
                expected_region = "kyiv"
            elif 49.5 <= lat <= 50.0 and 23.5 <= lon <= 24.5:
                expected_region = "lviv"
            elif 49.8 <= lat <= 50.2 and 36.0 <= lon <= 36.5:
                expected_region = "kharkiv"
            elif 46.0 <= lat <= 47.0 and 30.0 <= lon <= 31.0:
                expected_region = "odessa"
            elif 48.0 <= lat <= 49.0 and 35.0 <= lon <= 36.0:
                expected_region = "dnipro"
            
            expected_key = f"geo_{expected_region}"
            analysis['partitioning_effectiveness'] = partition_key == expected_key
        
        elif strategy == 'round_robin':
            # Для round robin перевіряємо чи ключ містить device_id
            device_id = record.get('device_id', 'unknown')
            analysis['partitioning_effectiveness'] = device_id in partition_key
        
        return analysis
    
    def process_message(self, message: Any):
        """Обробляє одне повідомлення та збирає метрики партиціонування"""
        try:
            # Отримуємо дані з повідомлення
            record = message.value
            strategy = record.get('partitioning_test', 'unknown')
            topic_name = message.topic
            
            # Розраховуємо метрики
            latency_ms = self.calculate_message_latency(message)
            partitioning_analysis = self.analyze_partitioning_strategy(record, message)
            
            # Визначаємо кількість партицій з назви топіку
            num_partitions = 10  # За замовчуванням
            if 'der-part-15' in topic_name:
                num_partitions = 15
            elif 'der-part-20' in topic_name:
                num_partitions = 20
            
            # Збираємо метрики
            test_key = f"{num_partitions}_{strategy}"
            if test_key not in self.partitioning_metrics:
                self.partitioning_metrics[test_key]['start_time'] = time.time()
            
            self.partitioning_metrics[test_key]['records'].append({
                'timestamp': record.get('timestamp'),
                'device_id': record.get('device_id'),
                'strategy': strategy,
                'topic_name': topic_name,
                'partition_id': message.partition,
                'partition_key': message.key.decode('utf-8') if message.key else 'none',
                'kafka_timestamp': message.timestamp,
                'offset': message.offset,
                'latency_ms': latency_ms,
                'partitioning_analysis': partitioning_analysis
            })
            
            self.partitioning_metrics[test_key]['latencies'].append(latency_ms)
            self.partitioning_metrics[test_key]['partition_distribution'][message.partition] += 1
            self.partitioning_metrics[test_key]['end_time'] = time.time()
            self.partitioning_metrics[test_key]['num_partitions'] = num_partitions
            self.partitioning_metrics[test_key]['strategy'] = strategy
            
            # Оновлюємо статистику в реальному часі
            self.realtime_stats['total_records'] += 1
            
        except Exception as e:
            logger.error(f"Помилка обробки повідомлення: {e}")
    
    def calculate_partitioning_metrics(self, test_key: str) -> Dict[str, Any]:
        """Розраховує метрики для конкретного тесту партиціонування"""
        if test_key not in self.partitioning_metrics:
            return {}
        
        metrics_data = self.partitioning_metrics[test_key]
        records = metrics_data['records']
        latencies = metrics_data['latencies']
        partition_distribution = dict(metrics_data['partition_distribution'])
        
        if not records:
            return {}
        
        # Розраховуємо тривалість тесту
        duration = metrics_data['end_time'] - metrics_data['start_time'] if metrics_data['end_time'] and metrics_data['start_time'] else 0
        
        # Throughput
        throughput = len(records) / duration if duration > 0 else 0
        
        # Latency статистика
        if latencies:
            avg_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            sorted_latencies = sorted(latencies)
            p50_latency = sorted_latencies[int(len(latencies) * 0.50)]
            p95_latency = sorted_latencies[int(len(latencies) * 0.95)]
            p99_latency = sorted_latencies[int(len(latencies) * 0.99)]
            latency_std_dev = statistics.stdev(latencies) if len(latencies) > 1 else 0
        else:
            avg_latency = min_latency = max_latency = p50_latency = p95_latency = p99_latency = latency_std_dev = 0
        
        # Аналіз розподілу по партиціях
        partition_balance = self.analyze_partition_balance(partition_distribution, metrics_data['num_partitions'])
        
        # Аналіз ефективності партиціонування
        partitioning_effectiveness = self.analyze_partitioning_effectiveness(records)
        
        return {
            'test_key': test_key,
            'num_partitions': metrics_data['num_partitions'],
            'strategy': metrics_data['strategy'],
            'total_records': len(records),
            'duration_seconds': round(duration, 2),
            'throughput_records_per_sec': round(throughput, 2),
            # Latency метрики
            'avg_latency_ms': round(avg_latency, 2),
            'min_latency_ms': round(min_latency, 2),
            'max_latency_ms': round(max_latency, 2),
            'p50_latency_ms': round(p50_latency, 2),
            'p95_latency_ms': round(p95_latency, 2),
            'p99_latency_ms': round(p99_latency, 2),
            'latency_std_dev': round(latency_std_dev, 2),
            # Партиціонування метрики
            'partition_distribution': partition_distribution,
            'partition_balance_score': partition_balance['balance_score'],
            'partition_utilization': partition_balance['utilization'],
            'partitioning_effectiveness': partitioning_effectiveness['effectiveness_score'],
            'start_time': metrics_data['start_time'],
            'end_time': metrics_data['end_time']
        }
    
    def analyze_partition_balance(self, partition_distribution: Dict[int, int], num_partitions: int) -> Dict[str, Any]:
        """Аналізує баланс розподілу по партиціях"""
        if not partition_distribution:
            return {'balance_score': 0.0, 'utilization': 0.0}
        
        partition_counts = list(partition_distribution.values())
        total_records = sum(partition_counts)
        
        if total_records == 0:
            return {'balance_score': 0.0, 'utilization': 0.0}
        
        # Ідеальний розподіл
        ideal_count_per_partition = total_records / num_partitions
        
        # Розраховуємо баланс
        variance = statistics.variance(partition_counts) if len(partition_counts) > 1 else 0
        balance_score = max(0, 1 - (variance / (ideal_count_per_partition ** 2)))
        
        # Утилізація партицій
        used_partitions = len(partition_distribution)
        utilization = used_partitions / num_partitions
        
        return {
            'balance_score': balance_score,
            'utilization': utilization,
            'ideal_count': ideal_count_per_partition,
            'actual_counts': partition_counts,
            'used_partitions': used_partitions,
            'total_partitions': num_partitions
        }
    
    def analyze_partitioning_effectiveness(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Аналізує ефективність стратегії партиціонування"""
        if not records:
            return {'effectiveness_score': 0.0, 'correct_partitioning': 0}
        
        correct_count = 0
        total_count = len(records)
        
        for record in records:
            if 'partitioning_analysis' in record:
                if record['partitioning_analysis'].get('partitioning_effectiveness', False):
                    correct_count += 1
        
        effectiveness_score = correct_count / total_count if total_count > 0 else 0
        
        return {
            'effectiveness_score': effectiveness_score,
            'correct_partitioning': correct_count,
            'total_records': total_count
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
            active_tests = [test_key for test_key, data in self.partitioning_metrics.items() if data['records']]
            if active_tests:
                print(f"   Активні тести: {', '.join(active_tests)}")
                
                # Показуємо partition balance
                for test_key in active_tests:
                    partition_dist = self.partitioning_metrics[test_key]['partition_distribution']
                    if partition_dist:
                        num_partitions = self.partitioning_metrics[test_key]['num_partitions']
                        balance = self.analyze_partition_balance(dict(partition_dist), num_partitions)
                        print(f"   {test_key}: {balance['balance_score']:.2f} balance, {balance['utilization']:.2f} utilization")
            
            self.realtime_stats['last_update'] = current_time
    
    def print_partitioning_summary(self, test_key: str):
        """Виводить підсумок тесту партиціонування"""
        metrics = self.calculate_partitioning_metrics(test_key)
        
        if not metrics:
            print(f"❌ Немає даних для тесту {test_key}")
            return
        
        print(f"\n📋 ПІДСУМОК ПАРТИЦІОНУВАННЯ: {test_key}")
        print(f"   Партицій: {metrics['num_partitions']}")
        print(f"   Стратегія: {metrics['strategy']}")
        print(f"   Записів: {metrics['total_records']}")
        print(f"   Тривалість: {metrics['duration_seconds']}с")
        print(f"   Throughput: {metrics['throughput_records_per_sec']} rec/sec")
        print(f"   Avg Latency: {metrics['avg_latency_ms']} ms")
        print(f"   P50 Latency: {metrics['p50_latency_ms']} ms")
        print(f"   P95 Latency: {metrics['p95_latency_ms']} ms")
        print(f"   Partition Balance: {metrics['partition_balance_score']:.2f}")
        print(f"   Partition Utilization: {metrics['partition_utilization']:.2f}")
        print(f"   Partitioning Effectiveness: {metrics['partitioning_effectiveness']:.2f}")
    
    def consume_and_analyze(self, topics: List[str], duration_minutes: int = 20):
        """Основна функція споживання та аналізу партиціонування"""
        logger.info(f"Початок споживання партиціонування метрик з топіків: {topics}")
        logger.info(f"Тривалість: {duration_minutes} хвилин")
        
        # Підписуємося на топіки
        self.consumer.subscribe(topics)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        print("🚀 Partitioning Metrics Consumer запущено!")
        print("📡 Збираємо метрики для аналізу партиціонування...")
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
                for test_key in list(self.partitioning_metrics.keys()):
                    metrics_data = self.partitioning_metrics[test_key]
                    if metrics_data['end_time'] and (time.time() - metrics_data['end_time']) > 10:
                        # Тест завершився більше 10 секунд тому
                        self.print_partitioning_summary(test_key)
                        # Видаляємо з активних тестів
                        del self.partitioning_metrics[test_key]
        
        except KeyboardInterrupt:
            print(f"\n🛑 Consumer зупинено користувачем")
        
        # Фінальний аналіз всіх тестів
        print(f"\n📊 === ФІНАЛЬНИЙ АНАЛІЗ ВСІХ ПАРТИЦІОНУВАННЯ ТЕСТІВ ===")
        for test_key in self.partitioning_metrics.keys():
            self.print_partitioning_summary(test_key)
    
    def save_metrics(self, filename: str = "data/partitioning_metrics.json"):
        """Зберігає зібрані метрики у файл"""
        try:
            # Створюємо папку data якщо не існує
            import os
            os.makedirs("data", exist_ok=True)
            
            all_metrics = {}
            for test_key in self.partitioning_metrics.keys():
                all_metrics[test_key] = self.calculate_partitioning_metrics(test_key)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_metrics, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Partitioning метрики збережено у файл: {filename}")
        except Exception as e:
            logger.error(f"Помилка збереження метрик: {e}")
    
    def close(self):
        """Закриття Consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("Partitioning Metrics Consumer закрито")

def main():
    """Основна функція"""
    print("=== PARTITIONING METRICS CONSUMER ===")
    print("Збір та аналіз метрик партиціонування та розподілу")
    print()
    
    consumer = PartitioningMetricsConsumer()
    
    try:
        # Споживання з partitioning топіків
        topics = ['der-part-10', 'der-part-15', 'der-part-20']
        
        # Запускаємо споживання на 25 хвилин (достатньо для всіх тестів)
        consumer.consume_and_analyze(topics, duration_minutes=25)
        
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
