#!/usr/bin/env python3
"""
Consumer для вимірювання метрик під час тестування compression алгоритмів
Аналізує compression ratio, latency та throughput для різних алгоритмів
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

class CompressionMetricsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Consumer для вимірювання метрик compression"""
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.setup_consumer()
        
        # Метрики для аналізу compression
        self.compression_metrics = defaultdict(lambda: {
            'records': [],
            'latencies': [],
            'uncompressed_sizes': [],
            'compressed_sizes': [],
            'start_time': None,
            'end_time': None,
            'compression_type': None
        })
        
        # Статистика в реальному часі
        self.realtime_stats = {
            'total_records': 0,
            'current_throughput': 0,
            'avg_latency': 0,
            'last_update': time.time(),
            'compression_ratios': defaultdict(list)
        }
    
    def setup_consumer(self):
        """Налаштування Consumer для вимірювання метрик compression"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                group_id=f'compression-metrics-consumer-{int(time.time())}',
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
            logger.info("Compression Metrics Consumer успішно налаштований")
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
    
    def calculate_compression_ratio(self, record: Dict[str, Any], message: Any) -> Dict[str, float]:
        """Розраховує compression ratio для повідомлення"""
        try:
            # Розмір до стиснення
            uncompressed_data = json.dumps(record, ensure_ascii=False).encode('utf-8')
            uncompressed_size = len(uncompressed_data)
            
            # Розмір після стиснення (приблизний)
            compressed_size = len(message.value) if hasattr(message, 'value') else uncompressed_size
            
            # Compression ratio
            if uncompressed_size > 0:
                compression_ratio = (1 - (compressed_size / uncompressed_size)) * 100
            else:
                compression_ratio = 0
            
            return {
                'uncompressed_size': uncompressed_size,
                'compressed_size': compressed_size,
                'compression_ratio': compression_ratio
            }
        except Exception as e:
            logger.error(f"Помилка розрахунку compression ratio: {e}")
            return {
                'uncompressed_size': 0,
                'compressed_size': 0,
                'compression_ratio': 0
            }
    
    def analyze_der_compression_patterns(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Аналізує паттерни стиснення для DER даних"""
        analysis = {
            'unit_type': record.get('unit_type', 'unknown'),
            'status': record.get('status', 'unknown'),
            'battery_soc': record.get('battery_soc', 0),
            'compression_friendly': True,
            'patterns': []
        }
        
        # Аналіз паттернів для кращого стиснення
        unit_type = record.get('unit_type', 'unknown')
        status = record.get('status', 'unknown')
        battery_soc = record.get('battery_soc', 0)
        
        # Циклічні паттерни battery_soc
        if unit_type in ['battery', 'combined'] and battery_soc > 0:
            analysis['patterns'].append('battery_soc_cyclic')
        
        # Обмежені набори значень
        if unit_type in ['solar_roof', 'micro_wind', 'battery', 'combined']:
            analysis['patterns'].append('limited_unit_types')
        
        if status in ['generating', 'consuming', 'idle', 'maintenance']:
            analysis['patterns'].append('limited_status_values')
        
        # Географічні координати (можуть бути схожі)
        location = record.get('location', {})
        if location:
            lat = location.get('lat', 0)
            lon = location.get('lon', 0)
            if 45.0 <= lat <= 52.0 and 22.0 <= lon <= 40.0:  # Україна
                analysis['patterns'].append('geographic_clustering')
        
        return analysis
    
    def process_message(self, message: Any):
        """Обробляє одне повідомлення та збирає метрики compression"""
        try:
            # Отримуємо дані з повідомлення
            record = message.value
            compression_type = record.get('compression_test', 'unknown')
            topic_name = message.topic
            
            # Розраховуємо метрики
            latency_ms = self.calculate_message_latency(message)
            compression_data = self.calculate_compression_ratio(record, message)
            der_analysis = self.analyze_der_compression_patterns(record)
            
            # Збираємо метрики
            if compression_type not in self.compression_metrics:
                self.compression_metrics[compression_type]['start_time'] = time.time()
            
            self.compression_metrics[compression_type]['records'].append({
                'timestamp': record.get('timestamp'),
                'device_id': record.get('device_id'),
                'compression_type': compression_type,
                'topic_name': topic_name,
                'kafka_timestamp': message.timestamp,
                'partition': message.partition,
                'offset': message.offset,
                'latency_ms': latency_ms,
                'uncompressed_size': compression_data['uncompressed_size'],
                'compressed_size': compression_data['compressed_size'],
                'compression_ratio': compression_data['compression_ratio'],
                'der_analysis': der_analysis
            })
            
            self.compression_metrics[compression_type]['latencies'].append(latency_ms)
            self.compression_metrics[compression_type]['uncompressed_sizes'].append(compression_data['uncompressed_size'])
            self.compression_metrics[compression_type]['compressed_sizes'].append(compression_data['compressed_size'])
            self.compression_metrics[compression_type]['end_time'] = time.time()
            self.compression_metrics[compression_type]['compression_type'] = compression_type
            
            # Оновлюємо статистику в реальному часі
            self.realtime_stats['total_records'] += 1
            self.realtime_stats['compression_ratios'][compression_type].append(compression_data['compression_ratio'])
            
        except Exception as e:
            logger.error(f"Помилка обробки повідомлення: {e}")
    
    def calculate_compression_metrics(self, compression_type: str) -> Dict[str, Any]:
        """Розраховує метрики для конкретного алгоритму стиснення"""
        if compression_type not in self.compression_metrics:
            return {}
        
        metrics_data = self.compression_metrics[compression_type]
        records = metrics_data['records']
        latencies = metrics_data['latencies']
        uncompressed_sizes = metrics_data['uncompressed_sizes']
        compressed_sizes = metrics_data['compressed_sizes']
        
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
        
        # Compression статистика
        if uncompressed_sizes and compressed_sizes:
            avg_uncompressed_size = statistics.mean(uncompressed_sizes)
            avg_compressed_size = statistics.mean(compressed_sizes)
            total_uncompressed = sum(uncompressed_sizes)
            total_compressed = sum(compressed_sizes)
            
            if total_uncompressed > 0:
                overall_compression_ratio = (1 - (total_compressed / total_uncompressed)) * 100
            else:
                overall_compression_ratio = 0
        else:
            avg_uncompressed_size = avg_compressed_size = overall_compression_ratio = 0
        
        # Аналіз DER паттернів
        der_patterns = defaultdict(int)
        for record in records:
            if 'der_analysis' in record:
                patterns = record['der_analysis'].get('patterns', [])
                for pattern in patterns:
                    der_patterns[pattern] += 1
        
        return {
            'compression_type': compression_type,
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
            # Compression метрики
            'avg_uncompressed_size_bytes': round(avg_uncompressed_size, 0),
            'avg_compressed_size_bytes': round(avg_compressed_size, 0),
            'overall_compression_ratio_percent': round(overall_compression_ratio, 1),
            'total_uncompressed_bytes': total_uncompressed,
            'total_compressed_bytes': total_compressed,
            # DER паттерни
            'der_compression_patterns': dict(der_patterns),
            'start_time': metrics_data['start_time'],
            'end_time': metrics_data['end_time']
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
            active_tests = [comp_type for comp_type, data in self.compression_metrics.items() if data['records']]
            if active_tests:
                print(f"   Активні тести: {', '.join(active_tests)}")
                
                # Показуємо compression ratios
                for comp_type in active_tests:
                    ratios = self.realtime_stats['compression_ratios'][comp_type]
                    if ratios:
                        avg_ratio = statistics.mean(ratios[-10:])  # Останні 10 записів
                        print(f"   {comp_type}: {avg_ratio:.1f}% compression")
            
            self.realtime_stats['last_update'] = current_time
    
    def print_compression_summary(self, compression_type: str):
        """Виводить підсумок compression тесту"""
        metrics = self.calculate_compression_metrics(compression_type)
        
        if not metrics:
            print(f"❌ Немає даних для тесту {compression_type}")
            return
        
        print(f"\n📋 ПІДСУМОК COMPRESSION ТЕСТУ: {compression_type}")
        print(f"   Записів: {metrics['total_records']}")
        print(f"   Тривалість: {metrics['duration_seconds']}с")
        print(f"   Throughput: {metrics['throughput_records_per_sec']} rec/sec")
        print(f"   Avg Latency: {metrics['avg_latency_ms']} ms")
        print(f"   P50 Latency: {metrics['p50_latency_ms']} ms")
        print(f"   P95 Latency: {metrics['p95_latency_ms']} ms")
        print(f"   Compression Ratio: {metrics['overall_compression_ratio_percent']}%")
        print(f"   Uncompressed Size: {metrics['avg_uncompressed_size_bytes']} bytes")
        print(f"   Compressed Size: {metrics['avg_compressed_size_bytes']} bytes")
        
        if metrics['der_compression_patterns']:
            print(f"   DER Patterns: {metrics['der_compression_patterns']}")
    
    def consume_and_analyze(self, topics: List[str], duration_minutes: int = 15):
        """Основна функція споживання та аналізу compression"""
        logger.info(f"Початок споживання compression метрик з топіків: {topics}")
        logger.info(f"Тривалість: {duration_minutes} хвилин")
        
        # Підписуємося на топіки
        self.consumer.subscribe(topics)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        print("🚀 Compression Metrics Consumer запущено!")
        print("📡 Збираємо метрики для аналізу compression алгоритмів...")
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
                for compression_type in list(self.compression_metrics.keys()):
                    metrics_data = self.compression_metrics[compression_type]
                    if metrics_data['end_time'] and (time.time() - metrics_data['end_time']) > 10:
                        # Тест завершився більше 10 секунд тому
                        self.print_compression_summary(compression_type)
                        # Видаляємо з активних тестів
                        del self.compression_metrics[compression_type]
        
        except KeyboardInterrupt:
            print(f"\n🛑 Consumer зупинено користувачем")
        
        # Фінальний аналіз всіх тестів
        print(f"\n📊 === ФІНАЛЬНИЙ АНАЛІЗ ВСІХ COMPRESSION ТЕСТІВ ===")
        for compression_type in self.compression_metrics.keys():
            self.print_compression_summary(compression_type)
    
    def save_metrics(self, filename: str = "data/compression_metrics.json"):
        """Зберігає зібрані метрики у файл"""
        try:
            # Створюємо папку data якщо не існує
            import os
            os.makedirs("data", exist_ok=True)
            
            all_metrics = {}
            for compression_type in self.compression_metrics.keys():
                all_metrics[compression_type] = self.calculate_compression_metrics(compression_type)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_metrics, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Compression метрики збережено у файл: {filename}")
        except Exception as e:
            logger.error(f"Помилка збереження метрик: {e}")
    
    def close(self):
        """Закриття Consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("Compression Metrics Consumer закрито")

def main():
    """Основна функція"""
    print("=== COMPRESSION METRICS CONSUMER ===")
    print("Збір та аналіз метрик compression ratio та latency")
    print()
    
    consumer = CompressionMetricsConsumer()
    
    try:
        # Споживання з compression топіків
        topics = ['der-comp-none', 'der-comp-snappy', 'der-comp-lz4', 'der-comp-gzip', 'der-comp-zstd']
        
        # Запускаємо споживання на 20 хвилин (достатньо для всіх тестів)
        consumer.consume_and_analyze(topics, duration_minutes=20)
        
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
