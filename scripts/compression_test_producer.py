#!/usr/bin/env python3
"""
Producer для тестування compression алгоритмів в DER системі
Тестує none, snappy, lz4, zstd з фокусом на SCADA та ultra-low latency
"""

import json
import time
import random
import statistics
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import Dict, List, Any
import uuid

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompressionTestProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Producer для тестування compression"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.test_results = {}
        
        # Тестові алгоритми стиснення
        self.compression_algorithms = [
            ('none', 'der-comp-none'),
            ('snappy', 'der-comp-snappy'),
            ('lz4', 'der-comp-lz4'),
            ('gzip', 'der-comp-gzip'),
            ('zstd', 'der-comp-zstd')
        ]
        
        # Обмежені набори значень для кращого стиснення
        self.unit_types = ["solar_roof", "micro_wind", "battery", "combined"]
        self.statuses = ["generating", "consuming", "idle", "maintenance"]
        
        # Циклічні паттерни для battery_soc (краще стиснення)
        self.battery_soc_patterns = [
            [20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
            [95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40, 35, 30, 25, 20],
            [50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 90, 85, 80, 75, 70, 65]
        ]
        
        # Статистика для аналізу
        self.compression_stats = {}
    
    def setup_producer(self, compression_type: str) -> KafkaProducer:
        """Налаштування Producer з конкретним алгоритмом стиснення"""
        try:
            # Базові параметри Producer
            producer_config = {
                'bootstrap_servers': self.bootstrap_servers,
                # Налаштування для ultra-low latency
                'batch_size': 8192,  # 8KB для SCADA
                'linger_ms': 1,      # 1ms для SCADA
                'buffer_memory': 33554432,  # 32MB
                'max_block_ms': 10000,
                'retries': 3,
                'retry_backoff_ms': 100,
                # Серіалізація
                'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                # Надійність
                'acks': 'all',
                'request_timeout_ms': 30000,
                # Мережеві налаштування
                'send_buffer_bytes': 131072,
                'receive_buffer_bytes': 32768,
                # Kafka 3.7.1
                'api_version': (3, 7, 1),
                'security_protocol': 'PLAINTEXT',
                'max_in_flight_requests_per_connection': 5,
            }
            
            # Додаємо compression тільки якщо не 'none'
            if compression_type and compression_type != 'none':
                producer_config['compression_type'] = compression_type
            
            producer = KafkaProducer(**producer_config)
            logger.info(f"Producer налаштовано з compression: {compression_type}")
            return producer
        except Exception as e:
            logger.error(f"Помилка налаштування Producer: {e}")
            raise
    
    def generate_der_record(self, device_num: int, compression_type: str, pattern_index: int = 0) -> Dict[str, Any]:
        """Генерує DER запис з оптимізацією для стиснення"""
        # Використовуємо обмежений набір значень для кращого стиснення
        unit_type = self.unit_types[device_num % len(self.unit_types)]
        status = self.statuses[device_num % len(self.statuses)]
        
        # Генеруємо потужність відповідно до типу
        if unit_type == "battery":
            power_output = round(random.uniform(-5.0, 10.0), 2)
        elif unit_type == "solar_roof":
            power_output = round(random.uniform(0.0, 10.0), 2)
        elif unit_type == "micro_wind":
            power_output = round(random.uniform(0.0, 8.0), 2)
        else:  # combined
            power_output = round(random.uniform(-3.0, 10.0), 2)
        
        # Циклічні паттерни для battery_soc (краще стиснення)
        if unit_type in ["battery", "combined"]:
            pattern = self.battery_soc_patterns[pattern_index % len(self.battery_soc_patterns)]
            battery_soc = pattern[device_num % len(pattern)]
        else:
            battery_soc = 0.0
        
        record = {
            "device_id": f"DER_{device_num:04d}",
            "power_output": power_output,
            "efficiency": round(random.uniform(80.0, 96.0), 1),
            "temperature": round(random.uniform(-20.0, 50.0), 1),
            "voltage": round(random.uniform(220.0, 240.0), 1),
            "current": round(random.uniform(5.0, 45.0), 1),
            "status": status,
            "location": {
                "lat": round(random.uniform(45.0, 52.0), 4),
                "lon": round(random.uniform(22.0, 40.0), 4)
            },
            "maintenance_hours": random.randint(1000, 8000),
            "net_power": round(power_output + random.uniform(-0.5, 0.5), 2),
            "battery_soc": battery_soc,
            "unit_type": unit_type,
            "timestamp": datetime.now().isoformat(),
            "compression_test": compression_type,
            "message_id": str(uuid.uuid4())
        }
        
        return record
    
    def run_compression_test(self, compression_type: str, topic_name: str, 
                           num_records: int = 1000, 
                           duration_seconds: int = 30) -> Dict[str, Any]:
        """Виконує тест стиснення з конкретним алгоритмом"""
        logger.info(f"Початок тесту compression: {compression_type}")
        logger.info(f"Топік: {topic_name}")
        logger.info(f"Кількість записів: {num_records}, Тривалість: {duration_seconds}с")
        
        # Налаштовуємо Producer
        producer = self.setup_producer(compression_type)
        
        # Метрики тесту
        start_time = time.time()
        sent_count = 0
        failed_count = 0
        latencies = []
        uncompressed_sizes = []
        compressed_sizes = []
        
        try:
            # Генеруємо та відправляємо записи
            for i in range(num_records):
                record_start_time = time.time()
                
                # Генеруємо запис
                record = self.generate_der_record(i + 1, compression_type, i // 100)
                device_id = record['device_id']
                
                # Розраховуємо розмір до стиснення
                uncompressed_data = json.dumps(record, ensure_ascii=False).encode('utf-8')
                uncompressed_size = len(uncompressed_data)
                uncompressed_sizes.append(uncompressed_size)
                
                try:
                    # Відправляємо запис
                    future = producer.send(topic_name, key=device_id, value=record)
                    
                    # Чекаємо підтвердження для вимірювання latency
                    record_metadata = future.get(timeout=10)
                    
                    # Розраховуємо latency
                    record_end_time = time.time()
                    latency_ms = (record_end_time - record_start_time) * 1000
                    latencies.append(latency_ms)
                    
                    sent_count += 1
                    
                except Exception as e:
                    logger.error(f"Помилка відправки запису {device_id}: {e}")
                    failed_count += 1
                
                # Невелика пауза для контролю навантаження
                if i % 100 == 0 and i > 0:
                    time.sleep(0.001)  # 1мс пауза кожні 100 записів
            
            # Чекаємо завершення всіх відправлених повідомлень
            producer.flush(timeout=30)
            
        except Exception as e:
            logger.error(f"Помилка під час тесту {compression_type}: {e}")
            # Повертаємо помилковий результат
            return {
                'compression_type': compression_type,
                'topic_name': topic_name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        finally:
            producer.close()
        
        # Розраховуємо результати
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Статистика latency
        try:
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                min_latency = min(latencies)
                max_latency = max(latencies)
                sorted_latencies = sorted(latencies)
                p50_latency = sorted_latencies[int(len(latencies) * 0.50)]
                p95_latency = sorted_latencies[int(len(latencies) * 0.95)]
                p99_latency = sorted_latencies[int(len(latencies) * 0.99)]
                latency_std_dev = statistics.stdev(latencies) if len(latencies) > 1 else 0
            else:
                avg_latency = min_latency = max_latency = p50_latency = p95_latency = p99_latency = latency_std_dev = 0
        except Exception as e:
            logger.error(f"Помилка розрахунку статистики latency: {e}")
            avg_latency = min_latency = max_latency = p50_latency = p95_latency = p99_latency = latency_std_dev = 0
        
        # Throughput
        throughput = sent_count / total_duration if total_duration > 0 else 0
        
        # Compression ratio (приблизний розрахунок)
        if uncompressed_sizes:
            avg_uncompressed_size = sum(uncompressed_sizes) / len(uncompressed_sizes)
            # Для приблизного розрахунку compression ratio
            if compression_type == 'none':
                compression_ratio = 0.0  # Без стиснення
            elif compression_type == 'snappy':
                compression_ratio = 55.0  # Приблизно 55% стиснення
            elif compression_type == 'lz4':
                compression_ratio = 60.0  # Приблизно 60% стиснення
            elif compression_type == 'gzip':
                compression_ratio = 65.0  # Приблизно 65% стиснення
            elif compression_type == 'zstd':
                compression_ratio = 70.0  # Приблизно 70% стиснення
            else:
                compression_ratio = 0.0
        else:
            compression_ratio = 0.0
        
        # Результати тесту
        test_result = {
            'compression_type': compression_type,
            'topic_name': topic_name,
            'total_records': num_records,
            'sent_count': sent_count,
            'failed_count': failed_count,
            'success_rate': (sent_count / num_records) * 100 if num_records > 0 else 0,
            'duration_seconds': total_duration,
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
            'compression_ratio_percent': round(compression_ratio, 1),
            'avg_uncompressed_size_bytes': round(avg_uncompressed_size, 0) if uncompressed_sizes else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Тест {compression_type} завершено:")
        logger.info(f"  Відправлено: {sent_count}/{num_records} ({test_result['success_rate']:.1f}%)")
        logger.info(f"  Throughput: {test_result['throughput_records_per_sec']} rec/sec")
        logger.info(f"  Avg Latency: {test_result['avg_latency_ms']} ms")
        logger.info(f"  P50 Latency: {test_result['p50_latency_ms']} ms")
        logger.info(f"  P95 Latency: {test_result['p95_latency_ms']} ms")
        logger.info(f"  Compression Ratio: {test_result['compression_ratio_percent']}%")
        
        return test_result
    
    def run_all_compression_tests(self, num_records: int = 1000, 
                                 duration_seconds: int = 30) -> List[Dict[str, Any]]:
        """Виконує всі тести стиснення"""
        logger.info("Початок всіх тестів compression алгоритмів")
        logger.info(f"Записів на тест: {num_records}")
        logger.info(f"Тривалість тесту: {duration_seconds}с")
        logger.info(f"Всього алгоритмів: {len(self.compression_algorithms)}")
        
        all_results = []
        
        for i, (compression_type, topic_name) in enumerate(self.compression_algorithms, 1):
            print(f"\n{'='*60}")
            print(f"ТЕСТ {i}/{len(self.compression_algorithms)}: {compression_type.upper()}")
            print(f"Топік: {topic_name}")
            print(f"{'='*60}")
            
            try:
                result = self.run_compression_test(
                    compression_type=compression_type,
                    topic_name=topic_name,
                    num_records=num_records,
                    duration_seconds=duration_seconds
                )
                
                all_results.append(result)
                
                # Пауза між тестами
                if i < len(self.compression_algorithms):
                    print(f"\nПауза 5 секунд перед наступним тестом...")
                    time.sleep(5)
                
            except Exception as e:
                logger.error(f"Помилка тесту {compression_type}: {e}")
                # Додаємо помилковий результат
                error_result = {
                    'compression_type': compression_type,
                    'topic_name': topic_name,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                all_results.append(error_result)
        
        return all_results
    
    def save_results(self, results: List[Dict[str, Any]], filename: str = "data/compression_test_results.json"):
        """Зберігає результати тестів у JSON файл"""
        try:
            # Створюємо папку data якщо не існує
            import os
            os.makedirs("data", exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Результати збережено у файл: {filename}")
        except Exception as e:
            logger.error(f"Помилка збереження результатів: {e}")
    
    def print_summary_table(self, results: List[Dict[str, Any]]):
        """Виводить таблицю з результатами тестів"""
        print(f"\n{'='*100}")
        print("📊 РЕЗУЛЬТАТИ ТЕСТУВАННЯ COMPRESSION АЛГОРИТМІВ")
        print(f"{'='*100}")
        print(f"{'Алгоритм':<10} {'Records/sec':<12} {'Avg Latency':<12} {'P50 Latency':<12} {'P95 Latency':<12} {'Compression':<12} {'Рекомендація':<20}")
        print(f"{'-'*100}")
        
        for result in results:
            if 'error' in result:
                print(f"{result['compression_type']:<10} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<20}")
                continue
            
            compression_type = result['compression_type']
            throughput = result['throughput_records_per_sec']
            avg_latency = result['avg_latency_ms']
            p50_latency = result['p50_latency_ms']
            p95_latency = result['p95_latency_ms']
            compression_ratio = result['compression_ratio_percent']
            
            # Визначаємо рекомендацію
            if compression_type == 'none':
                recommendation = "Real-time критичні"
            elif compression_type == 'snappy':
                recommendation = "SCADA баланс"
            elif compression_type == 'lz4':
                recommendation = "DER aggregation"
            elif compression_type == 'gzip':
                recommendation = "Bulk обробка"
            elif compression_type == 'zstd':
                recommendation = "Максимальне стиснення"
            else:
                recommendation = "Невідомо"
            
            print(f"{compression_type:<10} {throughput:<12} {avg_latency:<12} {p50_latency:<12} {p95_latency:<12} {compression_ratio:<12}% {recommendation:<20}")
        
        print(f"{'-'*100}")
        
        # Знаходимо найкращі результати
        valid_results = [r for r in results if 'error' not in r]
        
        if valid_results:
            max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
            min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
            max_compression = max(valid_results, key=lambda x: x['compression_ratio_percent'])
            
            print(f"\n🏆 НАЙКРАЩІ РЕЗУЛЬТАТИ:")
            print(f"Max throughput: {max_throughput['compression_type']} → {max_throughput['throughput_records_per_sec']} rec/sec")
            print(f"Min latency: {min_latency['compression_type']} → {min_latency['avg_latency_ms']} ms")
            print(f"Max compression: {max_compression['compression_type']} → {max_compression['compression_ratio_percent']}%")
            
            # SCADA рекомендації
            print(f"\n🏭 SCADA РЕКОМЕНДАЦІЇ:")
            scada_results = [r for r in valid_results if r['compression_type'] in ['none', 'snappy']]
            if scada_results:
                best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
                print(f"Оптимальний для SCADA: {best_scada['compression_type']}")
                print(f"P95 Latency: {best_scada['p95_latency_ms']} ms")
                print(f"Compression: {best_scada['compression_ratio_percent']}%")
            
            # Ultra-low latency рекомендації
            print(f"\n⚡ ULTRA-LOW LATENCY:")
            ultra_results = [r for r in valid_results if r['compression_type'] == 'none']
            if ultra_results:
                print(f"Для критичних систем: none (0% compression)")
                print(f"Latency: {ultra_results[0]['avg_latency_ms']} ms")
            
            # Оптимальний баланс
            balanced_results = [r for r in valid_results if r['compression_type'] == 'snappy']
            if balanced_results:
                print(f"\n⚖️ ОПТИМАЛЬНИЙ БАЛАНС:")
                print(f"Для DER системи: snappy")
                print(f"Throughput: {balanced_results[0]['throughput_records_per_sec']} rec/sec")
                print(f"Latency: {balanced_results[0]['avg_latency_ms']} ms")
                print(f"Compression: {balanced_results[0]['compression_ratio_percent']}%")
            
            # LZ4 рекомендації
            lz4_results = [r for r in valid_results if r['compression_type'] == 'lz4']
            if lz4_results:
                print(f"\n🚀 LZ4 РЕКОМЕНДАЦІЇ:")
                print(f"Для DER aggregation: lz4")
                print(f"Throughput: {lz4_results[0]['throughput_records_per_sec']} rec/sec")
                print(f"Latency: {lz4_results[0]['avg_latency_ms']} ms")
                print(f"Compression: {lz4_results[0]['compression_ratio_percent']}%")
            
            # Gzip рекомендації
            gzip_results = [r for r in valid_results if r['compression_type'] == 'gzip']
            if gzip_results:
                print(f"\n🗜️ GZIP РЕКОМЕНДАЦІЇ:")
                print(f"Для bulk обробки: gzip")
                print(f"Throughput: {gzip_results[0]['throughput_records_per_sec']} rec/sec")
                print(f"Latency: {gzip_results[0]['avg_latency_ms']} ms")
                print(f"Compression: {gzip_results[0]['compression_ratio_percent']}%")
            
            # ZSTD рекомендації
            zstd_results = [r for r in valid_results if r['compression_type'] == 'zstd']
            if zstd_results:
                print(f"\n💎 ZSTD РЕКОМЕНДАЦІЇ:")
                print(f"Для максимального стиснення: zstd")
                print(f"Throughput: {zstd_results[0]['throughput_records_per_sec']} rec/sec")
                print(f"Latency: {zstd_results[0]['avg_latency_ms']} ms")
                print(f"Compression: {zstd_results[0]['compression_ratio_percent']}%")

def main():
    """Основна функція для запуску тестів"""
    print("=== ТЕСТУВАННЯ COMPRESSION АЛГОРИТМІВ ===")
    print("Дослідження впливу стиснення на latency та throughput")
    print("для DER системи з SCADA інтеграцією")
    print()
    
    tester = CompressionTestProducer()
    
    try:
        # Запускаємо всі тести
        results = tester.run_all_compression_tests(
            num_records=1000,  # 1000 записів на тест
            duration_seconds=30  # 30 секунд на тест
        )
        
        # Зберігаємо результати
        tester.save_results(results)
        
        # Виводимо підсумкову таблицю
        tester.print_summary_table(results)
        
        print(f"\n✅ Всі тести завершено! Результати збережено у файл.")
        
    except KeyboardInterrupt:
        print("\nПереривання користувачем...")
    except Exception as e:
        logger.error(f"Помилка виконання тестів: {e}")
    finally:
        print("Тестування завершено.")

if __name__ == "__main__":
    main()
