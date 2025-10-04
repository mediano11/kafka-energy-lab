#!/usr/bin/env python3
"""
Тестовий Producer для дослідження впливу batch.size та linger.ms
Виконує тести з комбінаціями параметрів для оптимізації DER системи
"""

import json
import time
import random
import threading
import statistics
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import Dict, List, Any, Tuple
import uuid

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchTestProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація тестового Producer"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.test_results = {}
        
        # Розширені тестові комбінації для ultra-low latency та SCADA
        self.test_configs = [
            # Ultra-low latency тести (до 8KB)
            (4096, 0, "4KB_0ms"),      # Ultra-low latency
            (4096, 1, "4KB_1ms"),      # Детальний аналіз linger
            (4096, 2, "4KB_2ms"),
            (4096, 3, "4KB_3ms"),
            (4096, 5, "4KB_5ms"),
            (8192, 0, "8KB_0ms"),      # Ultra-low latency
            (8192, 1, "8KB_1ms"),      # Детальний аналіз linger
            (8192, 2, "8KB_2ms"),
            (8192, 3, "8KB_3ms"),
            (8192, 5, "8KB_5ms"),
            
            # Стандартні тести для порівняння
            (16384, 0, "16KB_0ms"),
            (16384, 10, "16KB_10ms"),
            (16384, 50, "16KB_50ms"),
            (65536, 0, "64KB_0ms"),
            (65536, 10, "64KB_10ms"),
            (65536, 50, "64KB_50ms"),
            (262144, 0, "256KB_0ms"),
            (262144, 10, "256KB_10ms"),
            (262144, 50, "256KB_50ms"),
        ]
    
    def setup_producer(self, batch_size: int, linger_ms: int) -> KafkaProducer:
        """Налаштування Producer з конкретними параметрами"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Тестові параметри
                batch_size=batch_size,
                linger_ms=linger_ms,
                compression_type='snappy',
                buffer_memory=33554432,  # 32MB
                max_block_ms=10000,
                retries=3,
                retry_backoff_ms=100,
                # Серіалізація
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Надійність
                acks='all',
                request_timeout_ms=30000,
                # Мережеві налаштування
                send_buffer_bytes=131072,
                receive_buffer_bytes=32768,
                # Kafka 3.7.1
                api_version=(3, 7, 1),
                security_protocol='PLAINTEXT',
                max_in_flight_requests_per_connection=5,
            )
            logger.info(f"Producer налаштовано: batch_size={batch_size}, linger_ms={linger_ms}")
            return producer
        except Exception as e:
            logger.error(f"Помилка налаштування Producer: {e}")
            raise
    
    def generate_test_record(self, device_num: int, test_id: str) -> Dict[str, Any]:
        """Генерує тестовий запис для DER пристрою"""
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
            "timestamp": datetime.now().isoformat(),
            "test_id": test_id,
            "message_id": str(uuid.uuid4())
        }
        
        return record
    
    def run_single_test(self, batch_size: int, linger_ms: int, config_name: str, 
                       topic_name: str = "der-batch-test", 
                       num_records: int = 1000, 
                       duration_seconds: int = 30) -> Dict[str, Any]:
        """Виконує один тест з конкретними параметрами"""
        logger.info(f"Початок тесту: {config_name}")
        logger.info(f"Параметри: batch_size={batch_size}, linger_ms={linger_ms}")
        logger.info(f"Кількість записів: {num_records}, Тривалість: {duration_seconds}с")
        
        # Налаштовуємо Producer
        producer = self.setup_producer(batch_size, linger_ms)
        
        # Метрики тесту
        start_time = time.time()
        sent_count = 0
        failed_count = 0
        latencies = []
        
        try:
            # Генеруємо та відправляємо записи
            for i in range(num_records):
                record_start_time = time.time()
                
                # Генеруємо запис
                record = self.generate_test_record(i + 1, config_name)
                device_id = record['device_id']
                
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
            logger.error(f"Помилка під час тесту {config_name}: {e}")
            # Повертаємо помилковий результат
            return {
                'config_name': config_name,
                'batch_size': batch_size,
                'linger_ms': linger_ms,
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
        
        # Результати тесту
        test_result = {
            'config_name': config_name,
            'batch_size': batch_size,
            'linger_ms': linger_ms,
            'total_records': num_records,
            'sent_count': sent_count,
            'failed_count': failed_count,
            'success_rate': (sent_count / num_records) * 100 if num_records > 0 else 0,
            'duration_seconds': total_duration,
            'throughput_records_per_sec': round(throughput, 2),
            'avg_latency_ms': round(avg_latency, 2),
            'min_latency_ms': round(min_latency, 2),
            'max_latency_ms': round(max_latency, 2),
            'p50_latency_ms': round(p50_latency, 2),
            'p95_latency_ms': round(p95_latency, 2),
            'p99_latency_ms': round(p99_latency, 2),
            'latency_std_dev': round(statistics.stdev(latencies) if len(latencies) > 1 else 0, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Тест {config_name} завершено:")
        logger.info(f"  Відправлено: {sent_count}/{num_records} ({test_result['success_rate']:.1f}%)")
        logger.info(f"  Throughput: {test_result['throughput_records_per_sec']} rec/sec")
        logger.info(f"  Avg Latency: {test_result['avg_latency_ms']} ms")
        logger.info(f"  P50 Latency: {test_result['p50_latency_ms']} ms")
        logger.info(f"  P95 Latency: {test_result['p95_latency_ms']} ms")
        logger.info(f"  P99 Latency: {test_result['p99_latency_ms']} ms")
        logger.info(f"  Latency StdDev: {test_result['latency_std_dev']} ms")
        
        return test_result
    
    def run_all_tests(self, topic_name: str = "der-batch-test", 
                     num_records: int = 1000, 
                     duration_seconds: int = 30) -> List[Dict[str, Any]]:
        """Виконує всі тести з різними комбінаціями параметрів"""
        logger.info("Початок всіх тестів batch.size та linger.ms")
        logger.info(f"Топік: {topic_name}")
        logger.info(f"Записів на тест: {num_records}")
        logger.info(f"Тривалість тесту: {duration_seconds}с")
        logger.info(f"Всього тестів: {len(self.test_configs)}")
        
        all_results = []
        
        for i, (batch_size, linger_ms, config_name) in enumerate(self.test_configs, 1):
            print(f"\n{'='*60}")
            print(f"ТЕСТ {i}/{len(self.test_configs)}: {config_name}")
            print(f"batch_size: {batch_size} bytes ({batch_size//1024}KB)")
            print(f"linger_ms: {linger_ms} ms")
            print(f"{'='*60}")
            
            try:
                result = self.run_single_test(
                    batch_size=batch_size,
                    linger_ms=linger_ms,
                    config_name=config_name,
                    topic_name=topic_name,
                    num_records=num_records,
                    duration_seconds=duration_seconds
                )
                
                all_results.append(result)
                
                # Пауза між тестами
                if i < len(self.test_configs):
                    print(f"\nПауза 5 секунд перед наступним тестом...")
                    time.sleep(5)
                
            except Exception as e:
                logger.error(f"Помилка тесту {config_name}: {e}")
                # Додаємо помилковий результат
                error_result = {
                    'config_name': config_name,
                    'batch_size': batch_size,
                    'linger_ms': linger_ms,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                all_results.append(error_result)
        
        return all_results
    
    def save_results(self, results: List[Dict[str, Any]], filename: str = "data/batch_test_results.json"):
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
        print("📊 РЕЗУЛЬТАТИ ТЕСТУВАННЯ BATCH.SIZE ТА LINGER.MS")
        print(f"{'='*100}")
        print(f"{'Конфігурація':<15} {'Records/sec':<12} {'Avg Latency':<12} {'P50 Latency':<12} {'P95 Latency':<12} {'Success Rate':<12} {'Використання':<15}")
        print(f"{'-'*120}")
        
        for result in results:
            if 'error' in result:
                print(f"{result['config_name']:<15} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<15}")
                continue
            
            config = result['config_name']
            throughput = result['throughput_records_per_sec']
            avg_latency = result['avg_latency_ms']
            p50_latency = result.get('p50_latency_ms', result['avg_latency_ms'])
            p95_latency = result['p95_latency_ms']
            success_rate = result['success_rate']
            
            # Визначаємо тип використання
            if result['batch_size'] <= 8192:
                if result['linger_ms'] == 0:
                    usage = "Ultra-low"
                elif result['linger_ms'] <= 5:
                    usage = "SCADA"
                else:
                    usage = "Low-latency"
            elif result['linger_ms'] == 0:
                usage = "Real-time"
            elif result['linger_ms'] <= 10:
                usage = "Balanced"
            else:
                usage = "Batch"
            
            print(f"{config:<15} {throughput:<12} {avg_latency:<12} {p50_latency:<12} {p95_latency:<12} {success_rate:<12.1f}% {usage:<15}")
        
        print(f"{'-'*100}")
        
        # Знаходимо найкращі результати
        valid_results = [r for r in results if 'error' not in r]
        
        if valid_results:
            max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
            min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
            
            print(f"\n🏆 НАЙКРАЩІ РЕЗУЛЬТАТИ:")
            print(f"Max throughput: {max_throughput['config_name']} → {max_throughput['throughput_records_per_sec']} rec/sec")
            print(f"Min latency: {min_latency['config_name']} → {min_latency['avg_latency_ms']} ms")
            
            # Рекомендації для DER системи та SCADA
            print(f"\n💡 РЕКОМЕНДАЦІЇ ДЛЯ DER СИСТЕМИ:")
            print(f"Для Virtual Power Plant aggregation рекомендується:")
            print(f"- Високий throughput: {max_throughput['config_name']}")
            print(f"- Низька latency: {min_latency['config_name']}")
            
            # SCADA рекомендації
            scada_results = [r for r in valid_results if r['batch_size'] <= 8192 and r['linger_ms'] <= 5]
            if scada_results:
                best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
                print(f"\n🏭 SCADA ІНТЕГРАЦІЯ:")
                print(f"- Оптимальна конфігурація: {best_scada['config_name']}")
                print(f"- P95 Latency: {best_scada['p95_latency_ms']} ms")
                print(f"- P50 Latency: {best_scada['p50_latency_ms']} ms")
                print(f"- Стандартне відхилення: {best_scada['latency_std_dev']} ms")
            
            # Ultra-low latency аналіз
            ultra_low_results = [r for r in valid_results if r['batch_size'] <= 8192 and r['linger_ms'] == 0]
            if ultra_low_results:
                best_ultra = min(ultra_low_results, key=lambda x: x['avg_latency_ms'])
                print(f"\n⚡ ULTRA-LOW LATENCY:")
                print(f"- Найкраща конфігурація: {best_ultra['config_name']}")
                print(f"- Середня latency: {best_ultra['avg_latency_ms']} ms")
                print(f"- P95 Latency: {best_ultra['p95_latency_ms']} ms")
            
            # Оптимальний баланс
            balanced_results = [r for r in valid_results if 10 <= r['linger_ms'] <= 50 and r['batch_size'] >= 65536]
            if balanced_results:
                optimal = max(balanced_results, key=lambda x: x['throughput_records_per_sec'] / max(x['avg_latency_ms'], 1))
                print(f"\n⚖️ ОПТИМАЛЬНИЙ БАЛАНС:")
                print(f"- Конфігурація: {optimal['config_name']} для aggregation 1000 DER пристроїв")
                print(f"- Throughput: {optimal['throughput_records_per_sec']} rec/sec")
                print(f"- Latency: {optimal['avg_latency_ms']} ms")

def main():
    """Основна функція для запуску тестів"""
    print("=== ТЕСТУВАННЯ BATCH.SIZE ТА LINGER.MS ===")
    print("Дослідження впливу параметрів на latency та throughput")
    print("для DER системи з 1000 пристроїв")
    print()
    
    tester = BatchTestProducer()
    
    try:
        # Запускаємо всі тести
        results = tester.run_all_tests(
            topic_name="der-batch-test",
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
