#!/usr/bin/env python3
"""
Producer для тестування партиціонування в DER системі
Тестує різні кількості партицій (10, 15, 20) з різними стратегіями партиціонування
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
import hashlib

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PartitioningTestProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація Producer для тестування партиціонування"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.test_results = {}
        
        # Тестові конфігурації партицій
        self.partition_configs = [
            (10, 'der-part-10'),
            (15, 'der-part-15'),
            (20, 'der-part-20')
        ]
        
        # Стратегії партиціонування
        self.partitioning_strategies = [
            'unit_type',      # Партиціонування по типу пристрою
            'geographic',     # Географічне партиціонування
            'round_robin'     # Кругова стратегія
        ]
        
        # Обмежені набори значень для кращого партиціонування
        self.unit_types = ["solar_roof", "micro_wind", "battery", "combined"]
        self.statuses = ["generating", "consuming", "idle", "maintenance"]
        
        # Географічні регіони України для партиціонування
        self.geographic_regions = [
            {"name": "kyiv", "lat_range": (50.0, 51.0), "lon_range": (30.0, 31.0)},
            {"name": "lviv", "lat_range": (49.5, 50.0), "lon_range": (23.5, 24.5)},
            {"name": "kharkiv", "lat_range": (49.8, 50.2), "lon_range": (36.0, 36.5)},
            {"name": "odessa", "lat_range": (46.0, 47.0), "lon_range": (30.0, 31.0)},
            {"name": "dnipro", "lat_range": (48.0, 49.0), "lon_range": (35.0, 36.0)}
        ]
        
        # Статистика для аналізу
        self.partitioning_stats = {}
    
    def setup_producer(self) -> KafkaProducer:
        """Налаштування Producer для тестування партиціонування"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Налаштування для ultra-low latency
                batch_size=8192,  # 8KB для SCADA
                linger_ms=1,      # 1ms для SCADA
                compression_type='snappy',  # Використовуємо snappy для балансу
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
            logger.info("Producer налаштовано для тестування партиціонування")
            return producer
        except Exception as e:
            logger.error(f"Помилка налаштування Producer: {e}")
            raise
    
    def calculate_partition_key(self, record: Dict[str, Any], strategy: str, num_partitions: int) -> str:
        """Розраховує ключ партиції відповідно до стратегії"""
        if strategy == 'unit_type':
            # Партиціонування по типу пристрою
            unit_type = record.get('unit_type', 'unknown')
            return f"unit_{unit_type}"
        
        elif strategy == 'geographic':
            # Географічне партиціонування
            location = record.get('location', {})
            lat = location.get('lat', 50.0)
            lon = location.get('lon', 30.0)
            
            # Визначаємо регіон
            region = "default"
            for geo_region in self.geographic_regions:
                lat_min, lat_max = geo_region['lat_range']
                lon_min, lon_max = geo_region['lon_range']
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    region = geo_region['name']
                    break
            
            return f"geo_{region}"
        
        elif strategy == 'round_robin':
            # Кругова стратегія (використовуємо device_id)
            device_id = record.get('device_id', 'unknown')
            return f"rr_{device_id}"
        
        else:
            # За замовчуванням - по device_id
            device_id = record.get('device_id', 'unknown')
            return f"default_{device_id}"
    
    def generate_der_record(self, device_num: int, strategy: str) -> Dict[str, Any]:
        """Генерує DER запис з оптимізацією для партиціонування"""
        # Використовуємо обмежений набір значень для кращого партиціонування
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
        
        # Генеруємо географічні координати для партиціонування
        if strategy == 'geographic':
            # Вибираємо випадковий регіон
            region = random.choice(self.geographic_regions)
            lat = round(random.uniform(*region['lat_range']), 4)
            lon = round(random.uniform(*region['lon_range']), 4)
        else:
            # Загальні координати України
            lat = round(random.uniform(45.0, 52.0), 4)
            lon = round(random.uniform(22.0, 40.0), 4)
        
        # Battery SOC для пристроїв з батареєю
        if unit_type in ["battery", "combined"]:
            battery_soc = round(random.uniform(20.0, 95.0), 1)
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
                "lat": lat,
                "lon": lon
            },
            "maintenance_hours": random.randint(1000, 8000),
            "net_power": round(power_output + random.uniform(-0.5, 0.5), 2),
            "battery_soc": battery_soc,
            "unit_type": unit_type,
            "timestamp": datetime.now().isoformat(),
            "partitioning_test": strategy,
            "message_id": str(uuid.uuid4())
        }
        
        return record
    
    def run_partitioning_test(self, num_partitions: int, topic_name: str, 
                            strategy: str, num_records: int = 1000, 
                            duration_seconds: int = 30) -> Dict[str, Any]:
        """Виконує тест партиціонування з конкретною кількістю партицій"""
        logger.info(f"Початок тесту партиціонування: {num_partitions} партицій, стратегія: {strategy}")
        logger.info(f"Топік: {topic_name}")
        logger.info(f"Кількість записів: {num_records}, Тривалість: {duration_seconds}с")
        
        # Налаштовуємо Producer
        producer = self.setup_producer()
        
        # Метрики тесту
        start_time = time.time()
        sent_count = 0
        failed_count = 0
        latencies = []
        partition_distribution = {}
        
        try:
            # Генеруємо та відправляємо записи
            for i in range(num_records):
                record_start_time = time.time()
                
                # Генеруємо запис
                record = self.generate_der_record(i + 1, strategy)
                device_id = record['device_id']
                
                # Розраховуємо ключ партиції
                partition_key = self.calculate_partition_key(record, strategy, num_partitions)
                
                try:
                    # Відправляємо запис з ключем партиції
                    future = producer.send(topic_name, key=partition_key, value=record)
                    
                    # Чекаємо підтвердження для вимірювання latency
                    record_metadata = future.get(timeout=10)
                    
                    # Розраховуємо latency
                    record_end_time = time.time()
                    latency_ms = (record_end_time - record_start_time) * 1000
                    latencies.append(latency_ms)
                    
                    # Відстежуємо розподіл по партиціях
                    partition_id = record_metadata.partition
                    partition_distribution[partition_id] = partition_distribution.get(partition_id, 0) + 1
                    
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
            logger.error(f"Помилка під час тесту {num_partitions} партицій: {e}")
            # Повертаємо помилковий результат
            return {
                'num_partitions': num_partitions,
                'topic_name': topic_name,
                'strategy': strategy,
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
        
        # Аналіз розподілу по партиціях
        partition_balance = self.analyze_partition_distribution(partition_distribution, num_partitions)
        
        # Результати тесту
        test_result = {
            'num_partitions': num_partitions,
            'topic_name': topic_name,
            'strategy': strategy,
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
            # Партиціонування метрики
            'partition_distribution': partition_distribution,
            'partition_balance_score': partition_balance['balance_score'],
            'partition_utilization': partition_balance['utilization'],
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Тест {num_partitions} партицій завершено:")
        logger.info(f"  Відправлено: {sent_count}/{num_records} ({test_result['success_rate']:.1f}%)")
        logger.info(f"  Throughput: {test_result['throughput_records_per_sec']} rec/sec")
        logger.info(f"  Avg Latency: {test_result['avg_latency_ms']} ms")
        logger.info(f"  P50 Latency: {test_result['p50_latency_ms']} ms")
        logger.info(f"  P95 Latency: {test_result['p95_latency_ms']} ms")
        logger.info(f"  Partition Balance: {test_result['partition_balance_score']:.2f}")
        
        return test_result
    
    def analyze_partition_distribution(self, partition_distribution: Dict[int, int], num_partitions: int) -> Dict[str, Any]:
        """Аналізує розподіл записів по партиціях"""
        if not partition_distribution:
            return {'balance_score': 0.0, 'utilization': 0.0}
        
        # Розраховуємо статистику розподілу
        partition_counts = list(partition_distribution.values())
        total_records = sum(partition_counts)
        
        if total_records == 0:
            return {'balance_score': 0.0, 'utilization': 0.0}
        
        # Ідеальний розподіл (рівномірний)
        ideal_count_per_partition = total_records / num_partitions
        
        # Розраховуємо баланс (1.0 = ідеальний баланс, 0.0 = повна нерівномірність)
        variance = statistics.variance(partition_counts) if len(partition_counts) > 1 else 0
        balance_score = max(0, 1 - (variance / (ideal_count_per_partition ** 2)))
        
        # Утилізація партицій (скільки партицій використовується)
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
    
    def run_all_partitioning_tests(self, num_records: int = 1000, 
                                 duration_seconds: int = 30) -> List[Dict[str, Any]]:
        """Виконує всі тести партиціонування"""
        logger.info("Початок всіх тестів партиціонування")
        logger.info(f"Записів на тест: {num_records}")
        logger.info(f"Тривалість тесту: {duration_seconds}с")
        logger.info(f"Всього конфігурацій: {len(self.partition_configs)}")
        
        all_results = []
        
        for i, (num_partitions, topic_name) in enumerate(self.partition_configs, 1):
            print(f"\n{'='*60}")
            print(f"ТЕСТ {i}/{len(self.partition_configs)}: {num_partitions} ПАРТИЦІЙ")
            print(f"Топік: {topic_name}")
            print(f"{'='*60}")
            
            # Тестуємо кожну стратегію партиціонування
            for strategy in self.partitioning_strategies:
                print(f"\n--- Стратегія: {strategy} ---")
                
                try:
                    result = self.run_partitioning_test(
                        num_partitions=num_partitions,
                        topic_name=topic_name,
                        strategy=strategy,
                        num_records=num_records,
                        duration_seconds=duration_seconds
                    )
                    
                    all_results.append(result)
                    
                    # Пауза між тестами
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Помилка тесту {num_partitions} партицій, стратегія {strategy}: {e}")
                    # Додаємо помилковий результат
                    error_result = {
                        'num_partitions': num_partitions,
                        'topic_name': topic_name,
                        'strategy': strategy,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    }
                    all_results.append(error_result)
            
            # Пауза між конфігураціями
            if i < len(self.partition_configs):
                print(f"\nПауза 5 секунд перед наступною конфігурацією...")
                time.sleep(5)
        
        return all_results
    
    def save_results(self, results: List[Dict[str, Any]], filename: str = "data/partitioning_test_results.json"):
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
        print(f"\n{'='*120}")
        print("📊 РЕЗУЛЬТАТИ ТЕСТУВАННЯ ПАРТИЦІОНУВАННЯ")
        print(f"{'='*120}")
        print(f"{'Партиції':<10} {'Стратегія':<12} {'Records/sec':<12} {'Avg Latency':<12} {'P50 Latency':<12} {'P95 Latency':<12} {'Balance':<10} {'Використання':<15}")
        print(f"{'-'*120}")
        
        for result in results:
            if 'error' in result:
                print(f"{result['num_partitions']:<10} {result['strategy']:<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<12} {'ERROR':<10} {'ERROR':<15}")
                continue
            
            num_partitions = result['num_partitions']
            strategy = result['strategy']
            throughput = result['throughput_records_per_sec']
            avg_latency = result['avg_latency_ms']
            p50_latency = result['p50_latency_ms']
            p95_latency = result['p95_latency_ms']
            balance_score = result['partition_balance_score']
            
            # Визначаємо використання
            if strategy == 'unit_type':
                usage = "DER aggregation"
            elif strategy == 'geographic':
                usage = "Local grid"
            elif strategy == 'round_robin':
                usage = "Load balancing"
            else:
                usage = "Невідомо"
            
            print(f"{num_partitions:<10} {strategy:<12} {throughput:<12} {avg_latency:<12} {p50_latency:<12} {p95_latency:<12} {balance_score:<10.2f} {usage:<15}")
        
        print(f"{'-'*120}")
        
        # Знаходимо найкращі результати
        valid_results = [r for r in results if 'error' not in r]
        
        if valid_results:
            max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
            min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
            best_balance = max(valid_results, key=lambda x: x['partition_balance_score'])
            
            print(f"\n🏆 НАЙКРАЩІ РЕЗУЛЬТАТИ:")
            print(f"Max throughput: {max_throughput['num_partitions']} партицій, {max_throughput['strategy']} → {max_throughput['throughput_records_per_sec']} rec/sec")
            print(f"Min latency: {min_latency['num_partitions']} партицій, {min_latency['strategy']} → {min_latency['avg_latency_ms']} ms")
            print(f"Best balance: {best_balance['num_partitions']} партицій, {best_balance['strategy']} → {best_balance['partition_balance_score']:.2f}")
            
            # Аналіз масштабування
            self.analyze_scaling(results)
            
            # Рекомендації
            self.print_recommendations(results)
    
    def analyze_scaling(self, results: List[Dict[str, Any]]):
        """Аналізує масштабування по кількості партицій"""
        print(f"\n📈 АНАЛІЗ МАСШТАБУВАННЯ:")
        
        # Групуємо результати по кількості партицій
        by_partitions = {}
        for result in results:
            if 'error' not in result:
                num_partitions = result['num_partitions']
                if num_partitions not in by_partitions:
                    by_partitions[num_partitions] = []
                by_partitions[num_partitions].append(result)
        
        # Знаходимо baseline (найменша кількість партицій)
        baseline_partitions = min(by_partitions.keys()) if by_partitions else 0
        baseline_throughput = 0
        
        if baseline_partitions in by_partitions:
            # Беремо середній throughput для baseline
            baseline_results = by_partitions[baseline_partitions]
            baseline_throughput = sum(r['throughput_records_per_sec'] for r in baseline_results) / len(baseline_results)
        
        print(f"Baseline: {baseline_partitions} партицій → {baseline_throughput:.2f} rec/sec")
        
        # Розраховуємо scaling factor для кожної кількості партицій
        for num_partitions in sorted(by_partitions.keys()):
            if num_partitions == baseline_partitions:
                continue
            
            results_for_partitions = by_partitions[num_partitions]
            avg_throughput = sum(r['throughput_records_per_sec'] for r in results_for_partitions) / len(results_for_partitions)
            
            if baseline_throughput > 0:
                scaling_factor = avg_throughput / baseline_throughput
                efficiency = "Відмінно" if scaling_factor >= 1.5 else "Добре" if scaling_factor >= 1.2 else "Задовільно" if scaling_factor >= 1.0 else "Погано"
            else:
                scaling_factor = 0
                efficiency = "Невідомо"
            
            print(f"{num_partitions} партицій → {avg_throughput:.2f} rec/sec (Scaling: {scaling_factor:.2f}x, {efficiency})")
    
    def print_recommendations(self, results: List[Dict[str, Any]]):
        """Виводить рекомендації на основі результатів"""
        print(f"\n🎯 РЕКОМЕНДАЦІЇ:")
        
        valid_results = [r for r in results if 'error' not in r]
        if not valid_results:
            print("Немає валідних результатів для рекомендацій")
            return
        
        # Рекомендації по стратегіях
        print(f"\n📋 СТРАТЕГІЇ ПАРТИЦІОНУВАННЯ:")
        
        # Unit type стратегія
        unit_type_results = [r for r in valid_results if r['strategy'] == 'unit_type']
        if unit_type_results:
            best_unit_type = max(unit_type_results, key=lambda x: x['throughput_records_per_sec'])
            print(f"• Unit Type: {best_unit_type['num_partitions']} партицій для DER aggregation")
            print(f"  Throughput: {best_unit_type['throughput_records_per_sec']} rec/sec")
            print(f"  Balance: {best_unit_type['partition_balance_score']:.2f}")
        
        # Geographic стратегія
        geo_results = [r for r in valid_results if r['strategy'] == 'geographic']
        if geo_results:
            best_geo = max(geo_results, key=lambda x: x['throughput_records_per_sec'])
            print(f"• Geographic: {best_geo['num_partitions']} партицій для Local grid support")
            print(f"  Throughput: {best_geo['throughput_records_per_sec']} rec/sec")
            print(f"  Balance: {best_geo['partition_balance_score']:.2f}")
        
        # Round robin стратегія
        rr_results = [r for r in valid_results if r['strategy'] == 'round_robin']
        if rr_results:
            best_rr = max(rr_results, key=lambda x: x['throughput_records_per_sec'])
            print(f"• Round Robin: {best_rr['num_partitions']} партицій для Load balancing")
            print(f"  Throughput: {best_rr['throughput_records_per_sec']} rec/sec")
            print(f"  Balance: {best_rr['partition_balance_score']:.2f}")
        
        # Загальні рекомендації
        print(f"\n🏭 SCADA РЕКОМЕНДАЦІЇ:")
        scada_results = [r for r in valid_results if r['p95_latency_ms'] <= 10]  # P95 < 10ms
        if scada_results:
            best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
            print(f"Оптимальний для SCADA: {best_scada['num_partitions']} партицій, {best_scada['strategy']}")
            print(f"P95 Latency: {best_scada['p95_latency_ms']} ms")
            print(f"Throughput: {best_scada['throughput_records_per_sec']} rec/sec")
        
        print(f"\n⚡ ULTRA-LOW LATENCY:")
        ultra_results = [r for r in valid_results if r['avg_latency_ms'] <= 5]  # Avg < 5ms
        if ultra_results:
            best_ultra = min(ultra_results, key=lambda x: x['avg_latency_ms'])
            print(f"Для критичних систем: {best_ultra['num_partitions']} партицій, {best_ultra['strategy']}")
            print(f"Avg Latency: {best_ultra['avg_latency_ms']} ms")
        else:
            print("Рекомендується мінімальна кількість партицій для найнижчої latency")
        
        print(f"\n⚖️ ОПТИМАЛЬНИЙ БАЛАНС:")
        # Знаходимо найкращий баланс throughput/latency
        balanced_results = []
        for result in valid_results:
            # Простий score: throughput / latency (вищий = кращий)
            if result['avg_latency_ms'] > 0:
                balance_score = result['throughput_records_per_sec'] / result['avg_latency_ms']
                balanced_results.append((result, balance_score))
        
        if balanced_results:
            best_balanced = max(balanced_results, key=lambda x: x[1])[0]
            print(f"Для DER системи: {best_balanced['num_partitions']} партицій, {best_balanced['strategy']}")
            print(f"Throughput: {best_balanced['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {best_balanced['avg_latency_ms']} ms")
            print(f"Balance Score: {best_balanced['partition_balance_score']:.2f}")

def main():
    """Основна функція для запуску тестів"""
    print("=== ТЕСТУВАННЯ ПАРТИЦІОНУВАННЯ ===")
    print("Дослідження впливу кількості партицій на throughput та latency")
    print("для DER системи з різними стратегіями партиціонування")
    print()
    
    tester = PartitioningTestProducer()
    
    try:
        # Запускаємо всі тести
        results = tester.run_all_partitioning_tests(
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
