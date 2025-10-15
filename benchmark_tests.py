#!/usr/bin/env python3
"""
Тести продуктивності для лабораторної роботи 3
Варіант 8: Розподілені енергетичні ресурси (DER)

Виконує benchmark тести для трьох схем:
1. Simple Wide Row
2. Hourly Bucketing  
3. Daily Bucketing + Pre-aggregation

Тестує 4 типи запитів:
1. Latest Data - останні 100 записів
2. Time Range - дані за 6 годин
3. Daily Aggregation - агрегація за день
4. Filtered Query - фільтрація з Materialized Views
"""

import time
import statistics
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import logging
import random

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DERBenchmark:
    def __init__(self, hosts=['127.0.0.1'], port=9042):
        """Ініціалізація benchmark тестів"""
        self.hosts = hosts
        self.port = port
        self.cluster = None
        self.session = None
        
        # Результати тестів
        self.results = {
            'simple': {},
            'hourly': {},
            'daily': {}
        }
        
        # Параметри тестування
        self.iterations = 100  # Кількість ітерацій для кожного тесту
        self.test_devices = ['DER_RESIDENTIAL_001', 'DER_COMMERCIAL_001', 'DER_INDUSTRIAL_001']
        
    def connect_to_cassandra(self):
        """Підключення до Cassandra"""
        try:
            self.cluster = Cluster(
                contact_points=self.hosts,
                port=self.port,
                load_balancing_policy=DCAwareRoundRobinPolicy()
            )
            self.session = self.cluster.connect('der_energy_lab')
            logger.info("Підключено до Cassandra")
            return True
        except Exception as e:
            logger.error(f"Помилка підключення до Cassandra: {e}")
            return False
    
    def disconnect(self):
        """Відключення від Cassandra"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Відключено від Cassandra")
    
    def measure_query_time(self, query_func, iterations=100):
        """Вимірювання часу виконання запиту"""
        times = []
        
        for i in range(iterations):
            start_time = time.time()
            try:
                query_func()
                end_time = time.time()
                times.append((end_time - start_time) * 1000)  # Конвертація в мілісекунди
            except Exception as e:
                logger.error(f"Помилка виконання запиту: {e}")
                times.append(float('inf'))
        
        return {
            'avg': statistics.mean(times),
            'p50': statistics.median(times),
            'p95': sorted(times)[int(len(times) * 0.95)],
            'p99': sorted(times)[int(len(times) * 0.99)],
            'min': min(times),
            'max': max(times),
            'times': times
        }
    
    def test_latest_data_simple(self, device_id):
        """Тест 1: Останні 100 записів (Simple схема)"""
        def query():
            result = self.session.execute("""
                SELECT * FROM der_simple 
                WHERE device_id = %s 
                LIMIT 100
            """, (device_id,))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_latest_data_hourly(self, device_id):
        """Тест 1: Останні 100 записів (Hourly схема)"""
        def query():
            # Отримуємо останню годину
            now = datetime.now()
            bucket_hour = now.replace(minute=0, second=0, microsecond=0)
            
            result = self.session.execute("""
                SELECT * FROM der_hourly 
                WHERE device_id = %s AND bucket_hour = %s
                LIMIT 100
            """, (device_id, bucket_hour))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_latest_data_daily(self, device_id):
        """Тест 1: Останні 100 записів (Daily схема)"""
        def query():
            # Отримуємо сьогоднішні дані
            today = datetime.now().date()
            
            result = self.session.execute("""
                SELECT * FROM der_daily_raw 
                WHERE device_id = %s AND bucket_date = %s
                LIMIT 100
            """, (device_id, today))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_time_range_simple(self, device_id, hours=6):
        """Тест 2: Дані за 6 годин (Simple схема)"""
        def query():
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            result = self.session.execute("""
                SELECT * FROM der_simple 
                WHERE device_id = %s AND timestamp >= %s AND timestamp <= %s
            """, (device_id, start_time, end_time))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_time_range_hourly(self, device_id, hours=6):
        """Тест 2: Дані за 6 годин (Hourly схема)"""
        def query():
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Отримуємо всі години в діапазоні
            results = []
            current_time = start_time.replace(minute=0, second=0, microsecond=0)
            
            while current_time <= end_time:
                result = self.session.execute("""
                    SELECT * FROM der_hourly 
                    WHERE device_id = %s AND bucket_hour = %s 
                    AND timestamp >= %s AND timestamp <= %s
                """, (device_id, current_time, start_time, end_time))
                results.extend(list(result))
                current_time += timedelta(hours=1)
            
            return results
        
        return self.measure_query_time(query)
    
    def test_time_range_daily(self, device_id, hours=6):
        """Тест 2: Дані за 6 годин (Daily схема)"""
        def query():
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Отримуємо дані за сьогодні
            today = end_time.date()
            
            result = self.session.execute("""
                SELECT * FROM der_daily_raw 
                WHERE device_id = %s AND bucket_date = %s
                AND timestamp >= %s AND timestamp <= %s
            """, (device_id, today, start_time, end_time))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_daily_aggregation_simple(self, device_id):
        """Тест 3: Денна агрегація (Simple схема)"""
        def query():
            today = datetime.now().date()
            start_time = datetime.combine(today, datetime.min.time())
            end_time = start_time + timedelta(days=1)
            
            result = self.session.execute("""
                SELECT AVG(net_power) as avg_power, MAX(net_power) as max_power, 
                       MIN(net_power) as min_power, AVG(battery_level) as avg_battery,
                       COUNT(*) as record_count
                FROM der_simple 
                WHERE device_id = %s AND timestamp >= %s AND timestamp < %s
            """, (device_id, start_time, end_time))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_daily_aggregation_hourly(self, device_id):
        """Тест 3: Денна агрегація (Hourly схема)"""
        def query():
            today = datetime.now().date()
            start_time = datetime.combine(today, datetime.min.time())
            end_time = start_time + timedelta(days=1)
            
            # Отримуємо всі години за день
            results = []
            current_time = start_time.replace(minute=0, second=0, microsecond=0)
            
            while current_time < end_time:
                result = self.session.execute("""
                    SELECT AVG(net_power) as avg_power, MAX(net_power) as max_power, 
                           MIN(net_power) as min_power, AVG(battery_level) as avg_battery,
                           COUNT(*) as record_count
                    FROM der_hourly 
                    WHERE device_id = %s AND bucket_hour = %s
                """, (device_id, current_time))
                results.extend(list(result))
                current_time += timedelta(hours=1)
            
            return results
        
        return self.measure_query_time(query)
    
    def test_daily_aggregation_daily(self, device_id):
        """Тест 3: Денна агрегація (Daily схема - використовуємо агреговані дані)"""
        def query():
            today = datetime.now().date()
            start_time = datetime.combine(today, datetime.min.time())
            
            result = self.session.execute("""
                SELECT AVG(avg_net_power) as avg_power, MAX(max_net_power) as max_power, 
                       MIN(min_net_power) as min_power, AVG(avg_battery_level) as avg_battery,
                       SUM(sample_count) as record_count
                FROM der_daily_aggregates 
                WHERE device_id = %s AND hour >= %s AND hour < %s
            """, (device_id, start_time, start_time + timedelta(days=1)))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_filtered_query_without_mv(self, device_id, power_threshold=2.5):
        """Тест 4: Фільтрований запит без Materialized View"""
        def query():
            result = self.session.execute("""
                SELECT * FROM der_hourly 
                WHERE device_id = %s AND net_power > %s
                ALLOW FILTERING
            """, (device_id, power_threshold))
            return list(result)
        
        return self.measure_query_time(query)
    
    def test_filtered_query_with_mv(self, device_id, power_threshold=2.5):
        """Тест 4: Фільтрований запит з Materialized View"""
        def query():
            result = self.session.execute("""
                SELECT * FROM der_high_power 
                WHERE device_id = %s ALLOW FILTERING
            """, (device_id,))
            return list(result)
        
        return self.measure_query_time(query)
    
    def run_all_tests(self):
        """Запуск всіх тестів"""
        logger.info("Початок benchmark тестів")
        
        for device_id in self.test_devices:
            logger.info(f"Тестування пристрою: {device_id}")
            
            # Тест 1: Latest Data
            logger.info("Тест 1: Latest Data")
            self.results['simple']['latest_data'] = self.test_latest_data_simple(device_id)
            self.results['hourly']['latest_data'] = self.test_latest_data_hourly(device_id)
            self.results['daily']['latest_data'] = self.test_latest_data_daily(device_id)
            
            # Тест 2: Time Range
            logger.info("Тест 2: Time Range")
            self.results['simple']['time_range'] = self.test_time_range_simple(device_id)
            self.results['hourly']['time_range'] = self.test_time_range_hourly(device_id)
            self.results['daily']['time_range'] = self.test_time_range_daily(device_id)
            
            # Тест 3: Daily Aggregation
            logger.info("Тест 3: Daily Aggregation")
            self.results['simple']['daily_aggregation'] = self.test_daily_aggregation_simple(device_id)
            self.results['hourly']['daily_aggregation'] = self.test_daily_aggregation_hourly(device_id)
            self.results['daily']['daily_aggregation'] = self.test_daily_aggregation_daily(device_id)
            
            # Тест 4: Filtered Query
            logger.info("Тест 4: Filtered Query")
            self.results['simple']['filtered_without_mv'] = self.test_filtered_query_without_mv(device_id)
            self.results['hourly']['filtered_without_mv'] = self.test_filtered_query_without_mv(device_id)
            self.results['daily']['filtered_without_mv'] = self.test_filtered_query_without_mv(device_id)
            
            self.results['simple']['filtered_with_mv'] = self.test_filtered_query_with_mv(device_id)
            self.results['hourly']['filtered_with_mv'] = self.test_filtered_query_with_mv(device_id)
            self.results['daily']['filtered_with_mv'] = self.test_filtered_query_with_mv(device_id)
            
            break  # Тестуємо тільки один пристрій для демонстрації
        
        logger.info("Benchmark тести завершено")
    
    def print_results(self):
        """Виведення результатів у вигляді таблиць"""
        print("\n" + "="*80)
        print("РЕЗУЛЬТАТИ BENCHMARK ТЕСТІВ")
        print("="*80)
        
        # Таблиця 1: Read Latency
        print("\nТаблиця 1: Порівняння Read Latency (мс)")
        print("-" * 60)
        print(f"{'Схема':<15} {'Query Type':<20} {'Avg':<8} {'P95':<8} {'P99':<8}")
        print("-" * 60)
        
        for schema in ['simple', 'hourly', 'daily']:
            for query_type in ['latest_data', 'time_range', 'daily_aggregation']:
                if query_type in self.results[schema]:
                    result = self.results[schema][query_type]
                    print(f"{schema:<15} {query_type:<20} {result['avg']:<8.2f} {result['p95']:<8.2f} {result['p99']:<8.2f}")
        
        # Таблиця 2: Filtered Query Performance
        print("\nТаблиця 2: Вплив Materialized Views (мс)")
        print("-" * 60)
        print(f"{'Схема':<15} {'Without MV':<12} {'With MV':<10} {'Improvement':<12}")
        print("-" * 60)
        
        for schema in ['simple', 'hourly', 'daily']:
            if 'filtered_without_mv' in self.results[schema] and 'filtered_with_mv' in self.results[schema]:
                without_mv = self.results[schema]['filtered_without_mv']['avg']
                with_mv = self.results[schema]['filtered_with_mv']['avg']
                improvement = without_mv / with_mv if with_mv > 0 else 0
                print(f"{schema:<15} {without_mv:<12.2f} {with_mv:<10.2f} x{improvement:<12.2f}")
        

def main():
    """Основна функція"""
    logger.info("Запуск benchmark тестів")
    
    benchmark = DERBenchmark()
    
    try:
        # Підключення до Cassandra
        if not benchmark.connect_to_cassandra():
            logger.error("Не вдалося підключитися до Cassandra")
            return
        
        # Запуск тестів
        benchmark.run_all_tests()
        
        # Виведення результатів
        benchmark.print_results()
        
    except Exception as e:
        logger.error(f"Помилка під час benchmark тестів: {e}")
    finally:
        benchmark.disconnect()

if __name__ == "__main__":
    main()
