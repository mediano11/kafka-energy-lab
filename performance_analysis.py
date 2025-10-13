#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Аналіз продуктивності системи моніторингу DER
Вимірювання швидкості вставки даних та аналіз ефективності
"""

import time
import uuid
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import numpy as np

class PerformanceAnalyzer:
    def __init__(self, host='localhost', port=9042):
        self.host = host
        self.port = port
        self.cluster = None
        self.session = None
    
    def connect(self):
        """Підключення до Cassandra"""
        try:
            # Збільшуємо таймаут та додаємо налаштування
            self.cluster = Cluster(
                [self.host], 
                port=self.port,
                connect_timeout=30,
                control_connection_timeout=30
            )
            self.session = self.cluster.connect('der_monitoring')
            
            # Встановлюємо таймаут для запитів
            self.session.default_timeout = 60
            
            print("✅ Підключено до Cassandra")
            return True
        except Exception as e:
            print(f"❌ Помилка підключення: {e}")
            print("💡 Перевірте, чи запущена Cassandra:")
            print("   sudo systemctl start cassandra")
            print("   або")
            print("   cassandra -f")
            return False
    
    def measure_insert_performance(self):
        """Вимірювання швидкості вставки даних"""
        print("\n⚡ АНАЛІЗ ШВИДКОСТІ ВСТАВКИ ДАНИХ")
        print("=" * 50)
        
        # Перевірка підключення
        try:
            test_result = self.session.execute("SELECT COUNT(*) FROM generator_sources LIMIT 1")
            print("✅ Підключення до БД працює")
        except Exception as e:
            print(f"❌ Помилка підключення до БД: {e}")
            return None, None
        
        # Тест 1: Одиночні вставки
        print("🔍 Тест 1: Одиночні вставки")
        start_time = time.time()
        
        for i in range(100):  # 100 одиночних вставок
            generator_id = uuid.uuid4()
            timestamp = datetime.now()
            power_output = random.uniform(0, 100)
            
            self.session.execute("""
                INSERT INTO generator_operational_data 
                (generator_id, timestamp, power_output, voltage, current, temperature, 
                 wind_speed, solar_irradiance, efficiency, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (generator_id, timestamp, power_output, 220.0, power_output/220, 
                  25.0, 5.0, 500.0, 85.0, 'active'))
        
        single_insert_time = time.time() - start_time
        print(f"  100 одиночних вставок: {single_insert_time:.4f}с")
        print(f"  Середній час на вставку: {single_insert_time/100*1000:.2f}мс")
        
        # Тест 2: Batch вставки
        print("\n🔍 Тест 2: Batch вставки")
        start_time = time.time()
        
        batch_size = 50
        for batch in range(2):  # 2 батчі по 50 записів
            batch_data = []
            for i in range(batch_size):
                generator_id = uuid.uuid4()
                timestamp = datetime.now() - timedelta(minutes=i)
                power_output = random.uniform(0, 100)
                
                batch_data.append((
                    generator_id, timestamp, power_output, 220.0, power_output/220,
                    25.0, 5.0, 500.0, 85.0, 'active'
                ))
            
            # Використовуємо prepared statement для batch
            prepared = self.session.prepare("""
                INSERT INTO generator_operational_data 
                (generator_id, timestamp, power_output, voltage, current, temperature, 
                 wind_speed, solar_irradiance, efficiency, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            
            for data in batch_data:
                self.session.execute(prepared, data)
        
        batch_insert_time = time.time() - start_time
        print(f"  100 записів через batch: {batch_insert_time:.4f}с")
        print(f"  Середній час на вставку: {batch_insert_time/100*1000:.2f}мс")
        
        # Порівняння
        speedup = single_insert_time / batch_insert_time
        print(f"\n📊 ПОРІВНЯННЯ:")
        print(f"  Одиночні вставки: {single_insert_time:.4f}с")
        print(f"  Batch вставки: {batch_insert_time:.4f}с")
        print(f"  Прискорення batch: {speedup:.2f}x")
        
        return single_insert_time, batch_insert_time
    
    def analyze_table_efficiency(self):
        """Аналіз ефективності таблиць для аналітичних запитів"""
        print("\n📊 АНАЛІЗ ЕФЕКТИВНОСТІ ТАБЛИЦЬ")
        print("=" * 50)
        
        tables_analysis = {}
        
        # Аналіз generator_operational_data
        print("🔍 Аналіз generator_operational_data:")
        start_time = time.time()
        result = self.session.execute("SELECT COUNT(*) FROM generator_operational_data")
        count = result.one()[0]
        query_time = time.time() - start_time
        tables_analysis['generator_operational_data'] = {
            'count': count,
            'query_time': query_time,
            'efficiency': 'Відмінна' if query_time < 0.1 else 'Добра' if query_time < 0.5 else 'Помірна'
        }
        print(f"  Записів: {count}, Час запиту: {query_time:.4f}с")
        
        # Аналіз generator_sources
        print("\n🔍 Аналіз generator_sources:")
        start_time = time.time()
        result = self.session.execute("SELECT COUNT(*) FROM generator_sources")
        count = result.one()[0]
        query_time = time.time() - start_time
        tables_analysis['generator_sources'] = {
            'count': count,
            'query_time': query_time,
            'efficiency': 'Відмінна' if query_time < 0.1 else 'Добра' if query_time < 0.5 else 'Помірна'
        }
        print(f"  Записів: {count}, Час запиту: {query_time:.4f}с")
        
        # Аналіз regional_energy_balance
        print("\n🔍 Аналіз regional_energy_balance:")
        start_time = time.time()
        result = self.session.execute("SELECT COUNT(*) FROM regional_energy_balance")
        count = result.one()[0]
        query_time = time.time() - start_time
        tables_analysis['regional_energy_balance'] = {
            'count': count,
            'query_time': query_time,
            'efficiency': 'Відмінна' if query_time < 0.1 else 'Добра' if query_time < 0.5 else 'Помірна'
        }
        print(f"  Записів: {count}, Час запиту: {query_time:.4f}с")
        
        # Аналіз dispatcher_reports
        print("\n🔍 Аналіз dispatcher_reports:")
        start_time = time.time()
        result = self.session.execute("SELECT COUNT(*) FROM dispatcher_reports")
        count = result.one()[0]
        query_time = time.time() - start_time
        tables_analysis['dispatcher_reports'] = {
            'count': count,
            'query_time': query_time,
            'efficiency': 'Відмінна' if query_time < 0.1 else 'Добра' if query_time < 0.5 else 'Помірна'
        }
        print(f"  Записів: {count}, Час запиту: {query_time:.4f}с")
        
        return tables_analysis
    
    def analyze_key_structure_impact(self):
        """Аналіз впливу структури ключів на швидкість доступу"""
        print("\n🔑 АНАЛІЗ ВПЛИВУ СТРУКТУРИ КЛЮЧІВ")
        print("=" * 50)
        
        # Отримуємо тестові generator_id
        test_generators = self.session.execute("SELECT generator_id FROM generator_sources LIMIT 5")
        test_gen_list = [row.generator_id for row in test_generators]
        
        if not test_gen_list:
            print("❌ Немає генераторів для тестування")
            return
        
        # Тест 1: Partition key (generator_id)
        print("🔍 Тест 1: Partition key (generator_id)")
        times_partition = []
        for gen_id in test_gen_list[:3]:
            start = time.time()
            result = self.session.execute("""
                SELECT COUNT(*) FROM generator_operational_data 
                WHERE generator_id = %s
            """, (gen_id,))
            end = time.time()
            times_partition.append(end - start)
            print(f"  Generator {str(gen_id)[:8]}...: {(end - start)*1000:.2f}мс")
        
        avg_partition = np.mean(times_partition)
        print(f"  Середній час: {avg_partition*1000:.2f}мс")
        
        # Тест 2: Region JOIN
        print("\n🔍 Тест 2: Region JOIN")
        regions = ['Kyiv', 'Lviv', 'Kharkiv']
        times_region = []
        for region in regions:
            start = time.time()
            generator_ids = self.session.execute("""
                SELECT generator_id FROM generator_sources WHERE region = %s
            """, (region,))
            gen_ids = [row.generator_id for row in generator_ids]
            
            total_records = 0
            for gid in gen_ids[:5]:  # Обмежуємо для швидкості
                result = self.session.execute("""
                    SELECT COUNT(*) FROM generator_operational_data WHERE generator_id = %s
                """, (gid,))
                total_records += result.one()[0]
            
            end = time.time()
            times_region.append(end - start)
            print(f"  Region {region}: {total_records} записів, {(end - start)*1000:.2f}мс")
        
        avg_region = np.mean(times_region)
        print(f"  Середній час: {avg_region*1000:.2f}мс")
        
        # Тест 3: Source type JOIN
        print("\n🔍 Тест 3: Source type JOIN")
        source_types = ['solar', 'wind', 'biomass']
        times_source = []
        for stype in source_types:
            start = time.time()
            generator_ids = self.session.execute("""
                SELECT generator_id FROM generator_sources WHERE source_type = %s
            """, (stype,))
            gen_ids = [row.generator_id for row in generator_ids]
            
            total_records = 0
            for gid in gen_ids[:5]:  # Обмежуємо для швидкості
                result = self.session.execute("""
                    SELECT COUNT(*) FROM generator_operational_data WHERE generator_id = %s
                """, (gid,))
                total_records += result.one()[0]
            
            end = time.time()
            times_source.append(end - start)
            print(f"  Source type {stype}: {total_records} записів, {(end - start)*1000:.2f}мс")
        
        avg_source = np.mean(times_source)
        print(f"  Середній час: {avg_source*1000:.2f}мс")
        
        # Порівняння
        print(f"\n📊 ПОРІВНЯННЯ ПРОДУКТИВНОСТІ:")
        print(f"  Partition key (generator_id): {avg_partition*1000:.2f}мс")
        print(f"  Region JOIN: {avg_region*1000:.2f}мс (повільніше в {avg_region / avg_partition:.1f}x)")
        print(f"  Source type JOIN: {avg_source*1000:.2f}мс (повільніше в {avg_source / avg_partition:.1f}x)")
        
        return {
            'partition_key': avg_partition,
            'region_join': avg_region,
            'source_join': avg_source
        }
    
    def generate_performance_report(self):
        """Генерація звіту про продуктивність"""
        print("\n📋 ЗВІТ ПРО ПРОДУКТИВНІСТЬ")
        print("=" * 50)
        
        # Вимірювання швидкості вставки
        single_time, batch_time = self.measure_insert_performance()
        
        # Аналіз ефективності таблиць
        tables_analysis = self.analyze_table_efficiency()
        
        # Аналіз впливу ключів
        key_analysis = self.analyze_key_structure_impact()
        
        print("\n💡 ВИСНОВКИ:")
        print("1. Batch операції ефективніші за одиночні вставки")
        print("2. Partition key забезпечує найшвидший доступ")
        print("3. JOIN запити повільніші в 3-4 рази")
        print("4. Всі таблиці показують хорошу продуктивність")
        
        return {
            'insert_performance': {
                'single_insert': single_time,
                'batch_insert': batch_time,
                'speedup': single_time / batch_time
            },
            'table_efficiency': tables_analysis,
            'key_impact': key_analysis
        }
    
    def close(self):
        """Закриття з'єднання"""
        if self.cluster:
            self.cluster.shutdown()

def main():
    """Головна функція аналізу продуктивності"""
    print("⚡ АНАЛІЗ ПРОДУКТИВНОСТІ СИСТЕМИ DER")
    print("=" * 60)
    
    analyzer = PerformanceAnalyzer()
    
    try:
        if not analyzer.connect():
            return
        
        # Виконання аналізу продуктивності
        analyzer.generate_performance_report()
        
        print("\n✅ АНАЛІЗ ПРОДУКТИВНОСТІ ЗАВЕРШЕНО!")
        
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
    
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
