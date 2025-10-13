#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Система моніторингу розподілених енергетичних ресурсів (DER)
Варіант 8: Розподілені енергетичні ресурси
Підваріант А – Аналітичний
"""

import uuid
import random
import datetime
from datetime import datetime, timedelta, date
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import pandas as pd
import numpy as np

class DERMonitoringSystem:
    def __init__(self, host='localhost', port=9042):
        """Ініціалізація системи моніторингу DER"""
        self.host = host
        self.port = port
        self.cluster = None
        self.session = None
        self.generator_count = 1000
        
        # Типи джерел енергії
        self.source_types = ['solar', 'wind', 'biomass']
        
        # Регіони
        self.regions = ['Kyiv', 'Lviv', 'Kharkiv', 'Dnipro', 'Odessa', 'Zaporizhzhia', 'Kryvyi Rih', 'Mykolaiv']
        
        # Статуси генераторів
        self.statuses = ['active', 'maintenance', 'offline', 'warning']
        
        # Виробники
        self.manufacturers = ['Siemens', 'GE', 'Vestas', 'Suntech', 'First Solar', 'Enphase', 'SMA', 'ABB']
        
    def connect_to_cassandra(self):
        """Підключення до Cassandra кластера"""
        try:
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect()
            print("✅ Успішно підключено до Cassandra")
            return True
        except Exception as e:
            print(f"❌ Помилка підключення до Cassandra: {e}")
            return False
    
    def create_keyspace_and_tables(self):
        """Створення keyspace та таблиць"""
        try:
            # Створення keyspace
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS der_monitoring 
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """)
            
            # Використання keyspace
            self.session.set_keyspace('der_monitoring')
            
            # Створення таблиць
            self._create_operational_data_table()
            self._create_sources_table()
            self._create_regional_balance_table()
            self._create_dispatcher_reports_table()
            
            print("✅ Keyspace та таблиці створено успішно")
            return True
            
        except Exception as e:
            print(f"❌ Помилка створення структури: {e}")
            return False
    
    def _create_operational_data_table(self):
        """Створення таблиці оперативних даних"""
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS generator_operational_data (
                generator_id UUID,
                timestamp TIMESTAMP,
                power_output DOUBLE,
                voltage DOUBLE,
                current DOUBLE,
                temperature DOUBLE,
                wind_speed DOUBLE,
                solar_irradiance DOUBLE,
                efficiency DOUBLE,
                status TEXT,
                PRIMARY KEY (generator_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)
    
    def _create_sources_table(self):
        """Створення таблиці типів джерел"""
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS generator_sources (
                generator_id UUID,
                source_type TEXT,
                region TEXT,
                capacity DOUBLE,
                installation_date TIMESTAMP,
                manufacturer TEXT,
                model TEXT,
                coordinates TEXT,
                PRIMARY KEY (generator_id, source_type)
            )
        """)
    
    def _create_regional_balance_table(self):
        """Створення таблиці регіональних балансів"""
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS regional_energy_balance (
                region TEXT,
                date DATE,
                total_generation DOUBLE,
                total_consumption DOUBLE,
                net_balance DOUBLE,
                peak_demand DOUBLE,
                renewable_percentage DOUBLE,
                grid_stability_index DOUBLE,
                PRIMARY KEY (region, date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
    
    def _create_dispatcher_reports_table(self):
        """Створення таблиці звітів диспетчера"""
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS dispatcher_reports (
                report_type TEXT,
                report_date TIMESTAMP,
                region TEXT,
                total_generators INT,
                total_capacity DOUBLE,
                average_efficiency DOUBLE,
                peak_generation DOUBLE,
                system_reliability DOUBLE,
                maintenance_alerts INT,
                performance_score DOUBLE,
                PRIMARY KEY (report_type, report_date)
            ) WITH CLUSTERING ORDER BY (report_date DESC)
        """)
    
    def generate_generator_sources(self):
        """Генерація даних про типи джерел для 1000 генераторів"""
        print("🔄 Генерування даних про типи джерел...")
        
        # Перевірка чи вже є дані
        existing_count = self.session.execute("SELECT COUNT(*) FROM generator_sources").one()[0]
        if existing_count > 0:
            print(f"✅ Дані про типи джерел вже існують ({existing_count} записів). Пропускаємо генерацію.")
            return
        
        for i in range(self.generator_count):
            generator_id = uuid.uuid4()
            source_type = random.choice(self.source_types)
            region = random.choice(self.regions)
            
            # Параметри залежно від типу джерела
            if source_type == 'solar':
                capacity = random.uniform(5, 50)  # кВт
                manufacturer = random.choice(['Suntech', 'First Solar', 'Enphase'])
            elif source_type == 'wind':
                capacity = random.uniform(10, 100)  # кВт
                manufacturer = random.choice(['Vestas', 'Siemens', 'GE'])
            else:  # biomass
                capacity = random.uniform(15, 80)  # кВт
                manufacturer = random.choice(['SMA', 'ABB', 'Siemens'])
            
            installation_date = datetime.now() - timedelta(days=random.randint(30, 1095))
            coordinates = f"{random.uniform(46.0, 52.0):.4f},{random.uniform(22.0, 40.0):.4f}"
            
            self.session.execute("""
                INSERT INTO generator_sources 
                (generator_id, source_type, region, capacity, installation_date, manufacturer, model, coordinates)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (generator_id, source_type, region, capacity, installation_date, 
                  manufacturer, f"{manufacturer}-{random.randint(1000, 9999)}", coordinates))
        
        print(f"✅ Згенеровано {self.generator_count} записів про типи джерел")
    
    def generate_operational_data(self, days=7):
        """Генерація оперативних даних за останні дні"""
        print(f"🔄 Генерування оперативних даних за {days} днів...")
        
        # Перевірка чи вже є оперативні дані
        existing_count = self.session.execute("SELECT COUNT(*) FROM generator_operational_data").one()[0]
        if existing_count > 0:
            print(f"✅ Оперативні дані вже існують ({existing_count} записів). Пропускаємо генерацію.")
            return existing_count
        
        # Отримуємо всі генератори
        generators = self.session.execute("SELECT generator_id, source_type, region, capacity FROM generator_sources")
        generator_list = list(generators)
        
        total_records = 0
        current_time = datetime.now()
        
        for generator in generator_list:
            generator_id = generator.generator_id
            source_type = generator.source_type
            region = generator.region
            capacity = generator.capacity
            
            # Генеруємо дані за кожну годину
            for day in range(days):
                for hour in range(24):
                    timestamp = current_time - timedelta(days=day, hours=hour)
                    
                    # Генерація параметрів залежно від типу джерела
                    if source_type == 'solar':
                        # Сонячні панелі: залежність від часу доби та погоди
                        hour_factor = max(0, np.sin(np.pi * hour / 24))  # Синусоїдальна залежність
                        weather_factor = random.uniform(0.6, 1.0)
                        power_output = capacity * hour_factor * weather_factor * random.uniform(0.8, 1.0)
                        voltage = random.uniform(220, 240)
                        current = power_output / voltage if voltage > 0 else 0
                        temperature = random.uniform(15, 45)
                        wind_speed = random.uniform(0, 10)
                        solar_irradiance = random.uniform(200, 1000)  # Вт/м²
                        
                    elif source_type == 'wind':
                        # Вітрові турбіни: залежність від швидкості вітру
                        wind_speed = random.uniform(0, 25)
                        if wind_speed < 3 or wind_speed > 20:
                            power_output = 0
                        else:
                            power_output = capacity * min(1.0, (wind_speed - 3) / 12) * random.uniform(0.7, 1.0)
                        voltage = random.uniform(400, 690)
                        current = power_output / voltage if voltage > 0 else 0
                        temperature = random.uniform(-10, 35)
                        solar_irradiance = 0
                        
                    else:  # biomass
                        # Біоенергетика: більш стабільна
                        power_output = capacity * random.uniform(0.6, 0.95)
                        voltage = random.uniform(380, 400)
                        current = power_output / voltage if voltage > 0 else 0
                        temperature = random.uniform(20, 80)
                        wind_speed = random.uniform(0, 5)
                        solar_irradiance = 0
                    
                    # Розрахунок ефективності
                    efficiency = (power_output / capacity * 100) if capacity > 0 else 0
                    efficiency = min(100, max(0, efficiency))
                    
                    # Статус генератора
                    if efficiency < 10:
                        status = 'offline'
                    elif efficiency < 50:
                        status = 'warning'
                    elif random.random() < 0.02:  # 2% ймовірність техобслуговування
                        status = 'maintenance'
                    else:
                        status = 'active'
                    
                    # Запис у базу
                    self.session.execute("""
                        INSERT INTO generator_operational_data 
                        (generator_id, timestamp, power_output, voltage, current, temperature, 
                         wind_speed, solar_irradiance, efficiency, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (generator_id, timestamp, power_output, voltage, current, temperature,
                          wind_speed, solar_irradiance, efficiency, status))
                    
                    total_records += 1
        
        print(f"✅ Згенеровано {total_records} записів оперативних даних")
        return total_records
    
    def generate_regional_balance(self, days=7):
        """Генерація регіональних балансів енергії"""
        print("🔄 Генерування регіональних балансів...")
        
        # Перевірка чи вже є регіональні баланси
        existing_count = self.session.execute("SELECT COUNT(*) FROM regional_energy_balance").one()[0]
        if existing_count > 0:
            print(f"✅ Регіональні баланси вже існують ({existing_count} записів). Пропускаємо генерацію.")
            return
        
        for day in range(days):
            current_date = date.today() - timedelta(days=day)
            
            for region in self.regions:
                # Отримуємо дані по регіону за день
                # Отримуємо generator_id для регіону
                region_generators = self.session.execute("""
                    SELECT generator_id FROM generator_sources WHERE region = %s
                """, (region,))
                
                generator_ids = [row.generator_id for row in region_generators]
                
                if generator_ids:
                    # Отримуємо дані для генераторів регіону (без функції date)
                    region_data = self.session.execute("""
                        SELECT power_output, efficiency FROM generator_operational_data 
                        WHERE generator_id IN ({}) AND timestamp >= %s AND timestamp < %s ALLOW FILTERING
                    """.format(','.join(['%s'] * len(generator_ids))), 
                    generator_ids + [current_date, current_date + timedelta(days=1)])
                else:
                    region_data = []
                
                region_list = list(region_data)
                
                if region_list:
                    total_generation = sum(record.power_output for record in region_list)
                    avg_efficiency = np.mean([record.efficiency for record in region_list])
                else:
                    total_generation = 0
                    avg_efficiency = 0
                
                # Розрахунок споживання (зазвичай більше ніж генерація)
                total_consumption = total_generation * random.uniform(1.2, 1.8)
                net_balance = total_generation - total_consumption
                peak_demand = total_consumption * random.uniform(1.3, 1.6)
                renewable_percentage = min(100, avg_efficiency)
                grid_stability_index = random.uniform(0.7, 0.95)
                
                self.session.execute("""
                    INSERT INTO regional_energy_balance 
                    (region, date, total_generation, total_consumption, net_balance, 
                     peak_demand, renewable_percentage, grid_stability_index)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (region, current_date, total_generation, total_consumption, net_balance,
                      peak_demand, renewable_percentage, grid_stability_index))
        
        print("✅ Згенеровано регіональні баланси")
    
    def generate_dispatcher_reports(self):
        """Генерація звітів для центрального диспетчера"""
        print("🔄 Генерування звітів диспетчера...")
        
        # Перевірка чи вже є звіти диспетчера
        existing_count = self.session.execute("SELECT COUNT(*) FROM dispatcher_reports").one()[0]
        if existing_count > 0:
            print(f"✅ Звіти диспетчера вже існують ({existing_count} записів). Пропускаємо генерацію.")
            return
        
        report_types = ['daily', 'weekly', 'monthly', 'emergency']
        current_time = datetime.now()
        
        for report_type in report_types:
            for region in self.regions:
                # Отримуємо статистику по регіону
                # Отримуємо статистику по регіону окремими запитами
                # Кількість генераторів та середня потужність
                gen_stats = self.session.execute("""
                    SELECT COUNT(*) as total_gens, AVG(capacity) as avg_capacity
                    FROM generator_sources WHERE region = %s
                """, (region,))
                
                gen_data = gen_stats.one()
                total_generators = gen_data.total_gens if gen_data else 0
                avg_capacity = gen_data.avg_capacity if gen_data else 0
                
                # Отримуємо оперативні дані для регіону
                region_generators = self.session.execute("""
                    SELECT generator_id FROM generator_sources WHERE region = %s
                """, (region,))
                
                generator_ids = [row.generator_id for row in region_generators]
                
                if generator_ids:
                    op_stats = self.session.execute("""
                        SELECT AVG(efficiency) as avg_efficiency, MAX(power_output) as peak_gen
                        FROM generator_operational_data 
                        WHERE generator_id IN ({}) AND timestamp > %s ALLOW FILTERING
                    """.format(','.join(['%s'] * len(generator_ids))), 
                    generator_ids + [current_time - timedelta(days=1)])
                    
                    op_data = op_stats.one()
                    avg_efficiency = op_data.avg_efficiency if op_data else 0
                    peak_generation = op_data.peak_gen if op_data else 0
                else:
                    avg_efficiency = 0
                    peak_generation = 0
                
                total_capacity = avg_capacity * total_generators
                
                system_reliability = random.uniform(0.85, 0.98)
                maintenance_alerts = random.randint(0, 5)
                performance_score = (avg_efficiency + system_reliability * 100) / 2
                
                self.session.execute("""
                    INSERT INTO dispatcher_reports 
                    (report_type, report_date, region, total_generators, total_capacity,
                     average_efficiency, peak_generation, system_reliability, 
                     maintenance_alerts, performance_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (report_type, current_time, region, total_generators, total_capacity,
                      avg_efficiency, peak_generation, system_reliability,
                      maintenance_alerts, performance_score))
        
        print("✅ Згенеровано звіти диспетчера")
    
    def analyze_power_data(self):
        """Аналітичні обчислення потужності (підваріант А)"""
        print("\n📊 АНАЛІТИЧНІ ОБЧИСЛЕННЯ ПОТУЖНОСТІ")
        print("=" * 50)
        
        # Отримуємо всі дані про потужність (без JOIN)
        # Спочатку отримуємо оперативні дані
        power_data = self.session.execute("""
            SELECT generator_id, power_output, timestamp 
            FROM generator_operational_data 
            WHERE timestamp > %s ALLOW FILTERING
        """, (datetime.now() - timedelta(days=1),))
        
        # Отримуємо метадані генераторів
        generators_meta = self.session.execute("""
            SELECT generator_id, region, source_type 
            FROM generator_sources
        """)
        
        # Створюємо словник для швидкого пошуку метаданих
        meta_dict = {row.generator_id: (row.region, row.source_type) for row in generators_meta}
        
        # Об'єднуємо дані
        combined_data = []
        for row in power_data:
            if row.generator_id in meta_dict:
                region, source_type = meta_dict[row.generator_id]
                combined_data.append({
                    'power_output': row.power_output,
                    'region': region,
                    'source_type': source_type
                })
        
        power_list = combined_data
        
        if not power_list:
            print("❌ Немає даних для аналізу")
            return
        
        # Розрахунок статистики
        power_values = [record['power_output'] for record in power_list]
        avg_power = np.mean(power_values)
        max_power = np.max(power_values)
        
        print(f"Середня потужність = {avg_power:.2f} кВт")
        print(f"Максимальна потужність = {max_power:.2f} кВт")
        
        # Аналіз по регіонах
        region_stats = {}
        for record in power_list:
            region = record['region']
            if region not in region_stats:
                region_stats[region] = []
            region_stats[region].append(record['power_output'])
        
        print("\n📈 АНАЛІЗ ПО РЕГІОНАХ:")
        for region, powers in region_stats.items():
            avg_region = np.mean(powers)
            max_region = np.max(powers)
            print(f"{region}: Середня = {avg_region:.2f} кВт, Максимальна = {max_region:.2f} кВт")
        
        # Знаходимо найкращий регіон
        best_region = max(region_stats.items(), key=lambda x: np.mean(x[1]))
        print(f"\n🏆 Найвищі показники: {best_region[0]} (середня: {np.mean(best_region[1]):.2f} кВт)")
        
        # Аналіз по типах джерел
        source_stats = {}
        for record in power_list:
            source_type = record['source_type']
            if source_type not in source_stats:
                source_stats[source_type] = []
            source_stats[source_type].append(record['power_output'])
        
        print("\n🔋 АНАЛІЗ ПО ТИПАХ ДЖЕРЕЛ:")
        for source_type, powers in source_stats.items():
            avg_source = np.mean(powers)
            max_source = np.max(powers)
            print(f"{source_type}: Середня = {avg_source:.2f} кВт, Максимальна = {max_source:.2f} кВт")
        
        # Найкращий тип джерела
        best_source = max(source_stats.items(), key=lambda x: np.mean(x[1]))
        print(f"\n⚡ Найефективніший тип: {best_source[0]} (середня: {np.mean(best_source[1]):.2f} кВт)")
    
    def analyze_partitioning_impact(self):
        """Аналіз впливу схеми партиціонування (generator_type, region)"""
        print("\n🔍 АНАЛІЗ ВПЛИВУ СХЕМИ ПАРТИЦІОНУВАННЯ")
        print("=" * 50)
        
        # Тестування швидкості запитів з різними ключами
        import time
        
        # Запит по generator_id (partition key)
        start_time = time.time()
        result1 = self.session.execute("SELECT COUNT(*) FROM generator_operational_data WHERE generator_id = %s LIMIT 1", 
                                     (list(self.session.execute("SELECT generator_id FROM generator_sources LIMIT 1"))[0].generator_id,))
        time1 = time.time() - start_time
        
        # Запит по region (без JOIN - через окремі запити)
        start_time = time.time()
        # Отримуємо generator_id для регіону
        kyiv_generators = self.session.execute("SELECT generator_id FROM generator_sources WHERE region = %s", ('Kyiv',))
        kyiv_ids = [row.generator_id for row in kyiv_generators]
        if kyiv_ids:
            result2 = self.session.execute("""
                SELECT COUNT(*) FROM generator_operational_data 
                WHERE generator_id IN ({}) ALLOW FILTERING
            """.format(','.join(['%s'] * len(kyiv_ids))), kyiv_ids)
        else:
            result2 = []
        time2 = time.time() - start_time
        
        # Запит по source_type (без JOIN - через окремі запити)
        start_time = time.time()
        # Отримуємо generator_id для типу джерела
        solar_generators = self.session.execute("SELECT generator_id FROM generator_sources WHERE source_type = %s", ('solar',))
        solar_ids = [row.generator_id for row in solar_generators]
        if solar_ids:
            result3 = self.session.execute("""
                SELECT COUNT(*) FROM generator_operational_data 
                WHERE generator_id IN ({}) ALLOW FILTERING
            """.format(','.join(['%s'] * len(solar_ids))), solar_ids)
        else:
            result3 = []
        time3 = time.time() - start_time
        
        print(f"⏱️ Час запиту по generator_id (partition key): {time1:.4f}с")
        print(f"⏱️ Час запиту по region (через окремі запити): {time2:.4f}с")
        print(f"⏱️ Час запиту по source_type (через окремі запити): {time3:.4f}с")
        
        print(f"\n📊 Висновки:")
        print(f"• Partition key (generator_id) - найшвидший доступ")
        print(f"• Окремі запити повільніші в {time2/time1:.1f}x та {time3/time1:.1f}x разів")
        print(f"• Схема (generator_type, region) потребує додаткових індексів для оптимізації")
    
    def get_system_statistics(self):
        """Отримання загальної статистики системи"""
        print("\n📊 ЗАГАЛЬНА СТАТИСТИКА СИСТЕМИ")
        print("=" * 50)
        
        # Кількість генераторів
        gen_count = self.session.execute("SELECT COUNT(*) FROM generator_sources").one()[0]
        print(f"Загальна кількість генераторів: {gen_count}")
        
        # Кількість оперативних записів
        op_count = self.session.execute("SELECT COUNT(*) FROM generator_operational_data").one()[0]
        print(f"Кількість оперативних записів: {op_count}")
        
        # Розподіл по типах джерел (без GROUP BY)
        source_data = self.session.execute("SELECT source_type FROM generator_sources")
        source_counts = {}
        for row in source_data:
            source_type = row.source_type
            source_counts[source_type] = source_counts.get(source_type, 0) + 1
        
        print("\nРозподіл по типах джерел:")
        for source_type, count in source_counts.items():
            print(f"  {source_type}: {count}")
        
        # Розподіл по регіонах (без GROUP BY)
        region_data = self.session.execute("SELECT region FROM generator_sources")
        region_counts = {}
        for row in region_data:
            region = row.region
            region_counts[region] = region_counts.get(region, 0) + 1
        
        print("\nРозподіл по регіонах:")
        for region, count in region_counts.items():
            print(f"  {region}: {count}")
    
    def close_connection(self):
        """Закриття з'єднання"""
        if self.cluster:
            self.cluster.shutdown()
            print("✅ З'єднання з Cassandra закрито")

def main():
    """Головна функція програми"""
    print("🚀 СИСТЕМА МОНІТОРИНГУ РОЗПОДІЛЕНИХ ЕНЕРГЕТИЧНИХ РЕСУРСІВ (DER)")
    print("=" * 70)
    
    # Ініціалізація системи
    der_system = DERMonitoringSystem()
    
    try:
        # Підключення до Cassandra
        if not der_system.connect_to_cassandra():
            return
        
        # Створення структури бази даних
        if not der_system.create_keyspace_and_tables():
            return
        
        # Генерація тестових даних
        print("\n🔄 ГЕНЕРАЦІЯ ТЕСТОВИХ ДАНИХ...")
        der_system.generate_generator_sources()
        der_system.generate_operational_data(days=7)
        der_system.generate_regional_balance(days=7)
        der_system.generate_dispatcher_reports()
        
        # Аналітичні обчислення
        der_system.analyze_power_data()
        
        # Аналіз партиціонування
        der_system.analyze_partitioning_impact()
        
        # Загальна статистика
        der_system.get_system_statistics()
        
        print("\n✅ ПРОГРАМА ВИКОНАНА УСПІШНО!")
        
    except Exception as e:
        print(f"❌ Помилка виконання: {e}")
    
    finally:
        der_system.close_connection()

if __name__ == "__main__":
    main()
