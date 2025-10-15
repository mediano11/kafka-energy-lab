#!/usr/bin/env python3
"""
Генератор тестових даних для лабораторної роботи 3
Варіант 8: Розподілені енергетичні ресурси (DER)
Підваріант А: орієнтація на зберігання

Генерує реалістичні дані для системи DER з урахуванням:
- Денних піків потужності (12:00-14:00)
- Ранкових/вечірніх піків (6-9, 17-19)
- Нічних мінімумів (21-5)
- Хмарності та випадкових падінь
- Різних типів установок (residential, commercial, industrial)
"""

import random
import math
import time
from datetime import datetime, timedelta, date
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import uuid
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DERDataGenerator:
    def __init__(self, hosts=['127.0.0.1'], port=9042):
        """Ініціалізація генератора даних"""
        self.hosts = hosts
        self.port = port
        self.cluster = None
        self.session = None
        
        # Параметри системи DER
        self.device_types = ['residential', 'commercial', 'industrial']
        self.device_count = 30  # Кількість пристроїв
        
        # Характеристики різних типів установок
        self.installation_profiles = {
            'residential': {
                'max_power': 5.0,      # кВт
                'battery_capacity': 10.0,  # кВт·год
                'base_consumption': 0.5,   # кВт
                'peak_hours': [12, 13, 18, 19],  # години піку
                'night_minimum': 0.1
            },
            'commercial': {
                'max_power': 50.0,
                'battery_capacity': 100.0,
                'base_consumption': 5.0,
                'peak_hours': [9, 10, 11, 12, 13, 14, 15, 16, 17],
                'night_minimum': 1.0
            },
            'industrial': {
                'max_power': 500.0,
                'battery_capacity': 1000.0,
                'base_consumption': 50.0,
                'peak_hours': [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
                'night_minimum': 10.0
            }
        }
        
        # Параметри генерації
        self.weather_impact = 0.3  # Вплив погоди (0-1)
        self.battery_efficiency = 0.95  # Ефективність акумулятора
        
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
    
    def generate_device_id(self, device_type, index):
        """Генерація унікального ID пристрою"""
        return f"DER_{device_type.upper()}_{index:03d}"
    
    def calculate_solar_irradiance(self, hour, day_of_year):
        """Розрахунок сонячної інсоляції залежно від часу та дати"""
        # Синусоїдальна модель сонячного дня
        if 6 <= hour <= 18:
            # Нормалізований час (0-1)
            normalized_hour = (hour - 6) / 12.0
            # Синусоїдальна крива
            irradiance = math.sin(normalized_hour * math.pi)
            
            # Сезонні корекції (літо/зима)
            seasonal_factor = 0.7 + 0.3 * math.cos(2 * math.pi * day_of_year / 365)
            irradiance *= seasonal_factor
            
            # Випадкові коливання (хмарність)
            weather_factor = 1.0 - random.uniform(0, self.weather_impact)
            irradiance *= weather_factor
            
            return max(0, irradiance)
        return 0.0
    
    def calculate_net_power(self, device_type, hour, day_of_year, battery_level):
        """Розрахунок чистої потужності"""
        profile = self.installation_profiles[device_type]
        
        # Базова споживання
        base_consumption = profile['base_consumption']
        
        # Сонячна генерація
        irradiance = self.calculate_solar_irradiance(hour, day_of_year)
        solar_generation = irradiance * profile['max_power']
        
        # Споживання залежно від часу
        if hour in profile['peak_hours']:
            consumption_multiplier = 1.5 + random.uniform(0, 0.5)
        elif 21 <= hour or hour <= 5:
            consumption_multiplier = 0.2 + random.uniform(0, 0.3)
        else:
            consumption_multiplier = 0.8 + random.uniform(0, 0.4)
        
        consumption = base_consumption * consumption_multiplier
        
        # Розрахунок чистої потужності
        net_power = solar_generation - consumption
        
        # Вплив акумулятора
        if battery_level > 80 and net_power > 0:
            # Акумулятор майже повний - обмежуємо зарядку
            net_power *= 0.3
        elif battery_level < 20 and net_power < 0:
            # Акумулятор розряджений - обмежуємо розрядку
            net_power *= 0.5
        
        return round(net_power, 2)
    
    def calculate_battery_level(self, device_type, hour, net_power, previous_battery_level):
        """Розрахунок рівня заряду акумулятора"""
        profile = self.installation_profiles[device_type]
        capacity = profile['battery_capacity']
        
        # Ефективність зарядки/розрядки
        if net_power > 0:  # Зарядка
            efficiency = self.battery_efficiency
        else:  # Розрядка
            efficiency = 1.0 / self.battery_efficiency
        
        # Зміна рівня заряду (кВт·год)
        energy_change = net_power * (1/60) * efficiency  # 1 хвилина
        
        # Новий рівень заряду
        new_level = previous_battery_level + energy_change
        
        # Обмеження 0-100%
        new_level = max(0, min(100, new_level))
        
        return round(new_level, 1)
    
    def generate_telemetry_record(self, device_id, device_type, timestamp, previous_battery_level):
        """Генерація одного запису телеметрії"""
        hour = timestamp.hour
        day_of_year = timestamp.timetuple().tm_yday
        
        # Розрахунок показників
        net_power = self.calculate_net_power(device_type, hour, day_of_year, previous_battery_level)
        battery_level = self.calculate_battery_level(device_type, hour, net_power, previous_battery_level)
        
        # Напруга (220В ± 10%)
        voltage = round(220 + random.uniform(-22, 22), 1)
        
        # Струм (розрахунок на основі потужності)
        if voltage > 0:
            current = abs(net_power * 1000 / voltage)  # кВт -> Вт, потім / В
        else:
            current = 0
        current = round(current, 2)
        
        # Температура (залежить від типу установки)
        if device_type == 'industrial':
            base_temp = 25 + random.uniform(-5, 15)
        elif device_type == 'commercial':
            base_temp = 22 + random.uniform(-3, 8)
        else:  # residential
            base_temp = 20 + random.uniform(-2, 5)
        
        temperature = round(base_temp, 1)
        
        return {
            'device_id': device_id,
            'timestamp': timestamp,
            'net_power': net_power,
            'battery_level': battery_level,
            'installation_type': device_type,
            'voltage': voltage,
            'current': current,
            'temperature': temperature
        }
    
    def generate_batch_data(self, start_date, days, batch_size=1000):
        """Генерація пакету даних"""
        logger.info(f"Генерація даних з {start_date} на {days} днів...")
        
        total_records = 0
        start_time = time.time()
        
        # Генерація пристроїв
        devices = []
        for i in range(self.device_count):
            device_type = random.choice(self.device_types)
            device_id = self.generate_device_id(device_type, i)
            devices.append({
                'id': device_id,
                'type': device_type,
                'battery_level': random.uniform(20, 80)  # Початковий рівень заряду
            })
        
        # Генерація даних по днях
        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            logger.info(f"Генерація даних за {current_date.strftime('%Y-%m-%d')}")
            
            # Генерація записів за день (1440 хвилин)
            for minute in range(1440):
                timestamp = current_date + timedelta(minutes=minute)
                
                # Генерація для кожного пристрою
                for device in devices:
                    record = self.generate_telemetry_record(
                        device['id'], 
                        device['type'], 
                        timestamp, 
                        device['battery_level']
                    )
                    
                    # Оновлення рівня заряду для наступного запису
                    device['battery_level'] = record['battery_level']
                    
                    # Вставка в базу даних
                    self.insert_record(record)
                    total_records += 1
                    
                    # Batch processing
                    if total_records % batch_size == 0:
                        logger.info(f"Оброблено {total_records} записів...")
        
        end_time = time.time()
        logger.info(f"Генерація завершена: {total_records} записів за {end_time - start_time:.2f} секунд")
        return total_records
    
    def insert_record(self, record):
        """Вставка запису в усі три схеми"""
        try:
            # Схема 1: Simple
            self.session.execute("""
                INSERT INTO der_simple (device_id, timestamp, net_power, battery_level, 
                                      installation_type, voltage, current, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (record['device_id'], record['timestamp'], record['net_power'], 
                  record['battery_level'], record['installation_type'], 
                  record['voltage'], record['current'], record['temperature']))
            
            # Схема 2: Hourly Bucketing
            bucket_hour = record['timestamp'].replace(minute=0, second=0, microsecond=0)
            self.session.execute("""
                INSERT INTO der_hourly (device_id, bucket_hour, timestamp, net_power, 
                                      battery_level, installation_type, voltage, current, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (record['device_id'], bucket_hour, record['timestamp'], record['net_power'],
                  record['battery_level'], record['installation_type'], 
                  record['voltage'], record['current'], record['temperature']))
            
            # Схема 3: Daily Raw
            bucket_date = record['timestamp'].date()
            self.session.execute("""
                INSERT INTO der_daily_raw (device_id, bucket_date, timestamp, net_power, 
                                         battery_level, installation_type, voltage, current, temperature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (record['device_id'], bucket_date, record['timestamp'], record['net_power'],
                  record['battery_level'], record['installation_type'], 
                  record['voltage'], record['current'], record['temperature']))
            
            # Додатково: вставка в фільтровані таблиці (замість MV)
            if record['net_power'] > 2.5:
                self.session.execute("""
                    INSERT INTO der_high_power (device_id, bucket_hour, timestamp, net_power, 
                                              battery_level, installation_type, voltage, current, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (record['device_id'], bucket_hour, record['timestamp'], record['net_power'],
                      record['battery_level'], record['installation_type'], 
                      record['voltage'], record['current'], record['temperature']))
            
            if record['battery_level'] < 20.0:
                self.session.execute("""
                    INSERT INTO der_low_battery (device_id, bucket_hour, timestamp, net_power, 
                                               battery_level, installation_type, voltage, current, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (record['device_id'], bucket_hour, record['timestamp'], record['net_power'],
                      record['battery_level'], record['installation_type'], 
                      record['voltage'], record['current'], record['temperature']))
            
            if record['installation_type'] == 'industrial':
                self.session.execute("""
                    INSERT INTO der_industrial (device_id, bucket_hour, timestamp, net_power, 
                                              battery_level, installation_type, voltage, current, temperature)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (record['device_id'], bucket_hour, record['timestamp'], record['net_power'],
                      record['battery_level'], record['installation_type'], 
                      record['voltage'], record['current'], record['temperature']))
            
        except Exception as e:
            logger.error(f"Помилка вставки запису: {e}")
    
    def generate_aggregates(self, start_date, days):
        """Генерація агрегованих даних для схеми 3"""
        logger.info("Генерація агрегованих даних...")
        
        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            
            # Отримання даних за день для кожного пристрою
            for device_type in self.device_types:
                devices = [f"DER_{device_type.upper()}_{i:03d}" for i in range(self.device_count // 3)]
                
                for device_id in devices:
                    # Агрегація по годинах
                    for hour in range(24):
                        hour_start = current_date + timedelta(hours=hour)
                        hour_end = hour_start + timedelta(hours=1)
                        
                        # Запит агрегованих даних
                        bucket_date = current_date.date()
                        result = self.session.execute("""
                            SELECT AVG(net_power) as avg_power, MAX(net_power) as max_power, 
                                   MIN(net_power) as min_power, AVG(battery_level) as avg_battery,
                                   MAX(battery_level) as max_battery, MIN(battery_level) as min_battery,
                                   AVG(voltage) as avg_voltage, AVG(current) as avg_current,
                                   AVG(temperature) as avg_temp, COUNT(*) as sample_count
                            FROM der_daily_raw 
                            WHERE device_id = %s AND bucket_date = %s 
                            AND timestamp >= %s AND timestamp < %s
                        """, (device_id, bucket_date, hour_start, hour_end))
                        
                        row = result.one()
                        if row and row.sample_count > 0:
                            # Вставка агрегованих даних
                            self.session.execute("""
                                INSERT INTO der_daily_aggregates 
                                (device_id, hour, installation_type, avg_net_power, max_net_power, 
                                 min_net_power, avg_battery_level, max_battery_level, min_battery_level,
                                 avg_voltage, avg_current, avg_temperature, sample_count)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (device_id, hour_start, device_type, 
                                  float(row.avg_power or 0), float(row.max_power or 0), float(row.min_power or 0),
                                  float(row.avg_battery or 0), float(row.max_battery or 0), float(row.min_battery or 0),
                                  float(row.avg_voltage or 0), float(row.avg_current or 0), 
                                  float(row.avg_temp or 0), int(row.sample_count or 0)))
        
        logger.info("Агрегація завершена")

def main():
    """Основна функція"""
    logger.info("Запуск генератора даних DER")
    
    # Параметри генерації
    start_date = datetime(2024, 1, 1)
    days_to_generate = 30
    
    # Створення генератора
    generator = DERDataGenerator()
    
    try:
        # Підключення до Cassandra
        if not generator.connect_to_cassandra():
            logger.error("Не вдалося підключитися до Cassandra")
            return
        
        # Перевірка чи вже є дані
        result = generator.session.execute("SELECT COUNT(*) FROM der_simple")
        existing_count = result.one()[0]
        
        if existing_count > 0:
            logger.info(f"Знайдено {existing_count} існуючих записів. Пропускаємо генерацію.")
        else:
            # Генерація даних
            total_records = generator.generate_batch_data(start_date, days_to_generate)
            logger.info(f"Генерація завершена успішно: {total_records} записів")
        
        # Генерація агрегованих даних (завжди виконуємо)
        generator.generate_aggregates(start_date, days_to_generate)
        
    except Exception as e:
        logger.error(f"Помилка під час генерації: {e}")
    finally:
        generator.disconnect()

if __name__ == "__main__":
    main()
