#!/usr/bin/env python3
"""
Генератор тестових даних для системи розподілених енергетичних ресурсів (DER)
Генерує дані для 1000 пристроїв з параметрами відповідно до варіанту
"""

import json
import random
import time
from datetime import datetime
from typing import Dict, List
import uuid

class DERDataGenerator:
    def __init__(self):
        self.device_types = ["solar_roof", "micro_wind", "battery", "combined"]
        self.statuses = ["generating", "consuming", "idle", "maintenance"]
        
        # Географічні координати України
        self.lat_range = (45.0, 52.0)
        self.lon_range = (22.0, 40.0)
        
    def generate_device_id(self, device_num: int) -> str:
        """Генерує ID пристрою у форматі DER_XXXX"""
        return f"DER_{device_num:04d}"
    
    def generate_power_output(self, device_type: str) -> float:
        """Генерує потужність від -5.0 до +10.0 кВт"""
        if device_type == "battery":
            # Батареї можуть як споживати, так і генерувати
            return round(random.uniform(-5.0, 10.0), 2)
        elif device_type == "solar_roof":
            # Сонячні дахи зазвичай генерують енергію
            return round(random.uniform(0.0, 10.0), 2)
        elif device_type == "micro_wind":
            # Малі вітряки генерують енергію
            return round(random.uniform(0.0, 8.0), 2)
        else:  # combined
            return round(random.uniform(-3.0, 10.0), 2)
    
    def generate_efficiency(self) -> float:
        """Генерує ККД від 80.0 до 96.0%"""
        return round(random.uniform(80.0, 96.0), 1)
    
    def generate_temperature(self) -> float:
        """Генерує температуру від -20.0 до 50.0°C"""
        return round(random.uniform(-20.0, 50.0), 1)
    
    def generate_voltage(self) -> float:
        """Генерує напругу від 220.0 до 240.0 В"""
        return round(random.uniform(220.0, 240.0), 1)
    
    def generate_current(self) -> float:
        """Генерує силу струму від 5.0 до 45.0 А"""
        return round(random.uniform(5.0, 45.0), 1)
    
    def generate_status(self, device_type: str) -> str:
        """Генерує статус пристрою з урахуванням типу"""
        if device_type == "maintenance":
            return "maintenance"
        else:
            return random.choice(["generating", "consuming", "idle"])
    
    def generate_location(self) -> Dict[str, float]:
        """Генерує географічні координати України"""
        return {
            "lat": round(random.uniform(*self.lat_range), 4),
            "lon": round(random.uniform(*self.lon_range), 4)
        }
    
    def generate_maintenance_hours(self) -> int:
        """Генерує години до ТО від 1000 до 8000"""
        return random.randint(1000, 8000)
    
    def generate_net_power(self, power_output: float) -> float:
        """Генерує чисту потужність на основі power_output"""
        # Додаємо невеликі варіації
        variation = random.uniform(-0.5, 0.5)
        return round(power_output + variation, 2)
    
    def generate_battery_soc(self, device_type: str) -> float:
        """Генерує заряд батареї від 0.0 до 100.0%"""
        if device_type in ["battery", "combined"]:
            return round(random.uniform(0.0, 100.0), 1)
        return 0.0
    
    def generate_single_record(self, device_num: int) -> Dict:
        """Генерує один запис для пристрою"""
        device_type = random.choice(self.device_types)
        power_output = self.generate_power_output(device_type)
        
        record = {
            "device_id": self.generate_device_id(device_num),
            "power_output": power_output,
            "efficiency": self.generate_efficiency(),
            "temperature": self.generate_temperature(),
            "voltage": self.generate_voltage(),
            "current": self.generate_current(),
            "status": self.generate_status(device_type),
            "location": self.generate_location(),
            "maintenance_hours": self.generate_maintenance_hours(),
            "net_power": self.generate_net_power(power_output),
            "battery_soc": self.generate_battery_soc(device_type),
            "unit_type": device_type,
            "timestamp": datetime.now().isoformat(),
            "message_id": str(uuid.uuid4())
        }
        
        return record
    
    def generate_test_data(self, num_records: int = 2000) -> List[Dict]:
        """Генерує задану кількість тестових записів"""
        records = []
        
        # Генеруємо записи для пристроїв від DER_0001 до DER_1000
        # Кожен пристрій може мати кілька записів
        devices_per_record = 1000 // (num_records // 2)  # Розподіляємо пристрої
        
        for i in range(num_records):
            device_num = (i % 1000) + 1  # Циклічно використовуємо пристрої 1-1000
            record = self.generate_single_record(device_num)
            records.append(record)
        
        return records
    
    def save_to_file(self, records: List[Dict], filename: str):
        """Зберігає записи у JSON файл"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print(f"Згенеровано {len(records)} записів та збережено у файл {filename}")

def main():
    """Основна функція для генерації тестових даних"""
    generator = DERDataGenerator()
    
    print("Генерація тестових даних для системи DER...")
    print("Параметри:")
    print("- Кількість пристроїв: 1000 (DER_0001 - DER_1000)")
    print("- Типи пристроїв: solar_roof, micro_wind, battery, combined")
    print("- Потужність: -5.0 до +10.0 кВт")
    print("- Географія: Україна (lat: 45.0-52.0, lon: 22.0-40.0)")
    print("- Кількість записів: 2000")
    print()
    
    # Генеруємо дані
    records = generator.generate_test_data(2000)
    
    # Зберігаємо у файл
    generator.save_to_file(records, "data/der_test_data.json")
    
    # Виводимо статистику
    print("\nСтатистика згенерованих даних:")
    device_types_count = {}
    status_count = {}
    power_ranges = {"negative": 0, "zero": 0, "positive": 0}
    
    for record in records:
        # Підрахунок типів пристроїв
        unit_type = record["unit_type"]
        device_types_count[unit_type] = device_types_count.get(unit_type, 0) + 1
        
        # Підрахунок статусів
        status = record["status"]
        status_count[status] = status_count.get(status, 0) + 1
        
        # Підрахунок діапазонів потужності
        power = record["power_output"]
        if power < 0:
            power_ranges["negative"] += 1
        elif power == 0:
            power_ranges["zero"] += 1
        else:
            power_ranges["positive"] += 1
    
    print("Типи пристроїв:")
    for device_type, count in device_types_count.items():
        print(f"  {device_type}: {count} записів")
    
    print("\nСтатуси:")
    for status, count in status_count.items():
        print(f"  {status}: {count} записів")
    
    print("\nДіапазони потужності:")
    print(f"  Негативна (< 0 кВт): {power_ranges['negative']} записів")
    print(f"  Нульова (= 0 кВт): {power_ranges['zero']} записів")
    print(f"  Позитивна (> 0 кВт): {power_ranges['positive']} записів")
    
    print(f"\nПриклад запису:")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
