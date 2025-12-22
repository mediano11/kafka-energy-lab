#!/usr/bin/env python3
"""
DER Producer для варіанту 8: Virtual Power Plant управління
Генерує дані для 1000 DER (solar + wind + battery storage)
Дані надходять кожну хвилину
"""

from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import json
import time
import random
import threading
from datetime import datetime

# === PROMETHEUS МЕТРИКИ ===

# Базові метрики DER
der_net_power = Gauge(
    'der_net_power_kw',
    'Чиста потужність DER (кВт) (+генерація/-споживання)',
    ['der_id', 'device_type', 'status']
)

der_soc = Gauge(
    'der_battery_soc_percent',
    'State of Charge батареї (%)',
    ['der_id']
)

der_available_flexibility = Gauge(
    'der_available_flexibility_kw',
    'Доступна гнучкість DER (кВт)',
    ['der_id', 'device_type']
)

der_status = Gauge(
    'der_status',
    'Статус DER (1=online, 0=offline, -1=maintenance)',
    ['der_id', 'device_type']
)

# VPP агреговані метрики
vpp_aggregated_generation = Gauge(
    'vpp_aggregated_generation_kw',
    'Агрегована генерація VPP (кВт)'
)

vpp_aggregated_consumption = Gauge(
    'vpp_aggregated_consumption_kw',
    'Агреговане споживання VPP (кВт)'
)

vpp_net_power = Gauge(
    'vpp_net_power_kw',
    'Чиста потужність VPP (кВт)'
)

vpp_total_flexibility = Gauge(
    'vpp_total_flexibility_kw',
    'Загальна доступна гнучкість VPP (кВт)'
)

vpp_battery_fleet_soc = Histogram(
    'vpp_battery_fleet_soc_percent',
    'Розподіл SOC батарейного флоту (%)',
    buckets=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
)

# Dispatch instructions compliance
vpp_setpoint_kw = Gauge(
    'vpp_setpoint_kw',
    'Setpoint для VPP (кВт)'
)

vpp_actual_power_kw = Gauge(
    'vpp_actual_power_kw',
    'Фактична потужність VPP (кВт)'
)

vpp_setpoint_deviation_kw = Gauge(
    'vpp_setpoint_deviation_kw',
    'Відхилення від setpoint (кВт)'
)

vpp_compliance_percent = Gauge(
    'vpp_compliance_percent',
    'Compliance з setpoint (%)'
)

# Dispatch instruction tracking
dispatch_instruction_compliance = Gauge(
    'vpp_dispatch_instruction_compliance',
    'Compliance з dispatch instruction (1=compliant, 0=non-compliant)'
)

# Аналітичні метрики
vpp_efficiency_percent = Gauge(
    'vpp_efficiency_percent',
    'Ефективність VPP (%)'
)

vpp_response_time_seconds = Gauge(
    'vpp_response_time_seconds',
    'Response time VPP (секунди)'
)

vpp_flexibility_utilization_percent = Gauge(
    'vpp_flexibility_utilization_percent',
    'Utilization flexibility (%)'
)

vpp_market_participation_success = Gauge(
    'vpp_market_participation_success',
    'Market participation success (1=success, 0=failure)'
)

# Лічильники
der_messages_sent = Counter(
    'der_messages_sent_total',
    'Загальна кількість відправлених повідомлень DER',
    ['device_type', 'status']
)

vpp_aggregation_updates = Counter(
    'vpp_aggregation_updates_total',
    'Кількість оновлень агрегації VPP'
)

# === КОНФІГУРАЦІЯ ===

TOTAL_DER = 1000
DEVICE_TYPES = ['solar', 'wind', 'battery', 'load']
DEVICE_DISTRIBUTION = {
    'solar': 0.35,   # 35% - 350 одиниць
    'wind': 0.25,   # 25% - 250 одиниць
    'battery': 0.25, # 25% - 250 одиниць
    'load': 0.15    # 15% - 150 одиниць
}

# Параметри генерації
SOLAR_MAX_POWER = 50  # кВт
WIND_MAX_POWER = 100  # кВт
BATTERY_MAX_POWER = 75  # кВт
BATTERY_CAPACITY = 100  # кВт·год
LOAD_MAX_POWER = 80  # кВт

# Статуси
STATUS_ONLINE = 1
STATUS_OFFLINE = 0
STATUS_MAINTENANCE = -1

# VPP параметри
SETPOINT_UPDATE_INTERVAL = 300  # 5 хвилин
COMPLIANCE_THRESHOLD = 50  # кВт
FLEXIBILITY_THRESHOLD = 100  # кВт
OFFLINE_THRESHOLD = 0.10  # 10%

# === КЛАСИ ===

class DER:
    """Клас для представлення одного DER"""
    
    def __init__(self, der_id, device_type):
        self.der_id = der_id
        self.device_type = device_type
        self.status = STATUS_ONLINE
        self.soc = 50.0 if device_type == 'battery' else None
        self.net_power = 0.0
        self.available_flexibility = 0.0
        self.last_update = time.time()
        
    def generate_data(self):
        """Генерує дані для DER"""
        # Випадковий статус (95% online, 4% offline, 1% maintenance)
        rand = random.random()
        if rand < 0.95:
            self.status = STATUS_ONLINE
        elif rand < 0.99:
            self.status = STATUS_OFFLINE
        else:
            self.status = STATUS_MAINTENANCE
        
        if self.status != STATUS_ONLINE:
            self.net_power = 0.0
            self.available_flexibility = 0.0
            return self._create_message()
        
        # Генерація потужності залежно від типу
        if self.device_type == 'solar':
            # Сонячна генерація залежить від часу доби
            hour = datetime.now().hour
            if 6 <= hour <= 18:
                solar_factor = max(0, 1 - abs(hour - 12) / 6)
                self.net_power = random.uniform(0.3, 1.0) * SOLAR_MAX_POWER * solar_factor
            else:
                self.net_power = random.uniform(0, 0.1) * SOLAR_MAX_POWER
            self.available_flexibility = self.net_power * random.uniform(0.5, 0.9)
            
        elif self.device_type == 'wind':
            # Вітрова генерація більш непередбачувана
            wind_factor = random.uniform(0.2, 1.0)
            self.net_power = WIND_MAX_POWER * wind_factor
            self.available_flexibility = self.net_power * random.uniform(0.4, 0.8)
            
        elif self.device_type == 'battery':
            # Батарея може заряжатися або розряджатися
            charge_discharge = random.choice([-1, 1])
            power = random.uniform(0.3, 1.0) * BATTERY_MAX_POWER * charge_discharge
            self.net_power = power
            
            # Оновлення SOC
            if self.soc is not None:
                soc_change = (power / BATTERY_CAPACITY) * (1/60) * 100  # зміна за хвилину
                self.soc = max(0, min(100, self.soc + soc_change))
            
            # Гнучкість батареї залежить від SOC та потужності
            if self.soc > 20 and self.soc < 80:
                self.available_flexibility = abs(power) * random.uniform(0.6, 1.0)
            else:
                self.available_flexibility = abs(power) * random.uniform(0.2, 0.5)
                
        elif self.device_type == 'load':
            # Навантаження (споживання)
            self.net_power = -random.uniform(0.2, 1.0) * LOAD_MAX_POWER
            # Гнучкість навантаження (demand response)
            self.available_flexibility = abs(self.net_power) * random.uniform(0.3, 0.7)
        
        self.last_update = time.time()
        return self._create_message()
    
    def _create_message(self):
        """Створює повідомлення для Kafka"""
        status_str = {STATUS_ONLINE: 'online', STATUS_OFFLINE: 'offline', 
                     STATUS_MAINTENANCE: 'maintenance'}[self.status]
        
        message = {
            'der_id': self.der_id,
            'device_type': self.device_type,
            'net_power_kw': round(self.net_power, 2),
            'soc_percent': round(self.soc, 2) if self.soc is not None else None,
            'available_flexibility_kw': round(self.available_flexibility, 2),
            'status': status_str,
            'timestamp': time.time()
        }
        return message

class VPPManager:
    """Менеджер Virtual Power Plant"""
    
    def __init__(self):
        self.der_list = []
        self.setpoint = 0.0
        self.last_setpoint_update = time.time()
        self.dispatch_instruction = None
        self.dispatch_compliance = True
        self.response_time_start = None
        
    def initialize_der(self):
        """Ініціалізує список DER"""
        der_id = 0
        for device_type, ratio in DEVICE_DISTRIBUTION.items():
            count = int(TOTAL_DER * ratio)
            for _ in range(count):
                self.der_list.append(DER(der_id, device_type))
                der_id += 1
        
        # Додаємо решту до останнього типу, щоб було рівно 1000
        remaining = TOTAL_DER - der_id
        for _ in range(remaining):
            self.der_list.append(DER(der_id, 'solar'))
            der_id += 1
    
    def aggregate_data(self, der_data_list):
        """Агрегує дані від усіх DER"""
        total_generation = 0.0
        total_consumption = 0.0
        total_flexibility = 0.0
        battery_soc_list = []
        online_count = 0
        offline_count = 0
        
        for der_data in der_data_list:
            net_power = der_data['net_power_kw']
            device_type = der_data['device_type']
            status = der_data['status']
            
            # Підрахунок статусів
            if status == 'online':
                online_count += 1
            elif status == 'offline':
                offline_count += 1
            
            # Агрегація потужності
            if net_power > 0:
                total_generation += net_power
            else:
                total_consumption += abs(net_power)
            
            # Агрегація гнучкості
            if status == 'online':
                total_flexibility += der_data['available_flexibility_kw']
            
            # Збір SOC батарей
            if device_type == 'battery' and der_data.get('soc_percent') is not None:
                battery_soc_list.append(der_data['soc_percent'])
        
        net_power = total_generation - total_consumption
        
        # Оновлення метрик
        vpp_aggregated_generation.set(total_generation)
        vpp_aggregated_consumption.set(total_consumption)
        vpp_net_power.set(net_power)
        vpp_total_flexibility.set(total_flexibility)
        
        # Histogram для SOC батарейного флоту (тільки якщо є батареї)
        # Спостерігаємо кожне значення SOC окремо для правильного розподілу
        if battery_soc_list:
            for soc in battery_soc_list:
                vpp_battery_fleet_soc.observe(soc)
        
        # Оновлення setpoint (кожні 5 хвилин)
        current_time = time.time()
        if current_time - self.last_setpoint_update >= SETPOINT_UPDATE_INTERVAL:
            # Setpoint генерується на основі поточної потужності ± варіація
            variation = random.uniform(-200, 200)  # кВт
            self.setpoint = net_power + variation
            vpp_setpoint_kw.set(self.setpoint)
            self.last_setpoint_update = current_time
            self.response_time_start = current_time
        
        # Розрахунок compliance
        deviation = abs(net_power - self.setpoint)
        vpp_setpoint_deviation_kw.set(deviation)
        vpp_actual_power_kw.set(net_power)
        
        # Compliance у відсотках (100% якщо відхилення < 5% від setpoint)
        if abs(self.setpoint) > 0.1:
            compliance = max(0, 100 - (deviation / abs(self.setpoint)) * 100)
        else:
            compliance = 100 if deviation < 5 else 0
        vpp_compliance_percent.set(compliance)
        
        # Dispatch instruction compliance
        if self.dispatch_instruction is not None:
            dispatch_deviation = abs(net_power - self.dispatch_instruction)
            self.dispatch_compliance = dispatch_deviation < COMPLIANCE_THRESHOLD
            dispatch_instruction_compliance.set(1 if self.dispatch_compliance else 0)
        
        # Response time (час досягнення setpoint)
        if self.response_time_start and abs(deviation) < 10:
            response_time = current_time - self.response_time_start
            vpp_response_time_seconds.set(response_time)
            self.response_time_start = None
        
        # Аналітичні метрики
        # Ефективність VPP (відношення фактичної до потенційної потужності)
        potential_power = total_generation + total_consumption
        if potential_power > 0:
            efficiency = (abs(net_power) / potential_power) * 100
            vpp_efficiency_percent.set(efficiency)
        
        # Utilization flexibility (використання гнучкості)
        if total_flexibility > 0:
            used_flexibility = min(abs(deviation), total_flexibility)
            utilization = (used_flexibility / total_flexibility) * 100
            vpp_flexibility_utilization_percent.set(utilization)
        
        # Market participation success (успіх якщо compliance > 80%)
        market_success = 1 if compliance > 80 else 0
        vpp_market_participation_success.set(market_success)
        
        vpp_aggregation_updates.inc()
        
        return {
            'aggregated_generation_kw': round(total_generation, 2),
            'aggregated_consumption_kw': round(total_consumption, 2),
            'net_power_kw': round(net_power, 2),
            'total_flexibility_kw': round(total_flexibility, 2),
            'setpoint_kw': round(self.setpoint, 2),
            'deviation_kw': round(deviation, 2),
            'compliance_percent': round(compliance, 2),
            'online_der_count': online_count,
            'offline_der_count': offline_count,
            'battery_count': len(battery_soc_list),
            'timestamp': time.time()
        }

# === KAFKA PRODUCER ===

def create_producer():
    """Створює Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_der_data(producer, der_data):
    """Відправляє дані DER в Kafka та оновлює метрики"""
    try:
        # Відправити в Kafka
        producer.send('der_telemetry', value=der_data)
        
        # Оновити Prometheus метрики
        der_net_power.labels(
            der_id=str(der_data['der_id']),
            device_type=der_data['device_type'],
            status=der_data['status']
        ).set(der_data['net_power_kw'])
        
        if der_data.get('soc_percent') is not None:
            der_soc.labels(der_id=str(der_data['der_id'])).set(der_data['soc_percent'])
        
        der_available_flexibility.labels(
            der_id=str(der_data['der_id']),
            device_type=der_data['device_type']
        ).set(der_data['available_flexibility_kw'])
        
        status_value = {'online': 1, 'offline': 0, 'maintenance': -1}[der_data['status']]
        der_status.labels(
            der_id=str(der_data['der_id']),
            device_type=der_data['device_type']
        ).set(status_value)
        
        der_messages_sent.labels(
            device_type=der_data['device_type'],
            status=der_data['status']
        ).inc()
        
    except Exception as e:
        print(f"✗ Помилка відправки DER {der_data['der_id']}: {e}")
        der_messages_sent.labels(
            device_type=der_data['device_type'],
            status='error'
        ).inc()

def send_vpp_data(producer, vpp_data):
    """Відправляє агреговані дані VPP в Kafka"""
    try:
        producer.send('vpp_aggregated', value=vpp_data)
    except Exception as e:
        print(f"✗ Помилка відправки VPP даних: {e}")

# === MAIN ===

def main():
    # Запустити HTTP сервер для Prometheus
    start_http_server(8000)
    print("✓ Prometheus метрики доступні на http://localhost:8000/metrics")
    
    # Створити producer
    producer = create_producer()
    print("✓ Kafka producer створено")
    
    # Ініціалізувати VPP менеджер
    vpp_manager = VPPManager()
    vpp_manager.initialize_der()
    print(f"✓ Ініціалізовано {len(vpp_manager.der_list)} DER")
    
    print("\n=== ПОЧАТОК ГЕНЕРАЦІЇ ДАНИХ DER ===\n")
    
    # Нескінченний цикл генерації даних (кожну хвилину)
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Ітерація {iteration} ---")
        
        # Генерація даних для всіх DER
        der_data_list = []
        for der in vpp_manager.der_list:
            der_data = der.generate_data()
            send_der_data(producer, der_data)
            der_data_list.append(der_data)
        
        # Агрегація VPP
        vpp_data = vpp_manager.aggregate_data(der_data_list)
        send_vpp_data(producer, vpp_data)
        
        print(f"VPP: Net Power = {vpp_data['net_power_kw']:.1f} кВт, "
              f"Flexibility = {vpp_data['total_flexibility_kw']:.1f} кВт, "
              f"Compliance = {vpp_data['compliance_percent']:.1f}%")
        
        # Чекати 60 секунд (1 хвилина)
        time.sleep(60)

if __name__ == '__main__':
    main()

