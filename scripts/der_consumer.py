#!/usr/bin/env python3
"""
Спеціалізований Consumer для DER системи з aggregation для Virtual Power Plant
Обробляє дані від 1000 пристроїв з фокусом на aggregation та аналіз
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
from typing import Dict, List, Any
import statistics

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DERConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Ініціалізація DER Consumer з налаштуваннями для aggregation"""
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.setup_consumer()
        
        # Структури для aggregation
        self.device_data = {}  # Останні дані по кожному пристрою
        self.aggregated_stats = {
            'total_power': 0.0,
            'generating_devices': 0,
            'consuming_devices': 0,
            'idle_devices': 0,
            'maintenance_devices': 0,
            'device_types': defaultdict(int),
            'power_by_type': defaultdict(list),
            'location_clusters': defaultdict(list)
        }
        
        # Буфери для аналізу трендів
        self.power_history = deque(maxlen=60)  # Останні 60 хвилин
        self.efficiency_history = deque(maxlen=60)
        
        # Налаштування для Virtual Power Plant
        self.vpp_thresholds = {
            'min_efficiency': 80.0,
            'max_temperature': 45.0,
            'min_voltage': 220.0,
            'max_voltage': 240.0,
            'battery_min_soc': 20.0
        }
    
    def setup_consumer(self):
        """Налаштування Consumer з оптимізацією для high-volume"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                # Налаштування для high-volume обробки
                auto_offset_reset='latest',
                group_id=f'der-vpp-consumer-{int(time.time())}',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=1000,  # Великі батчі для aggregation
                fetch_min_bytes=1024,
                fetch_max_wait_ms=2000,
                # Налаштування для Kafka 3.7.1
                api_version=(3, 7, 1),
                security_protocol='PLAINTEXT',
                # Десеріалізація
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
            )
            logger.info("DER Consumer успішно налаштований для Kafka 3.7.1")
        except Exception as e:
            logger.error(f"Помилка налаштування Consumer: {e}")
            raise
    
    def process_der_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Обробка одного запису DER з аналізом та aggregation"""
        device_id = record.get('device_id', 'unknown')
        
        # Оновлюємо останні дані пристрою
        self.device_data[device_id] = {
            'timestamp': record.get('timestamp'),
            'power_output': record.get('power_output', 0.0),
            'net_power': record.get('net_power', 0.0),
            'efficiency': record.get('efficiency', 0.0),
            'status': record.get('status', 'unknown'),
            'unit_type': record.get('unit_type', 'unknown'),
            'temperature': record.get('temperature', 0.0),
            'voltage': record.get('voltage', 0.0),
            'battery_soc': record.get('battery_soc', 0.0),
            'location': record.get('location', {}),
            'maintenance_hours': record.get('maintenance_hours', 0)
        }
        
        # Аналіз запису
        analysis = self.analyze_device_status(record)
        
        # Оновлення aggregated статистики
        self.update_aggregated_stats(record)
        
        return analysis
    
    def analyze_device_status(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Аналіз статусу пристрою та виявлення проблем"""
        device_id = record.get('device_id', 'unknown')
        power_output = record.get('power_output', 0.0)
        efficiency = record.get('efficiency', 0.0)
        temperature = record.get('temperature', 0.0)
        voltage = record.get('voltage', 0.0)
        status = record.get('status', 'unknown')
        unit_type = record.get('unit_type', 'unknown')
        battery_soc = record.get('battery_soc', 0.0)
        
        analysis = {
            'device_id': device_id,
            'timestamp': record.get('timestamp'),
            'status': status,
            'unit_type': unit_type,
            'power_output': power_output,
            'efficiency': efficiency,
            'alerts': [],
            'recommendations': [],
            'vpp_eligible': True
        }
        
        # Перевірка ефективності
        if efficiency < self.vpp_thresholds['min_efficiency']:
            analysis['alerts'].append(f"Низька ефективність: {efficiency}%")
            analysis['recommendations'].append("Потрібне технічне обслуговування")
            analysis['vpp_eligible'] = False
        
        # Перевірка температури
        if temperature > self.vpp_thresholds['max_temperature']:
            analysis['alerts'].append(f"Висока температура: {temperature}°C")
            analysis['recommendations'].append("Перевірити систему охолодження")
        
        # Перевірка напруги
        if voltage < self.vpp_thresholds['min_voltage'] or voltage > self.vpp_thresholds['max_voltage']:
            analysis['alerts'].append(f"Напруга поза межами: {voltage}В")
            analysis['recommendations'].append("Перевірити підключення до мережі")
        
        # Перевірка батареї
        if unit_type in ['battery', 'combined'] and battery_soc < self.vpp_thresholds['battery_min_soc']:
            analysis['alerts'].append(f"Низький заряд батареї: {battery_soc}%")
            analysis['recommendations'].append("Зарядка батареї")
        
        # Статус пристрою
        if status == 'maintenance':
            analysis['vpp_eligible'] = False
            analysis['recommendations'].append("Пристрій на технічному обслуговуванні")
        
        return analysis
    
    def update_aggregated_stats(self, record: Dict[str, Any]):
        """Оновлення агрегованої статистики для Virtual Power Plant"""
        power_output = record.get('power_output', 0.0)
        status = record.get('status', 'unknown')
        unit_type = record.get('unit_type', 'unknown')
        location = record.get('location', {})
        
        # Оновлення загальної потужності
        self.aggregated_stats['total_power'] += power_output
        
        # Підрахунок пристроїв за статусом
        if status == 'generating':
            self.aggregated_stats['generating_devices'] += 1
        elif status == 'consuming':
            self.aggregated_stats['consuming_devices'] += 1
        elif status == 'idle':
            self.aggregated_stats['idle_devices'] += 1
        elif status == 'maintenance':
            self.aggregated_stats['maintenance_devices'] += 1
        
        # Статистика за типами пристроїв
        self.aggregated_stats['device_types'][unit_type] += 1
        self.aggregated_stats['power_by_type'][unit_type].append(power_output)
        
        # Географічне групування (спрощене)
        if location:
            lat = location.get('lat', 0)
            lon = location.get('lon', 0)
            cluster_key = f"{int(lat)}_{int(lon)}"  # Групування по градусах
            self.aggregated_stats['location_clusters'][cluster_key].append(power_output)
        
        # Додаємо до історії для аналізу трендів
        self.power_history.append(power_output)
        self.efficiency_history.append(record.get('efficiency', 0.0))
    
    def calculate_vpp_metrics(self) -> Dict[str, Any]:
        """Розрахунок метрик Virtual Power Plant"""
        total_devices = len(self.device_data)
        
        if total_devices == 0:
            return {}
        
        # Розрахунок середніх значень
        avg_efficiency = statistics.mean(self.efficiency_history) if self.efficiency_history else 0
        avg_power = statistics.mean(self.power_history) if self.power_history else 0
        
        # Розрахунок потужності по типах
        power_by_type = {}
        for device_type, powers in self.aggregated_stats['power_by_type'].items():
            if powers:
                power_by_type[device_type] = {
                    'total': sum(powers),
                    'average': statistics.mean(powers),
                    'count': len(powers)
                }
        
        # Розрахунок географічного розподілу
        location_distribution = {}
        for cluster, powers in self.aggregated_stats['location_clusters'].items():
            if powers:
                location_distribution[cluster] = {
                    'total_power': sum(powers),
                    'device_count': len(powers),
                    'avg_power': statistics.mean(powers)
                }
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_devices': total_devices,
            'total_power_kw': round(self.aggregated_stats['total_power'], 2),
            'average_efficiency': round(avg_efficiency, 1),
            'average_power_per_device': round(avg_power, 2),
            'device_status_distribution': {
                'generating': self.aggregated_stats['generating_devices'],
                'consuming': self.aggregated_stats['consuming_devices'],
                'idle': self.aggregated_stats['idle_devices'],
                'maintenance': self.aggregated_stats['maintenance_devices']
            },
            'power_by_device_type': power_by_type,
            'location_distribution': location_distribution,
            'vpp_capacity': round(self.aggregated_stats['total_power'], 2),
            'vpp_efficiency': round(avg_efficiency, 1)
        }
    
    def print_aggregated_report(self):
        """Виведення агрегованого звіту"""
        metrics = self.calculate_vpp_metrics()
        
        if not metrics:
            print("📊 Немає даних для звіту")
            return
        
        print("\n" + "="*80)
        print("🏭 === VIRTUAL POWER PLANT AGGREGATION REPORT ===")
        print("="*80)
        print(f"🕐 Час звіту: {metrics['timestamp']}")
        print(f"📊 Загальна кількість пристроїв: {metrics['total_devices']}")
        print(f"⚡ Загальна потужність VPP: {metrics['total_power_kw']} кВт")
        print(f"📈 Середня ефективність: {metrics['average_efficiency']}%")
        print(f"🔋 Середня потужність на пристрій: {metrics['average_power_per_device']} кВт")
        
        print(f"\n📋 Розподіл пристроїв за статусом:")
        for status, count in metrics['device_status_distribution'].items():
            print(f"   {status}: {count} пристроїв")
        
        print(f"\n🔧 Потужність по типах пристроїв:")
        for device_type, stats in metrics['power_by_device_type'].items():
            print(f"   {device_type}: {stats['total']:.2f} кВт ({stats['count']} пристроїв)")
        
        print(f"\n🌍 Географічний розподіл:")
        for cluster, stats in metrics['location_distribution'].items():
            print(f"   Кластер {cluster}: {stats['total_power']:.2f} кВт ({stats['device_count']} пристроїв)")
        
        print(f"\n🎯 VPP Характеристики:")
        print(f"   Загальна потужність: {metrics['vpp_capacity']} кВт")
        print(f"   Ефективність VPP: {metrics['vpp_efficiency']}%")
        
        print("="*80)
    
    def print_device_analysis(self, analysis: Dict[str, Any]):
        """Виведення аналізу окремого пристрою"""
        device_id = analysis['device_id']
        status = analysis['status']
        unit_type = analysis['unit_type']
        power_output = analysis['power_output']
        efficiency = analysis['efficiency']
        
        print(f"\n🔌 Пристрій: {device_id} ({unit_type})")
        print(f"   Статус: {status}")
        print(f"   Потужність: {power_output} кВт")
        print(f"   Ефективність: {efficiency}%")
        print(f"   VPP Ready: {'✅' if analysis['vpp_eligible'] else '❌'}")
        
        if analysis['alerts']:
            print(f"   🚨 Попередження:")
            for alert in analysis['alerts']:
                print(f"      - {alert}")
        
        if analysis['recommendations']:
            print(f"   💡 Рекомендації:")
            for rec in analysis['recommendations']:
                print(f"      - {rec}")
    
    def consume_der_data(self, topics: List[str], duration_minutes: int = 10):
        """Основна функція споживання даних з aggregation"""
        logger.info(f"Початок споживання даних з топіків: {topics}")
        logger.info(f"Тривалість: {duration_minutes} хвилин")
        
        # Підписуємося на топіки
        self.consumer.subscribe(topics)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        message_count = 0
        
        print("🚀 DER Consumer запущено!")
        print("📡 Очікуємо дані від DER пристроїв...")
        print("🛑 Натисніть Ctrl+C для зупинки\n")
        
        try:
            while time.time() < end_time:
                # Отримуємо повідомлення батчами
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            message_count += 1
                            
                            # Обробляємо запис
                            analysis = self.process_der_record(message.value)
                            
                            # Виводимо аналіз кожного 10-го пристрою для економії виводу
                            if message_count % 10 == 0:
                                self.print_device_analysis(analysis)
                
                # Виводимо агрегований звіт кожні 30 секунд
                if int(time.time() - start_time) % 30 == 0:
                    self.print_aggregated_report()
                    time.sleep(1)  # Щоб не виводити звіт кілька разів за секунду
        
        except KeyboardInterrupt:
            print(f"\n🛑 Consumer зупинено користувачем")
        
        # Фінальний звіт
        print(f"\n📊 === ФІНАЛЬНИЙ ЗВІТ ===")
        print(f"Оброблено повідомлень: {message_count}")
        print(f"Унікальних пристроїв: {len(self.device_data)}")
        self.print_aggregated_report()
    
    def close(self):
        """Закриття Consumer"""
        if self.consumer:
            self.consumer.close()
            logger.info("DER Consumer закрито")

def main():
    """Основна функція"""
    print("=== DER Virtual Power Plant Consumer ===")
    print("Спеціалізований консюмер для aggregation та аналізу DER даних")
    print()
    
    consumer = DERConsumer()
    
    try:
        # Споживання з обох топіків
        topics = ['der-main', 'der-batch-test']
        
        # Запускаємо споживання на 10 хвилин
        consumer.consume_der_data(topics, duration_minutes=10)
        
    except KeyboardInterrupt:
        print("\nПереривання користувачем...")
    except Exception as e:
        logger.error(f"Помилка виконання: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
