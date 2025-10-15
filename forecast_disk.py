#!/usr/bin/env python3
"""
розраховує споживання дискового простору за рік (з захардкоженими значеннями)
"""

import logging
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryAnalyzer:
    def __init__(self, hosts=['127.0.0.1'], port=9042):
        self.hosts = hosts
        self.port = port
        self.cluster = None
        self.session = None

    def connect_to_cassandra(self):
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
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Відключено від Cassandra")

    def calculate_disk_usage_per_year(self):
        """
        Прогноз споживання дискового простору за рік (на основі місячних даних)
        """

        device_count = 30
        records_per_minute = 1
        record_size_bytes = 256
        compression_ratio = 0.4

        # Розрахунки за рік
        days_per_year = 365
        minutes_per_year = days_per_year * 24 * 60 # 525600
        records_per_year = device_count * minutes_per_year * records_per_minute

        # Теоретичний розмір (байти)
        theoretical_size_bytes = records_per_year * record_size_bytes

        # Переведення у GB (1 GB = 1024^3 байт)
        theoretical_size_gb = theoretical_size_bytes / (1024 ** 3)

        # actual_size_bytes = theoretical_size_bytes * compression_ratio
        # actual_size_gb = theoretical_size_gb / (1024 ** 3)

        months = 12 

        base_data = {
            'simple': {
                'total_size_gb': 0.03792,
                'partitions': 60,
                'avg_partition_size_mb': 0.64722,
                'max_partition_size_mb': 0.97082,
                'compression_ratio': 0.120
            },
            'hourly': {
                'total_size_gb': 0.04054,
                'partitions': 21751,
                'avg_partition_size_mb': 0.00191,
                'max_partition_size_mb': 0.00286,
                'compression_ratio': 0.128
            },
            'daily': {
                'total_size_gb': 0.03651,
                'partitions': 930,
                'avg_partition_size_mb': 0.04020,
                'max_partition_size_mb': 0.06030,
                'compression_ratio': 0.115
            }
        }

        results = {}

        for schema, data in base_data.items():
            results[schema] = {
                'total_size_gb': data['total_size_gb'] * months,
                'partitions': data['partitions'] * months,
                'avg_partition_size_mb': data['avg_partition_size_mb'], 
                'max_partition_size_mb': data['max_partition_size_mb'], 
                'compression_ratio': data['compression_ratio']
            }

        actual_size_gb = sum([data['total_size_gb'] for data in results.values()])

        return results, theoretical_size_gb, actual_size_gb

    def print_disk_usage_analysis(self):
        print("\n" + "="*80)
        print("СПОЖИВАННЯ ДИСКОВОГО ПРОСТОРУ ЗА РІК")
        print("="*80)

        results, theoretical_gb, actual_gb = self.calculate_disk_usage_per_year()

        # Таблиця 1: Розрахунок за рік
        print("\nТаблиця 1: Розрахунок за рік")
        print("-" * 80)
        print(f"{'Параметр':<30} {'Значення':<20}")
        print("-" * 80)
        print(f"{'Кількість пристроїв':<30} {'30':<20}")
        print(f"{'Записів на хвилину':<30} {'1':<20}")
        print(f"{'Записів за рік':<30} {'15,768,000':<20}")
        print(f"{'Теоретичний розмір (GB)':<30} {'{:.2f}'.format(theoretical_gb):<20}")
        print(f"{'Фактичний розмір (GB)':<30} {'{:.2f}'.format(actual_gb):<20}")

        # Таблиця 2: Порівняння схем
        print("\nТаблиця 2: Порівняння схем за рік")
        print("-" * 80)
        print(f"{'Схема':<15} {'Total Size (GB)':<18} {'Partitions':<12} {'Avg Partition (MB)':<20} {'Max Partition (MB)':<20}")
        print("-" * 80)

        for schema, data in results.items():
            print(f"{schema:<15} {data['total_size_gb']:<18.5f} {data['partitions']:<12} "
                  f"{data['avg_partition_size_mb']:<20.5f} {data['max_partition_size_mb']:<20.5f}")


        print("Економія дискового простору:")
        print(f"   - Теоретичний розмір: {theoretical_gb:.2f} GB")
        print(f"   - Фактичний розмір: {actual_gb:.2f} GB")
        print(f"   - Економія: {((theoretical_gb - actual_gb) / theoretical_gb * 100):.1f}%")
        print("   - Рекомендовано: LZ4 стиснення для балансу швидкості/економії")


        
def main():
    logger.info("Запуск аналізу")
    analyzer = QueryAnalyzer()

    try:
        if not analyzer.connect_to_cassandra():
            logger.error("Не вдалося підключитися до Cassandra")
            return

        analyzer.print_disk_usage_analysis()

    except Exception as e:
        logger.error(f"Помилка під час аналізу: {e}")
    finally:
        analyzer.disconnect()

if __name__ == "__main__":
    main()
