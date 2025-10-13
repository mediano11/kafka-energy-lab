#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Аналіз впливу схеми партиціонування (generator_type, region) на рівномірність розподілу
"""

import time
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import numpy as np
from collections import defaultdict

class PartitioningAnalyzer:
    def __init__(self, host='localhost', port=9042):
        self.host = host
        self.port = port
        self.cluster = None
        self.session = None
    
    def connect(self):
        """Підключення до Cassandra"""
        try:
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect('der_monitoring')
            print("✅ Підключено до Cassandra")
            return True
        except Exception as e:
            print(f"❌ Помилка підключення: {e}")
            return False
    
    def analyze_partition_balance(self):
        """Аналіз балансу партицій"""
        print("\n⚖️ АНАЛІЗ БАЛАНСУ ПАРТИЦІЙ")
        print("=" * 50)

        try:
            # Витягуємо всі generator_id та рахуємо кількість записів
            rows = self.session.execute("SELECT generator_id FROM generator_operational_data")
            
            # Рахуємо кількість записів на кожен generator_id
            partition_sizes = defaultdict(int)
            for row in rows:
                partition_sizes[row.generator_id] += 1

            if not partition_sizes:
                print("⚠️ Немає даних для аналізу")
                return

            # Сортуємо за кількістю записів
            sorted_partitions = sorted(partition_sizes.items(), key=lambda x: x[1], reverse=True)

            print("📊 Топ-10 найбільших партицій:")
            for gen_id, count in sorted_partitions[:10]:
                print(f"  Generator {str(gen_id)[:8]}...: {count} записів")

            sizes = list(partition_sizes.values())

            print("\n📈 Статистика розмірів партицій:")
            print(f"  Максимальний розмір: {max(sizes)}")
            print(f"  Мінімальний розмір: {min(sizes)}")
            print(f"  Середній розмір: {np.mean(sizes):.2f}")
            print(f"  Медіана: {np.median(sizes):.2f}")
            print(f"  Стандартне відхилення: {np.std(sizes):.2f}")

            cv = np.std(sizes) / np.mean(sizes) * 100
            print(f"  Коефіцієнт варіації: {cv:.2f}%")

            if cv < 20:
                print("  ✅ Партиції добре збалансовані (CV < 20%)")
            elif cv < 50:
                print("  ⚠️ Партиції помірно збалансовані (20% < CV < 50%)")
            else:
                print("  ❌ Партиції погано збалансовані (CV > 50%)")
        
        except Exception as e:
            print(f"❌ Помилка аналізу: {e}")

    def analyze_schema_impact(self):
        """Аналіз впливу схеми (generator_type, region)"""
        print("\n🏗️ АНАЛІЗ ВПЛИВУ СХЕМИ (GENERATOR_TYPE, REGION)")
        print("=" * 60)

        try:
            # Отримуємо комбінації source_type + region
            rows = self.session.execute("SELECT source_type, region FROM generator_sources")
            combo_counts = defaultdict(int)
            for row in rows:
                combo_counts[(row.source_type, row.region)] += 1

            values = list(combo_counts.values())
            for (stype, region), count in combo_counts.items():
                print(f"  {stype} + {region}: {count} генераторів")

            if values:
                print(f"\n📈 Статистика комбінацій:")
                print(f"  Максимальна кількість: {max(values)}")
                print(f"  Мінімальна кількість: {min(values)}")
                print(f"  Середня кількість: {np.mean(values):.2f}")
                print(f"  Стандартне відхилення: {np.std(values):.2f}")
                cv = np.std(values) / np.mean(values) * 100
                print(f"  Коефіцієнт варіації: {cv:.2f}%")

                if cv < 30:
                    print("  ✅ Схема (generator_type, region) забезпечує рівномірний розподіл")
                elif cv < 60:
                    print("  ⚠️ Схема (generator_type, region) забезпечує помірно рівномірний розподіл")
                else:
                    print("  ❌ Схема (generator_type, region) не забезпечує рівномірний розподіл")
        
        except Exception as e:
            print(f"❌ Помилка аналізу: {e}")

    def analyze_partition_distribution(self):
        print("\n🔍 АНАЛІЗ РОЗПОДІЛУ ПАРТИЦІЙ")
        print("=" * 50)

        print("📊 Розподіл записів по generator_id:")
        rows = self.session.execute("SELECT generator_id FROM generator_operational_data")
        generator_counts = defaultdict(int)

        for row in rows:
            generator_counts[row.generator_id] += 1

        gen_counts = list(generator_counts.values())
        for gen_id, count in generator_counts.items():
            print(f"  Generator {str(gen_id)[:8]}...: {count} записів")

        if gen_counts:
            print(f"  Середня кількість записів на генератор: {np.mean(gen_counts):.2f}")
            print(f"  Стандартне відхилення: {np.std(gen_counts):.2f}")
            print(f"  Коефіцієнт варіації: {np.std(gen_counts) / np.mean(gen_counts) * 100:.2f}%")

        # Регіони
        print("\n🌍 Розподіл по регіонах:")
        rows = self.session.execute("SELECT region FROM generator_sources")
        region_counts = defaultdict(int)
        for row in rows:
            region_counts[row.region] += 1

        region_values = list(region_counts.values())
        for region, count in region_counts.items():
            print(f"  {region}: {count} генераторів")
        if region_values:
            print(f"  Середня кількість генераторів на регіон: {np.mean(region_values):.2f}")
            print(f"  Стандартне відхилення: {np.std(region_values):.2f}")
            print(f"  Коефіцієнт варіації: {np.std(region_values) / np.mean(region_values) * 100:.2f}%")

        # Типи джерел
        print("\n⚡ Розподіл по типах джерел:")
        rows = self.session.execute("SELECT source_type FROM generator_sources")
        source_counts = defaultdict(int)
        for row in rows:
            source_counts[row.source_type] += 1

        source_values = list(source_counts.values())
        for stype, count in source_counts.items():
            print(f"  {stype}: {count} генераторів")
        if source_values:
            print(f"  Середня кількість генераторів на тип: {np.mean(source_values):.2f}")
            print(f"  Стандартне відхилення: {np.std(source_values):.2f}")
            print(f"  Коефіцієнт варіації: {np.std(source_values) / np.mean(source_values) * 100:.2f}%")
    
    def analyze_query_performance(self):
        """Аналіз продуктивності запитів"""
        print("\n⚡ АНАЛІЗ ПРОДУКТИВНОСТІ ЗАПИТІВ")
        print("=" * 50)

        # Отримуємо перші 5 generator_id для тесту
        try:
            test_generators = self.session.execute("SELECT generator_id FROM generator_sources LIMIT 5")
            test_gen_list = [row.generator_id for row in test_generators]
        except Exception as e:
            print(f"❌ Помилка при отриманні generator_id: {e}")
            return

        if not test_gen_list:
            print("❌ Немає генераторів для тестування")
            return

        # --- Тест 1: Швидкий запит по partition key ---
        print("🔍 Тест 1: Запит по partition key (generator_id)")
        times_partition = []
        for gen_id in test_gen_list[:3]:  # Тестуємо перших 3
            start = time.time()
            result = self.session.execute("""
                SELECT COUNT(*) FROM generator_operational_data 
                WHERE generator_id = %s
            """, (gen_id,))
            end = time.time()
            times_partition.append(end - start)
            print(f"  Generator {str(gen_id)[:8]}...: {end - start:.4f}с")

        avg_partition = np.mean(times_partition)
        print(f"  Середній час: {avg_partition:.4f}с")

        # --- Тест 2: Запит по регіону (через JOIN) ---
        print("\n🔍 Тест 2: Запит по region (через JOIN)")
        regions = ['Kyiv', 'Lviv', 'Kharkiv']
        times_region = []
        for region in regions:
            start = time.time()
            generator_ids = self.session.execute("""
                SELECT generator_id FROM generator_sources WHERE region = %s
            """, (region,))
            gen_ids = [row.generator_id for row in generator_ids]

            total_records = 0
            for gid in gen_ids:
                result = self.session.execute("""
                    SELECT COUNT(*) FROM generator_operational_data WHERE generator_id = %s
                """, (gid,))
                total_records += result.one()[0]

            end = time.time()
            times_region.append(end - start)
            print(f"  Region {region}: {total_records} записів, {end - start:.4f}с")

        avg_region = np.mean(times_region)
        print(f"  Середній час: {avg_region:.4f}с")

        # --- Тест 3: Запит по source_type (через JOIN) ---
        print("\n🔍 Тест 3: Запит по source_type (через JOIN)")
        source_types = ['solar', 'wind', 'biomass']
        times_source = []
        for stype in source_types:
            start = time.time()
            generator_ids = self.session.execute("""
                SELECT generator_id FROM generator_sources WHERE source_type = %s
            """, (stype,))
            gen_ids = [row.generator_id for row in generator_ids]

            total_records = 0
            for gid in gen_ids:
                result = self.session.execute("""
                    SELECT COUNT(*) FROM generator_operational_data WHERE generator_id = %s
                """, (gid,))
                total_records += result.one()[0]

            end = time.time()
            times_source.append(end - start)
            print(f"  Source type {stype}: {total_records} записів, {end - start:.4f}с")

        avg_source = np.mean(times_source)
        print(f"  Середній час: {avg_source:.4f}с")

        # --- Порівняння ---
        print(f"\n📊 ПОРІВНЯННЯ ПРОДУКТИВНОСТІ:")
        print(f"  Partition key (generator_id): {avg_partition:.4f}с")
        print(f"  Region JOIN: {avg_region:.4f}с (повільніше в {avg_region / avg_partition:.1f}x)")
        print(f"  Source type JOIN: {avg_source:.4f}с (повільніше в {avg_source / avg_partition:.1f}x)")

    
    def generate_recommendations(self):
        """Генерація рекомендацій по оптимізації"""
        print("\n💡 РЕКОМЕНДАЦІЇ ПО ОПТИМІЗАЦІЇ")
        print("=" * 50)
        
        print("1. 🎯 Оптимізація partition key:")
        print("   - Використовуйте generator_id як partition key для рівномірного розподілу")
        print("   - Уникайте \"hot partitions\" - одна партиція з великою кількістю записів")
        
        print("\n2. 🔍 Оптимізація запитів:")
        print("   - Запити по partition key (generator_id) найшвидші")
        print("   - JOIN запити повільніші, використовуйте індекси")
        print("   - Для аналітичних запитів створіть додаткові таблиці з агрегованими даними")
        
        print("\n3. 📊 Оптимізація схеми:")
        print("   - Схема (generator_type, region) корисна для аналітичних запитів")
        print("   - Створіть додаткові індекси для часто використовуваних полів")
        print("   - Розгляньте створення матеріалізованих представлень для складних запитів")
        
        print("\n4. ⚡ Рекомендації по продуктивності:")
        print("   - Використовуйте batch операції для масових вставок")
        print("   - Налаштуйте consistency level відповідно до потреб")
        print("   - Моніторьте розмір партицій (рекомендовано < 100MB)")
    
    def close(self):
        """Закриття з'єднання"""
        if self.cluster:
            self.cluster.shutdown()

def main():
    """Головна функція аналізу"""
    print("🔍 АНАЛІЗ СХЕМИ ПАРТИЦІОНУВАННЯ DER СИСТЕМИ")
    print("=" * 60)
    
    analyzer = PartitioningAnalyzer()
    
    try:
        if not analyzer.connect():
            return
        
        # Виконання аналізів
        analyzer.analyze_partition_distribution()
        analyzer.analyze_query_performance()
        analyzer.analyze_partition_balance()
        analyzer.analyze_schema_impact()
        analyzer.generate_recommendations()
        
        print("\n✅ АНАЛІЗ ЗАВЕРШЕНО!")
        
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
    
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
