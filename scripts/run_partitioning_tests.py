#!/usr/bin/env python3
"""
Автоматизований запуск тестів партиціонування
Координує роботу producer та consumer для повного тестування
"""

import subprocess
import time
import json
import threading
from datetime import datetime
import os
import sys

def create_partitioning_topics():
    """Створює топіки для тестів партиціонування"""
    print("📁 Створення топіків для тестів партиціонування...")
    
    topics = [
        ('der-part-10', 10),
        ('der-part-15', 15),
        ('der-part-20', 20)
    ]
    
    for topic_name, partitions in topics:
        try:
            result = subprocess.run([
                './kafka/bin/kafka-topics.sh', '--create',
                '--bootstrap-server', 'localhost:9092',
                '--topic', topic_name,
                '--partitions', str(partitions),
                '--replication-factor', '1'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print(f"   ✅ Топік {topic_name} створено ({partitions} партицій)")
            else:
                print(f"   ⚠️ Топік {topic_name} вже існує або помилка: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ❌ Таймаут створення топіку {topic_name}")
        except Exception as e:
            print(f"   ❌ Помилка створення топіку {topic_name}: {e}")

def run_partitioning_producer_tests():
    """Запускає тести partitioning producer"""
    print("🚀 Запуск тестів Partitioning Producer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/partitioning_test_producer.py"
        ], capture_output=True, text=True, timeout=1200)  # 20 хвилин timeout
        
        print("Partitioning Producer тести завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Producer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Partitioning Producer тести перевищили час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Partitioning Producer тестів: {e}")
        return False

def run_partitioning_metrics_consumer():
    """Запускає consumer для збору partitioning метрик"""
    print("📊 Запуск Partitioning Metrics Consumer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/partitioning_metrics_consumer.py"
        ], capture_output=True, text=True, timeout=1500)  # 25 хвилин timeout
        
        print("Partitioning Metrics Consumer завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Consumer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Partitioning Metrics Consumer перевищив час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Partitioning Metrics Consumer: {e}")
        return False

def analyze_partitioning_results():
    """Аналізує результати тестів партиціонування та створює звіт"""
    print("📈 Аналіз результатів тестів партиціонування...")
    
    # Читаємо результати producer
    producer_results = []
    if os.path.exists("data/partitioning_test_results.json"):
        try:
            with open("data/partitioning_test_results.json", 'r', encoding='utf-8') as f:
                producer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів producer: {e}")
    
    # Читаємо результати consumer
    consumer_results = {}
    if os.path.exists("data/partitioning_metrics.json"):
        try:
            with open("data/partitioning_metrics.json", 'r', encoding='utf-8') as f:
                consumer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів consumer: {e}")
    
    # Створюємо звіт
    create_partitioning_final_report(producer_results, consumer_results)

def create_partitioning_final_report(producer_results, consumer_results):
    """Створює фінальний звіт з результатами партиціонування"""
    print("\n" + "="*120)
    print("📊 ФІНАЛЬНИЙ ЗВІТ: ТЕСТУВАННЯ ПАРТИЦІОНУВАННЯ")
    print("="*120)
    
    # Таблиця результатів
    print("\nТаблиця 3 - Scaling results")
    print("| Партиції | Стратегія | Records/sec | Avg Latency (ms) | P50 Latency (ms) | P95 Latency (ms) | Scaling Factor | Ефективність |")
    print("|----------|-----------|-------------|------------------|------------------|------------------|----------------|--------------|")
    
    valid_results = [r for r in producer_results if 'error' not in r]
    
    if not valid_results:
        print("❌ Немає валідних результатів для відображення")
        print("Можливі причини:")
        print("- Всі тести завершилися з помилками")
        print("- Файл результатів порожній або пошкоджений")
        print("- Проблеми з підключенням до Kafka")
        return
    
    # Розраховуємо scaling factors
    baseline_throughput = 0
    scaling_factors = {}
    
    # Знаходимо baseline (10 партицій)
    baseline_results = [r for r in valid_results if r['num_partitions'] == 10]
    if baseline_results:
        baseline_throughput = sum(r['throughput_records_per_sec'] for r in baseline_results) / len(baseline_results)
        scaling_factors[10] = 1.0  # Baseline
    
    # Розраховуємо scaling factors для інших конфігурацій
    for num_partitions in [15, 20]:
        partition_results = [r for r in valid_results if r['num_partitions'] == num_partitions]
        if partition_results and baseline_throughput > 0:
            avg_throughput = sum(r['throughput_records_per_sec'] for r in partition_results) / len(partition_results)
            scaling_factors[num_partitions] = avg_throughput / baseline_throughput
        else:
            scaling_factors[num_partitions] = 0.0
    
    for result in valid_results:
        num_partitions = result['num_partitions']
        strategy = result['strategy']
        throughput = result['throughput_records_per_sec']
        avg_latency = result['avg_latency_ms']
        p50_latency = result['p50_latency_ms']
        p95_latency = result['p95_latency_ms']
        scaling_factor = scaling_factors.get(num_partitions, 0.0)
        
        # Визначаємо ефективність
        if scaling_factor >= 1.5:
            efficiency = "Відмінно"
        elif scaling_factor >= 1.2:
            efficiency = "Добре"
        elif scaling_factor >= 1.0:
            efficiency = "Задовільно"
        else:
            efficiency = "Погано"
        
        print(f"| {num_partitions:<8} | {strategy:<9} | {throughput:<11} | {avg_latency:<16} | {p50_latency:<16} | {p95_latency:<16} | {scaling_factor:<14.2f}x | {efficiency:<12} |")
    
    print("|----------|-----------|-------------|------------------|------------------|------------------|----------------|--------------|")
    
    # Аналіз найкращих результатів
    if valid_results:
        print(f"\n🏆 КЛЮЧОВІ ВИСНОВКИ:")
        
        max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
        min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
        best_balance = max(valid_results, key=lambda x: x['partition_balance_score'])
        
        print(f"Max throughput: {max_throughput['num_partitions']} партицій, {max_throughput['strategy']} → {max_throughput['throughput_records_per_sec']} rec/sec")
        print(f"Min latency: {min_latency['num_partitions']} партицій, {min_latency['strategy']} → {min_latency['avg_latency_ms']} ms")
        print(f"Best balance: {best_balance['num_partitions']} партицій, {best_balance['strategy']} → {best_balance['partition_balance_score']:.2f}")
        
        # Аналіз масштабування
        print(f"\n📈 АНАЛІЗ МАСШТАБУВАННЯ:")
        print(f"Baseline (10 партицій): {baseline_throughput:.2f} rec/sec")
        
        for num_partitions in [15, 20]:
            partition_results = [r for r in valid_results if r['num_partitions'] == num_partitions]
            if partition_results:
                avg_throughput = sum(r['throughput_records_per_sec'] for r in partition_results) / len(partition_results)
                scaling_factor = scaling_factors.get(num_partitions, 0.0)
                efficiency = "Відмінно" if scaling_factor >= 1.5 else "Добре" if scaling_factor >= 1.2 else "Задовільно" if scaling_factor >= 1.0 else "Погано"
                print(f"{num_partitions} партицій → {avg_throughput:.2f} rec/sec (Scaling: {scaling_factor:.2f}x, {efficiency})")
        
        # SCADA рекомендації
        print(f"\n🏭 SCADA РЕКОМЕНДАЦІЇ:")
        scada_results = [r for r in valid_results if r['p95_latency_ms'] <= 10]  # P95 < 10ms
        if scada_results:
            best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
            print(f"Оптимальний для SCADA: {best_scada['num_partitions']} партицій, {best_scada['strategy']}")
            print(f"P95 Latency: {best_scada['p95_latency_ms']} ms")
            print(f"Throughput: {best_scada['throughput_records_per_sec']} rec/sec")
        
        # Ultra-low latency рекомендації
        print(f"\n⚡ ULTRA-LOW LATENCY:")
        ultra_results = [r for r in valid_results if r['avg_latency_ms'] <= 5]  # Avg < 5ms
        if ultra_results:
            best_ultra = min(ultra_results, key=lambda x: x['avg_latency_ms'])
            print(f"Для критичних систем: {best_ultra['num_partitions']} партицій, {best_ultra['strategy']}")
            print(f"Avg Latency: {best_ultra['avg_latency_ms']} ms")
        else:
            print("Рекомендується мінімальна кількість партицій для найнижчої latency")
        
        # Оптимальний баланс
        print(f"\n⚖️ ОПТИМАЛЬНИЙ БАЛАНС:")
        balanced_results = []
        for result in valid_results:
            if result['avg_latency_ms'] > 0:
                balance_score = result['throughput_records_per_sec'] / result['avg_latency_ms']
                balanced_results.append((result, balance_score))
        
        if balanced_results:
            best_balanced = max(balanced_results, key=lambda x: x[1])[0]
            print(f"Для DER системи: {best_balanced['num_partitions']} партицій, {best_balanced['strategy']}")
            print(f"Throughput: {best_balanced['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {best_balanced['avg_latency_ms']} ms")
            print(f"Balance Score: {best_balanced['partition_balance_score']:.2f}")
        
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
        
        # Оптимальна кількість партицій
        print(f"\n🎯 ОПТИМАЛЬНА КІЛЬКІСТЬ ПАРТИЦІЙ:")
        best_overall = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
        print(f"Оптимальна кількість: {best_overall['num_partitions']} партицій для DER системи")
        print(f"Стратегія: {best_overall['strategy']}")
        print(f"Throughput: {best_overall['throughput_records_per_sec']} rec/sec")
        print(f"Scaling Factor: {scaling_factors.get(best_overall['num_partitions'], 0.0):.2f}x")
        
        # Зберігаємо звіт у файл
        save_partitioning_report_to_file(valid_results, scaling_factors)

def save_partitioning_report_to_file(results, scaling_factors):
    """Зберігає partitioning звіт у файл"""
    try:
        # Створюємо папку data якщо не існує
        os.makedirs("data", exist_ok=True)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_type': 'partitioning_scaling',
            'test_results': results,
            'scaling_factors': scaling_factors,
            'summary': {
                'max_throughput': max(results, key=lambda x: x['throughput_records_per_sec']),
                'min_latency': min(results, key=lambda x: x['avg_latency_ms']),
                'best_balance': max(results, key=lambda x: x['partition_balance_score'])
            }
        }
        
        with open("data/partitioning_test_final_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Partitioning звіт збережено у файл: data/partitioning_test_final_report.json")
    except Exception as e:
        print(f"❌ Помилка збереження partitioning звіту: {e}")

def main():
    """Основна функція автоматизованого partitioning тестування"""
    print("="*80)
    print("🚀 АВТОМАТИЗОВАНЕ ТЕСТУВАННЯ ПАРТИЦІОНУВАННЯ")
    print("="*80)
    print("Цей скрипт виконає повне тестування різних кількостей партицій")
    print("для оптимізації DER системи з різними стратегіями партиціонування")
    print()
    
    start_time = time.time()
    
    try:
        # Крок 1: Створення топіків
        print("📁 Крок 1: Створення топіків...")
        create_partitioning_topics()
        
        # Крок 2: Запуск Partitioning Metrics Consumer в окремому потоці
        print("📊 Крок 2: Запуск Partitioning Metrics Consumer...")
        consumer_thread = threading.Thread(target=run_partitioning_metrics_consumer)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Чекаємо трохи, щоб consumer підключився
        time.sleep(5)
        
        # Крок 3: Запуск Partitioning Producer тестів
        print("🚀 Крок 3: Запуск Partitioning Producer тестів...")
        producer_success = run_partitioning_producer_tests()
        
        # Чекаємо завершення consumer
        print("⏳ Очікування завершення Partitioning Metrics Consumer...")
        consumer_thread.join(timeout=900)  # 15 хвилин timeout
        
        # Крок 4: Аналіз результатів
        print("📈 Крок 4: Аналіз результатів...")
        analyze_partitioning_results()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"\n✅ ПАРТИЦІОНУВАННЯ ТЕСТУВАННЯ ЗАВЕРШЕНО!")
        print(f"Загальна тривалість: {total_duration:.1f} секунд ({total_duration/60:.1f} хвилин)")
        print(f"Результати збережено у файлах:")
        print(f"  - data/partitioning_test_results.json (результати producer)")
        print(f"  - data/partitioning_metrics.json (метрики consumer)")
        print(f"  - data/partitioning_test_final_report.json (фінальний звіт)")
        
    except KeyboardInterrupt:
        print("\n🛑 Partitioning тестування перервано користувачем")
    except Exception as e:
        print(f"\n❌ Помилка під час partitioning тестування: {e}")
    finally:
        print("\nPartitioning тестування завершено.")

if __name__ == "__main__":
    main()
