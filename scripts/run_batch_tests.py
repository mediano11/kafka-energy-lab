#!/usr/bin/env python3
"""
Автоматизований запуск тестів batch.size та linger.ms
Координує роботу producer та consumer для повного тестування
"""

import subprocess
import time
import json
import threading
from datetime import datetime
import os
import sys

def run_producer_tests():
    """Запускає тести producer"""
    print("🚀 Запуск тестів Producer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/batch_test_producer.py"
        ], capture_output=True, text=True, timeout=600)  # 10 хвилин timeout
        
        print("Producer тести завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Producer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Producer тести перевищили час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Producer тестів: {e}")
        return False

def run_metrics_consumer():
    """Запускає consumer для збору метрик"""
    print("📊 Запуск Metrics Consumer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/metrics_consumer.py"
        ], capture_output=True, text=True, timeout=900)  # 15 хвилин timeout
        
        print("Metrics Consumer завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Consumer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Metrics Consumer перевищив час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Metrics Consumer: {e}")
        return False

def analyze_results():
    """Аналізує результати тестів та створює звіт"""
    print("📈 Аналіз результатів тестів...")
    
    # Читаємо результати producer
    producer_results = []
    if os.path.exists("batch_test_results.json"):
        try:
            with open("batch_test_results.json", 'r', encoding='utf-8') as f:
                producer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів producer: {e}")
    
    # Читаємо результати consumer
    consumer_results = {}
    if os.path.exists("consumer_metrics.json"):
        try:
            with open("consumer_metrics.json", 'r', encoding='utf-8') as f:
                consumer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів consumer: {e}")
    
    # Створюємо звіт
    create_final_report(producer_results, consumer_results)

def create_final_report(producer_results, consumer_results):
    """Створює фінальний звіт з результатами"""
    print("\n" + "="*100)
    print("📊 ФІНАЛЬНИЙ ЗВІТ: ТЕСТУВАННЯ BATCH.SIZE ТА LINGER.MS")
    print("="*100)
    
    # Таблиця результатів
    print("\nТаблиця 1 - Ключові результати (9 тестів)")
    print("| Конфігурація | Records/sec | Avg Latency (ms) | P95 Latency (ms) | Success Rate (%) | Використання |")
    print("|--------------|-------------|------------------|------------------|------------------|--------------|")
    
    valid_results = [r for r in producer_results if 'error' not in r]
    
    for result in valid_results:
        config = result['config_name']
        throughput = result['throughput_records_per_sec']
        avg_latency = result['avg_latency_ms']
        p95_latency = result['p95_latency_ms']
        success_rate = result['success_rate']
        
        # Визначаємо тип використання
        if result['linger_ms'] == 0:
            usage = "Real-time"
        elif result['linger_ms'] <= 10:
            usage = "Balanced"
        else:
            usage = "Batch"
        
        print(f"| {config:<12} | {throughput:<11} | {avg_latency:<16} | {p95_latency:<16} | {success_rate:<16.1f} | {usage:<12} |")
    
    print("|--------------|-------------|------------------|------------------|------------------|--------------|")
    
    # Аналіз найкращих результатів
    if valid_results:
        print(f"\n🏆 НАЙКРАЩІ РЕЗУЛЬТАТИ:")
        
        max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
        min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
        
        print(f"Max throughput: {max_throughput['config_name']} → {max_throughput['throughput_records_per_sec']} rec/sec")
        print(f"Min latency: {min_latency['config_name']} → {min_latency['avg_latency_ms']} ms")
        
        # Оптимальний баланс для DER системи
        balanced_results = [r for r in valid_results if 10 <= r['linger_ms'] <= 50 and r['batch_size'] >= 65536]
        if balanced_results:
            optimal = max(balanced_results, key=lambda x: x['throughput_records_per_sec'] / max(x['avg_latency_ms'], 1))
            print(f"Оптимальний баланс: {optimal['config_name']} для aggregation 1000 DER пристроїв")
        
        # Аналіз по типах використання
        print(f"\n📊 АНАЛІЗ ПО ТИПАХ ВИКОРИСТАННЯ:")
        
        realtime_results = [r for r in valid_results if r['linger_ms'] == 0]
        balanced_results = [r for r in valid_results if 0 < r['linger_ms'] <= 10]
        batch_results = [r for r in valid_results if r['linger_ms'] > 10]
        
        if realtime_results:
            avg_realtime_throughput = sum(r['throughput_records_per_sec'] for r in realtime_results) / len(realtime_results)
            avg_realtime_latency = sum(r['avg_latency_ms'] for r in realtime_results) / len(realtime_results)
            print(f"Real-time (0ms): Середній throughput {avg_realtime_throughput:.1f} rec/sec, latency {avg_realtime_latency:.1f} ms")
        
        if balanced_results:
            avg_balanced_throughput = sum(r['throughput_records_per_sec'] for r in balanced_results) / len(balanced_results)
            avg_balanced_latency = sum(r['avg_latency_ms'] for r in balanced_results) / len(balanced_results)
            print(f"Balanced (10ms): Середній throughput {avg_balanced_throughput:.1f} rec/sec, latency {avg_balanced_latency:.1f} ms")
        
        if batch_results:
            avg_batch_throughput = sum(r['throughput_records_per_sec'] for r in batch_results) / len(batch_results)
            avg_batch_latency = sum(r['avg_latency_ms'] for r in batch_results) / len(batch_results)
            print(f"Batch (50ms): Середній throughput {avg_batch_throughput:.1f} rec/sec, latency {avg_batch_latency:.1f} ms")
        
        # Рекомендації для DER системи
        print(f"\n💡 РЕКОМЕНДАЦІЇ ДЛЯ DER СИСТЕМИ:")
        print(f"Для Virtual Power Plant aggregation з 1000 пристроїв:")
        
        if max_throughput['throughput_records_per_sec'] >= 1000:
            print(f"✅ Досягнуто цільовий throughput 1000+ rec/sec")
        else:
            print(f"⚠️ Цільовий throughput 1000+ rec/sec не досягнуто")
        
        if min_latency['avg_latency_ms'] <= 100:
            print(f"✅ Досягнуто цільову latency ≤100ms")
        else:
            print(f"⚠️ Цільова latency ≤100ms не досягнута")
        
        print(f"\n🎯 ОПТИМАЛЬНІ НАЛАШТУВАННЯ:")
        print(f"Для максимального throughput: batch_size={max_throughput['batch_size']}, linger_ms={max_throughput['linger_ms']}")
        print(f"Для мінімальної latency: batch_size={min_latency['batch_size']}, linger_ms={min_latency['linger_ms']}")
        
        # Зберігаємо звіт у файл
        save_report_to_file(valid_results)

def save_report_to_file(results):
    """Зберігає звіт у файл"""
    try:
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_results': results,
            'summary': {
                'max_throughput': max(results, key=lambda x: x['throughput_records_per_sec']),
                'min_latency': min(results, key=lambda x: x['avg_latency_ms'])
            }
        }
        
        with open("batch_test_final_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Звіт збережено у файл: batch_test_final_report.json")
    except Exception as e:
        print(f"❌ Помилка збереження звіту: {e}")

def main():
    """Основна функція автоматизованого тестування"""
    print("="*80)
    print("🚀 АВТОМАТИЗОВАНЕ ТЕСТУВАННЯ BATCH.SIZE ТА LINGER.MS")
    print("="*80)
    print("Цей скрипт виконає повне тестування різних комбінацій параметрів")
    print("для оптимізації DER системи з 1000 пристроїв")
    print()
    
    start_time = time.time()
    
    try:
        # Крок 1: Запуск Metrics Consumer в окремому потоці
        print("📊 Крок 1: Запуск Metrics Consumer...")
        consumer_thread = threading.Thread(target=run_metrics_consumer)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Чекаємо трохи, щоб consumer підключився
        time.sleep(5)
        
        # Крок 2: Запуск Producer тестів
        print("🚀 Крок 2: Запуск Producer тестів...")
        producer_success = run_producer_tests()
        
        # Чекаємо завершення consumer
        print("⏳ Очікування завершення Metrics Consumer...")
        consumer_thread.join(timeout=300)  # 5 хвилин timeout
        
        # Крок 3: Аналіз результатів
        print("📈 Крок 3: Аналіз результатів...")
        analyze_results()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"\n✅ ТЕСТУВАННЯ ЗАВЕРШЕНО!")
        print(f"Загальна тривалість: {total_duration:.1f} секунд ({total_duration/60:.1f} хвилин)")
        print(f"Результати збережено у файлах:")
        print(f"  - batch_test_results.json (результати producer)")
        print(f"  - consumer_metrics.json (метрики consumer)")
        print(f"  - batch_test_final_report.json (фінальний звіт)")
        
    except KeyboardInterrupt:
        print("\n🛑 Тестування перервано користувачем")
    except Exception as e:
        print(f"\n❌ Помилка під час тестування: {e}")
    finally:
        print("\nТестування завершено.")

if __name__ == "__main__":
    main()
