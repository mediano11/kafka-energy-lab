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
    if os.path.exists("data/batch_test_results.json"):
        try:
            with open("data/batch_test_results.json", 'r', encoding='utf-8') as f:
                producer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів producer: {e}")
    
    # Читаємо результати consumer
    consumer_results = {}
    if os.path.exists("data/consumer_metrics.json"):
        try:
            with open("data/consumer_metrics.json", 'r', encoding='utf-8') as f:
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
    print("\nТаблиця 1 - Ключові результати (19 тестів)")
    print("| Конфігурація | Records/sec | Avg Latency (ms) | P50 Latency (ms) | P95 Latency (ms) | Success Rate (%) | Використання |")
    print("|--------------|-------------|------------------|------------------|------------------|------------------|--------------|")
    
    valid_results = [r for r in producer_results if 'error' not in r]
    
    if not valid_results:
        print("❌ Немає валідних результатів для відображення")
        print("Можливі причини:")
        print("- Всі тести завершилися з помилками")
        print("- Файл результатів порожній або пошкоджений")
        print("- Проблеми з підключенням до Kafka")
        return
    
    for result in valid_results:
        config = result['config_name']
        throughput = result['throughput_records_per_sec']
        avg_latency = result['avg_latency_ms']
        p50_latency = result.get('p50_latency_ms', result['avg_latency_ms'])
        p95_latency = result['p95_latency_ms']
        success_rate = result['success_rate']
        
        # Визначаємо тип використання
        if result['batch_size'] <= 8192:
            if result['linger_ms'] == 0:
                usage = "Ultra-low"
            elif result['linger_ms'] <= 5:
                usage = "SCADA"
            else:
                usage = "Low-latency"
        elif result['linger_ms'] == 0:
            usage = "Real-time"
        elif result['linger_ms'] <= 10:
            usage = "Balanced"
        else:
            usage = "Batch"
        
        print(f"| {config:<12} | {throughput:<11} | {avg_latency:<16} | {p50_latency:<16} | {p95_latency:<16} | {success_rate:<16.1f} | {usage:<12} |")
    
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
        
        # SCADA аналіз
        scada_results = [r for r in valid_results if r['batch_size'] <= 8192 and r['linger_ms'] <= 5]
        if scada_results:
            best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
            print(f"\n🏭 SCADA ІНТЕГРАЦІЯ:")
            print(f"Оптимальна конфігурація: {best_scada['config_name']}")
            print(f"P95 Latency: {best_scada['p95_latency_ms']} ms")
            print(f"P50 Latency: {best_scada['p50_latency_ms']} ms")
            print(f"Network Jitter: {best_scada.get('latency_std_dev', 0)} ms")
        
        # Ultra-low latency аналіз
        ultra_low_results = [r for r in valid_results if r['batch_size'] <= 8192 and r['linger_ms'] == 0]
        if ultra_low_results:
            best_ultra = min(ultra_low_results, key=lambda x: x['avg_latency_ms'])
            print(f"\n⚡ ULTRA-LOW LATENCY:")
            print(f"Найкраща конфігурація: {best_ultra['config_name']}")
            print(f"Середня latency: {best_ultra['avg_latency_ms']} ms")
            print(f"P95 Latency: {best_ultra['p95_latency_ms']} ms")
        
        # Consumer lag аналіз
        print(f"\n📊 CONSUMER LAG АНАЛІЗ:")
        if consumer_results:
            for test_id, metrics in consumer_results.items():
                if metrics:
                    print(f"Тест {test_id}:")
                    print(f"  Середній Consumer Lag: {metrics.get('avg_consumer_lag_ms', 0)} ms")
                    print(f"  P95 Consumer Lag: {metrics.get('p95_consumer_lag_ms', 0)} ms")
                    print(f"  Network Jitter: {metrics.get('avg_network_jitter_ms', 0)} ms")
                    print(f"  Критичні алерти: {metrics.get('critical_alerts_count', 0)}")
        
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
        
        # SCADA рекомендації
        if scada_results:
            print(f"Для SCADA інтеграції: batch_size={best_scada['batch_size']}, linger_ms={best_scada['linger_ms']}")
        
        # Ultra-low latency рекомендації
        if ultra_low_results:
            print(f"Для ultra-low latency: batch_size={best_ultra['batch_size']}, linger_ms={best_ultra['linger_ms']}")
        
        # Зберігаємо звіт у файл
        save_report_to_file(valid_results)

def save_report_to_file(results):
    """Зберігає звіт у файл"""
    try:
        # Створюємо папку data якщо не існує
        os.makedirs("data", exist_ok=True)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_results': results,
            'summary': {
                'max_throughput': max(results, key=lambda x: x['throughput_records_per_sec']),
                'min_latency': min(results, key=lambda x: x['avg_latency_ms'])
            }
        }
        
        with open("data/batch_test_final_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Звіт збережено у файл: data/batch_test_final_report.json")
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
        print(f"  - data/batch_test_results.json (результати producer)")
        print(f"  - data/consumer_metrics.json (метрики consumer)")
        print(f"  - data/batch_test_final_report.json (фінальний звіт)")
        
    except KeyboardInterrupt:
        print("\n🛑 Тестування перервано користувачем")
    except Exception as e:
        print(f"\n❌ Помилка під час тестування: {e}")
    finally:
        print("\nТестування завершено.")

if __name__ == "__main__":
    main()
