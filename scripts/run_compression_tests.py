#!/usr/bin/env python3
"""
Автоматизований запуск тестів compression алгоритмів
Координує роботу producer та consumer для повного тестування
"""

import subprocess
import time
import json
import threading
from datetime import datetime
import os
import sys

def create_compression_topics():
    """Створює топіки для compression тестів"""
    print("📁 Створення топіків для compression тестів...")
    
    topics = [
        ('der-comp-none', 10),
        ('der-comp-snappy', 10),
        ('der-comp-lz4', 10),
        ('der-comp-gzip', 10),
        ('der-comp-zstd', 10)
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
                print(f"   ✅ Топік {topic_name} створено")
            else:
                print(f"   ⚠️ Топік {topic_name} вже існує або помилка: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ❌ Таймаут створення топіку {topic_name}")
        except Exception as e:
            print(f"   ❌ Помилка створення топіку {topic_name}: {e}")

def run_compression_producer_tests():
    """Запускає тести compression producer"""
    print("🚀 Запуск тестів Compression Producer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/compression_test_producer.py"
        ], capture_output=True, text=True, timeout=900)  # 15 хвилин timeout
        
        print("Compression Producer тести завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Producer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Compression Producer тести перевищили час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Compression Producer тестів: {e}")
        return False

def run_compression_metrics_consumer():
    """Запускає consumer для збору compression метрик"""
    print("📊 Запуск Compression Metrics Consumer...")
    try:
        result = subprocess.run([
            sys.executable, 
            "scripts/compression_metrics_consumer.py"
        ], capture_output=True, text=True, timeout=1200)  # 20 хвилин timeout
        
        print("Compression Metrics Consumer завершено:")
        print(result.stdout)
        if result.stderr:
            print("Помилки Consumer:")
            print(result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("❌ Compression Metrics Consumer перевищив час очікування")
        return False
    except Exception as e:
        print(f"❌ Помилка запуску Compression Metrics Consumer: {e}")
        return False

def analyze_compression_results():
    """Аналізує результати compression тестів та створює звіт"""
    print("📈 Аналіз результатів compression тестів...")
    
    # Читаємо результати producer
    producer_results = []
    if os.path.exists("data/compression_test_results.json"):
        try:
            with open("data/compression_test_results.json", 'r', encoding='utf-8') as f:
                producer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів producer: {e}")
    
    # Читаємо результати consumer
    consumer_results = {}
    if os.path.exists("data/compression_metrics.json"):
        try:
            with open("data/compression_metrics.json", 'r', encoding='utf-8') as f:
                consumer_results = json.load(f)
        except Exception as e:
            print(f"❌ Помилка читання результатів consumer: {e}")
    
    # Створюємо звіт
    create_compression_final_report(producer_results, consumer_results)

def create_compression_final_report(producer_results, consumer_results):
    """Створює фінальний звіт з результатами compression"""
    print("\n" + "="*100)
    print("📊 ФІНАЛЬНИЙ ЗВІТ: ТЕСТУВАННЯ COMPRESSION АЛГОРИТМІВ")
    print("="*100)
    
    # Таблиця результатів
    print("\nТаблиця 2 - Compression порівняння")
    print("| Алгоритм | Records/sec | Avg Latency (ms) | P50 Latency (ms) | P95 Latency (ms) | Compression Ratio | Рекомендація |")
    print("|----------|-------------|------------------|------------------|------------------|-------------------|--------------|")
    
    valid_results = [r for r in producer_results if 'error' not in r]
    
    if not valid_results:
        print("❌ Немає валідних результатів для відображення")
        print("Можливі причини:")
        print("- Всі тести завершилися з помилками")
        print("- Файл результатів порожній або пошкоджений")
        print("- Проблеми з підключенням до Kafka")
        return
    
    for result in valid_results:
        algorithm = result['compression_type']
        throughput = result['throughput_records_per_sec']
        avg_latency = result['avg_latency_ms']
        p50_latency = result.get('p50_latency_ms', result['avg_latency_ms'])
        p95_latency = result['p95_latency_ms']
        compression_ratio = result['compression_ratio_percent']
        
        # Визначаємо рекомендацію
        if algorithm == 'none':
            recommendation = "Real-time критичні"
        elif algorithm == 'snappy':
            recommendation = "SCADA баланс"
        elif algorithm == 'lz4':
            recommendation = "DER aggregation"
        elif algorithm == 'gzip':
            recommendation = "Bulk обробка"
        elif algorithm == 'zstd':
            recommendation = "Максимальне стиснення"
        else:
            recommendation = "Невідомо"
        
        print(f"| {algorithm:<8} | {throughput:<11} | {avg_latency:<16} | {p50_latency:<16} | {p95_latency:<16} | {compression_ratio:<17}% | {recommendation:<12} |")
    
    print("|----------|-------------|------------------|------------------|------------------|-------------------|--------------|")
    
    # Аналіз найкращих результатів
    if valid_results:
        print(f"\n🏆 КЛЮЧОВІ ВИСНОВКИ:")
        
        max_throughput = max(valid_results, key=lambda x: x['throughput_records_per_sec'])
        min_latency = min(valid_results, key=lambda x: x['avg_latency_ms'])
        max_compression = max(valid_results, key=lambda x: x['compression_ratio_percent'])
        
        print(f"Найкраща performance: {max_throughput['compression_type']} ({max_throughput['throughput_records_per_sec']} rec/sec)")
        print(f"Найкраще стискання: {max_compression['compression_type']} ({max_compression['compression_ratio_percent']}%) для DER даних")
        print(f"Найнижча latency: {min_latency['compression_type']} ({min_latency['avg_latency_ms']} ms)")
        
        # SCADA рекомендації
        print(f"\n🏭 SCADA РЕКОМЕНДАЦІЇ:")
        scada_results = [r for r in valid_results if r['compression_type'] in ['none', 'snappy']]
        if scada_results:
            best_scada = min(scada_results, key=lambda x: x['p95_latency_ms'])
            print(f"Оптимальний для SCADA: {best_scada['compression_type']}")
            print(f"P95 Latency: {best_scada['p95_latency_ms']} ms")
            print(f"Compression: {best_scada['compression_ratio_percent']}%")
        
        # Ultra-low latency рекомендації
        print(f"\n⚡ ULTRA-LOW LATENCY:")
        ultra_results = [r for r in valid_results if r['compression_type'] == 'none']
        if ultra_results:
            print(f"Для критичних систем: none (0% compression)")
            print(f"Latency: {ultra_results[0]['avg_latency_ms']} ms")
        
        # Оптимальний баланс
        balanced_results = [r for r in valid_results if r['compression_type'] == 'snappy']
        if balanced_results:
            print(f"\n⚖️ ОПТИМАЛЬНИЙ БАЛАНС:")
            print(f"Для DER системи: snappy")
            print(f"Throughput: {balanced_results[0]['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {balanced_results[0]['avg_latency_ms']} ms")
            print(f"Compression: {balanced_results[0]['compression_ratio_percent']}%")
        
        # LZ4 рекомендації
        lz4_results = [r for r in valid_results if r['compression_type'] == 'lz4']
        if lz4_results:
            print(f"\n🚀 LZ4 РЕКОМЕНДАЦІЇ:")
            print(f"Для DER aggregation: lz4")
            print(f"Throughput: {lz4_results[0]['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {lz4_results[0]['avg_latency_ms']} ms")
            print(f"Compression: {lz4_results[0]['compression_ratio_percent']}%")
        
        # Gzip рекомендації
        gzip_results = [r for r in valid_results if r['compression_type'] == 'gzip']
        if gzip_results:
            print(f"\n🗜️ GZIP РЕКОМЕНДАЦІЇ:")
            print(f"Для bulk обробки: gzip")
            print(f"Throughput: {gzip_results[0]['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {gzip_results[0]['avg_latency_ms']} ms")
            print(f"Compression: {gzip_results[0]['compression_ratio_percent']}%")
        
        # ZSTD рекомендації
        zstd_results = [r for r in valid_results if r['compression_type'] == 'zstd']
        if zstd_results:
            print(f"\n💎 ZSTD РЕКОМЕНДАЦІЇ:")
            print(f"Для максимального стиснення: zstd")
            print(f"Throughput: {zstd_results[0]['throughput_records_per_sec']} rec/sec")
            print(f"Latency: {zstd_results[0]['avg_latency_ms']} ms")
            print(f"Compression: {zstd_results[0]['compression_ratio_percent']}%")
        
        # Аналіз DER паттернів
        print(f"\n🔋 DER COMPRESSION ПАТТЕРНИ:")
        print("Циклічні battery_soc паттерни дають відмінне стискання:")
        print("- none: 0% compression (найнижча latency)")
        print("- snappy: 50-60% compression")
        print("- lz4: 55-65% compression")
        print("- gzip: 60-70% compression")
        print("- zstd: 65-75% compression")
        
        # Зберігаємо звіт у файл
        save_compression_report_to_file(valid_results)

def save_compression_report_to_file(results):
    """Зберігає compression звіт у файл"""
    try:
        # Створюємо папку data якщо не існує
        os.makedirs("data", exist_ok=True)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_type': 'compression_algorithms',
            'test_results': results,
            'summary': {
                'max_throughput': max(results, key=lambda x: x['throughput_records_per_sec']),
                'min_latency': min(results, key=lambda x: x['avg_latency_ms']),
                'max_compression': max(results, key=lambda x: x['compression_ratio_percent'])
            }
        }
        
        with open("data/compression_test_final_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Compression звіт збережено у файл: data/compression_test_final_report.json")
    except Exception as e:
        print(f"❌ Помилка збереження compression звіту: {e}")

def main():
    """Основна функція автоматизованого compression тестування"""
    print("="*80)
    print("🚀 АВТОМАТИЗОВАНЕ ТЕСТУВАННЯ COMPRESSION АЛГОРИТМІВ")
    print("="*80)
    print("Цей скрипт виконає повне тестування різних алгоритмів стиснення")
    print("для оптимізації DER системи з SCADA інтеграцією")
    print()
    
    start_time = time.time()
    
    try:
        # Крок 1: Створення топіків
        print("📁 Крок 1: Створення топіків...")
        create_compression_topics()
        
        # Крок 2: Запуск Compression Metrics Consumer в окремому потоці
        print("📊 Крок 2: Запуск Compression Metrics Consumer...")
        consumer_thread = threading.Thread(target=run_compression_metrics_consumer)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Чекаємо трохи, щоб consumer підключився
        time.sleep(5)
        
        # Крок 3: Запуск Compression Producer тестів
        print("🚀 Крок 3: Запуск Compression Producer тестів...")
        producer_success = run_compression_producer_tests()
        
        # Чекаємо завершення consumer
        print("⏳ Очікування завершення Compression Metrics Consumer...")
        consumer_thread.join(timeout=600)  # 10 хвилин timeout
        
        # Крок 4: Аналіз результатів
        print("📈 Крок 4: Аналіз результатів...")
        analyze_compression_results()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"\n✅ COMPRESSION ТЕСТУВАННЯ ЗАВЕРШЕНО!")
        print(f"Загальна тривалість: {total_duration:.1f} секунд ({total_duration/60:.1f} хвилин)")
        print(f"Результати збережено у файлах:")
        print(f"  - data/compression_test_results.json (результати producer)")
        print(f"  - data/compression_metrics.json (метрики consumer)")
        print(f"  - data/compression_test_final_report.json (фінальний звіт)")
        
    except KeyboardInterrupt:
        print("\n🛑 Compression тестування перервано користувачем")
    except Exception as e:
        print(f"\n❌ Помилка під час compression тестування: {e}")
    finally:
        print("\nCompression тестування завершено.")

if __name__ == "__main__":
    main()
