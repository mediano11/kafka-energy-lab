#!/usr/bin/env python3
"""
Аналіз результатів тестування DER системи та генерація рекомендацій
Аналізує результати batch/linger, compression та partitioning тестів
"""

import json
import statistics
from datetime import datetime
from typing import Dict, List, Any, Tuple

class DERSystemAnalyzer:
    def __init__(self):
        """Ініціалізація аналізатора DER системи"""
        self.batch_results = []
        self.compression_results = []
        self.partitioning_results = []
        
        # Критерії успіху для DER системи
        self.success_criteria = {
            'target_throughput': 2000,  # rec/sec для 1000 DER пристроїв
            'max_latency': 10,  # ms для real-time
            'min_compression': 65,  # % для battery_soc даних
            'min_scaling': 1.5  # x для партиціонування
        }
    
    def load_batch_results(self, results: List[Dict[str, Any]]):
        """Завантажує результати batch/linger тестів"""
        self.batch_results = results
        print(f"✅ Завантажено {len(results)} результатів batch/linger тестів")
    
    def load_compression_results(self, results: List[Dict[str, Any]]):
        """Завантажує результати compression тестів"""
        self.compression_results = results
        print(f"✅ Завантажено {len(results)} результатів compression тестів")
    
    def load_partitioning_results(self, results: List[Dict[str, Any]]):
        """Завантажує результати partitioning тестів"""
        self.partitioning_results = results
        print(f"✅ Завантажено {len(results)} результатів partitioning тестів")
    
    def analyze_batch_performance(self) -> Dict[str, Any]:
        """Аналізує результати batch/linger тестів"""
        if not self.batch_results:
            return {}
        
        # Знаходимо найкращі результати по категоріях
        ultra_low_latency = [r for r in self.batch_results if r.get('Використання') == 'Ultra-low']
        scada_ready = [r for r in self.batch_results if r.get('Використання') == 'SCADA']
        real_time = [r for r in self.batch_results if r.get('Використання') == 'Real-time']
        balanced = [r for r in self.batch_results if r.get('Використання') == 'Balanced']
        batch_processing = [r for r in self.batch_results if r.get('Використання') == 'Batch']
        
        analysis = {
            'ultra_low_latency': {
                'best_throughput': max(ultra_low_latency, key=lambda x: x['Records/sec']) if ultra_low_latency else None,
                'best_latency': min(ultra_low_latency, key=lambda x: x['Avg Latency (ms)']) if ultra_low_latency else None
            },
            'scada_ready': {
                'best_throughput': max(scada_ready, key=lambda x: x['Records/sec']) if scada_ready else None,
                'best_latency': min(scada_ready, key=lambda x: x['P95 Latency (ms)']) if scada_ready else None
            },
            'real_time': {
                'best_throughput': max(real_time, key=lambda x: x['Records/sec']) if real_time else None,
                'best_latency': min(real_time, key=lambda x: x['Avg Latency (ms)']) if real_time else None
            },
            'balanced': {
                'best_throughput': max(balanced, key=lambda x: x['Records/sec']) if balanced else None
            },
            'batch_processing': {
                'best_throughput': max(batch_processing, key=lambda x: x['Records/sec']) if batch_processing else None
            }
        }
        
        return analysis
    
    def analyze_compression_performance(self) -> Dict[str, Any]:
        """Аналізує результати compression тестів"""
        if not self.compression_results:
            return {}
        
        # Знаходимо найкращі результати по категоріях
        analysis = {
            'best_throughput': max(self.compression_results, key=lambda x: x['Records/sec']),
            'best_latency': min(self.compression_results, key=lambda x: x['Avg Latency (ms)']),
            'best_compression': max(self.compression_results, key=lambda x: x['Compression Ratio']),
            'scada_optimal': None,
            'battery_soc_optimal': None
        }
        
        # SCADA оптимальний (баланс latency та compression)
        scada_candidates = [r for r in self.compression_results if r['P95 Latency (ms)'] <= 10]
        if scada_candidates:
            analysis['scada_optimal'] = min(scada_candidates, key=lambda x: x['P95 Latency (ms)'])
        
        # Battery SOC оптимальний (високе стиснення для циклічних даних)
        battery_candidates = [r for r in self.compression_results if r['Compression Ratio'] >= 65]
        if battery_candidates:
            analysis['battery_soc_optimal'] = max(battery_candidates, key=lambda x: x['Compression Ratio'])
        
        return analysis
    
    def analyze_partitioning_performance(self) -> Dict[str, Any]:
        """Аналізує результати partitioning тестів"""
        if not self.partitioning_results:
            return {}
        
        # Групуємо по кількості партицій
        by_partitions = {}
        for result in self.partitioning_results:
            num_partitions = result['num_partitions']
            if num_partitions not in by_partitions:
                by_partitions[num_partitions] = []
            by_partitions[num_partitions].append(result)
        
        # Знаходимо baseline (10 партицій)
        baseline_throughput = 0
        if 10 in by_partitions:
            baseline_results = by_partitions[10]
            baseline_throughput = sum(r['throughput_records_per_sec'] for r in baseline_results) / len(baseline_results)
        
        analysis = {
            'baseline_throughput': baseline_throughput,
            'scaling_analysis': {},
            'best_strategies': {},
            'optimal_configuration': None
        }
        
        # Аналіз масштабування
        for num_partitions in sorted(by_partitions.keys()):
            partition_results = by_partitions[num_partitions]
            avg_throughput = sum(r['throughput_records_per_sec'] for r in partition_results) / len(partition_results)
            
            if baseline_throughput > 0:
                scaling_factor = avg_throughput / baseline_throughput
            else:
                scaling_factor = 0
            
            analysis['scaling_analysis'][num_partitions] = {
                'avg_throughput': avg_throughput,
                'scaling_factor': scaling_factor,
                'results': partition_results
            }
        
        # Найкращі стратегії
        for strategy in ['unit_type', 'geographic', 'round_robin']:
            strategy_results = [r for r in self.partitioning_results if r['strategy'] == strategy]
            if strategy_results:
                best_strategy = max(strategy_results, key=lambda x: x['throughput_records_per_sec'])
                analysis['best_strategies'][strategy] = best_strategy
        
        # Оптимальна конфігурація (найкращий баланс throughput/latency)
        balanced_results = []
        for result in self.partitioning_results:
            if result['avg_latency_ms'] > 0:
                balance_score = result['throughput_records_per_sec'] / result['avg_latency_ms']
                balanced_results.append((result, balance_score))
        
        if balanced_results:
            analysis['optimal_configuration'] = max(balanced_results, key=lambda x: x[1])[0]
        
        return analysis
    
    def generate_vpp_recommendations(self) -> Dict[str, Any]:
        """Генерує рекомендації для VPP aggregation"""
        batch_analysis = self.analyze_batch_performance()
        compression_analysis = self.analyze_compression_performance()
        partitioning_analysis = self.analyze_partitioning_performance()
        
        # VPP aggregation: високий throughput, ефективне стиснення
        vpp_config = {
            'use_case': 'VPP aggregation',
            'priority': 'throughput',
            'target_throughput': 2000,  # rec/sec для 1000 DER пристроїв
            'batch_config': None,
            'compression_config': None,
            'partitioning_config': None,
            'expected_performance': {}
        }
        
        # Batch конфігурація для VPP
        if batch_analysis.get('batch_processing', {}).get('best_throughput'):
            vpp_config['batch_config'] = batch_analysis['batch_processing']['best_throughput']
        elif batch_analysis.get('real_time', {}).get('best_throughput'):
            vpp_config['batch_config'] = batch_analysis['real_time']['best_throughput']
        
        # Compression для VPP (zstd для максимального стиснення)
        if compression_analysis.get('battery_soc_optimal'):
            vpp_config['compression_config'] = compression_analysis['battery_soc_optimal']
        elif compression_analysis.get('best_compression'):
            vpp_config['compression_config'] = compression_analysis['best_compression']
        
        # Partitioning для VPP (unit_type для aggregation)
        if partitioning_analysis.get('best_strategies', {}).get('unit_type'):
            vpp_config['partitioning_config'] = partitioning_analysis['best_strategies']['unit_type']
        
        return vpp_config
    
    def generate_p2p_recommendations(self) -> Dict[str, Any]:
        """Генерує рекомендації для P2P trading"""
        batch_analysis = self.analyze_batch_performance()
        compression_analysis = self.analyze_compression_performance()
        partitioning_analysis = self.analyze_partitioning_performance()
        
        # P2P trading: баланс latency та throughput
        p2p_config = {
            'use_case': 'P2P trading',
            'priority': 'balanced',
            'target_latency': 5,  # ms
            'batch_config': None,
            'compression_config': None,
            'partitioning_config': None,
            'expected_performance': {}
        }
        
        # Batch конфігурація для P2P
        if batch_analysis.get('balanced', {}).get('best_throughput'):
            p2p_config['batch_config'] = batch_analysis['balanced']['best_throughput']
        elif batch_analysis.get('real_time', {}).get('best_latency'):
            p2p_config['batch_config'] = batch_analysis['real_time']['best_latency']
        
        # Compression для P2P (lz4 для балансу)
        lz4_results = [r for r in self.compression_results if r.get('Алгоритм') == 'lz4']
        if lz4_results:
            p2p_config['compression_config'] = lz4_results[0]
        elif compression_analysis.get('scada_optimal'):
            p2p_config['compression_config'] = compression_analysis['scada_optimal']
        
        # Partitioning для P2P (geographic для локальних торгів)
        if partitioning_analysis.get('best_strategies', {}).get('geographic'):
            p2p_config['partitioning_config'] = partitioning_analysis['best_strategies']['geographic']
        
        return p2p_config
    
    def generate_grid_support_recommendations(self) -> Dict[str, Any]:
        """Генерує рекомендації для Grid support"""
        batch_analysis = self.analyze_batch_performance()
        compression_analysis = self.analyze_compression_performance()
        partitioning_analysis = self.analyze_partitioning_performance()
        
        # Grid support: SCADA сумісність, низька latency
        grid_config = {
            'use_case': 'Grid support',
            'priority': 'scada_compatibility',
            'target_p95_latency': 10,  # ms
            'batch_config': None,
            'compression_config': None,
            'partitioning_config': None,
            'expected_performance': {}
        }
        
        # Batch конфігурація для Grid support
        if batch_analysis.get('scada_ready', {}).get('best_latency'):
            grid_config['batch_config'] = batch_analysis['scada_ready']['best_latency']
        elif batch_analysis.get('ultra_low_latency', {}).get('best_latency'):
            grid_config['batch_config'] = batch_analysis['ultra_low_latency']['best_latency']
        
        # Compression для Grid support (snappy для SCADA)
        snappy_results = [r for r in self.compression_results if r.get('Алгоритм') == 'snappy']
        if snappy_results:
            grid_config['compression_config'] = snappy_results[0]
        elif compression_analysis.get('scada_optimal'):
            grid_config['compression_config'] = compression_analysis['scada_optimal']
        
        # Partitioning для Grid support (round_robin для балансу)
        if partitioning_analysis.get('best_strategies', {}).get('round_robin'):
            grid_config['partitioning_config'] = partitioning_analysis['best_strategies']['round_robin']
        
        return grid_config
    
    def analyze_trade_offs(self) -> Dict[str, Any]:
        """Аналізує trade-offs між різними параметрами"""
        trade_offs = {
            'latency_vs_throughput': {},
            'compression_vs_performance': {},
            'partitions_vs_complexity': {},
            'main_trade_off': '',
            'critical_parameter': '',
            'recommended_strategy': ''
        }
        
        # Latency vs Throughput
        if self.batch_results:
            high_throughput = max(self.batch_results, key=lambda x: x['Records/sec'])
            low_latency = min(self.batch_results, key=lambda x: x['Avg Latency (ms)'])
            
            trade_offs['latency_vs_throughput'] = {
                'high_throughput': high_throughput,
                'low_latency': low_latency,
                'throughput_difference': high_throughput['Records/sec'] - low_latency['Records/sec'],
                'latency_difference': high_throughput['Avg Latency (ms)'] - low_latency['Avg Latency (ms)']
            }
        
        # Compression vs Performance
        if self.compression_results:
            no_compression = [r for r in self.compression_results if r.get('Алгоритм') == 'none']
            max_compression = max(self.compression_results, key=lambda x: x['Compression Ratio'])
            
            if no_compression:
                trade_offs['compression_vs_performance'] = {
                    'no_compression': no_compression[0],
                    'max_compression': max_compression,
                    'throughput_impact': no_compression[0]['Records/sec'] - max_compression['Records/sec'],
                    'latency_impact': max_compression['Avg Latency (ms)'] - no_compression[0]['Avg Latency (ms)']
                }
        
        # Partitions vs Complexity
        if self.partitioning_results:
            min_partitions = min(self.partitioning_results, key=lambda x: x['num_partitions'])
            max_partitions = max(self.partitioning_results, key=lambda x: x['num_partitions'])
            
            trade_offs['partitions_vs_complexity'] = {
                'min_partitions': min_partitions,
                'max_partitions': max_partitions,
                'throughput_gain': max_partitions['throughput_records_per_sec'] - min_partitions['throughput_records_per_sec'],
                'complexity_increase': max_partitions['num_partitions'] - min_partitions['num_partitions']
            }
        
        # Головний trade-off для DER системи
        trade_offs['main_trade_off'] = "Latency vs Throughput - критичний для real-time DER моніторингу"
        trade_offs['critical_parameter'] = "P95 Latency потребує пріоритету для SCADA інтеграції"
        trade_offs['recommended_strategy'] = "Гібридний підхід: різні конфігурації для різних use cases"
        
        return trade_offs
    
    def generate_final_report(self) -> Dict[str, Any]:
        """Генерує фінальний звіт з рекомендаціями"""
        vpp_config = self.generate_vpp_recommendations()
        p2p_config = self.generate_p2p_recommendations()
        grid_config = self.generate_grid_support_recommendations()
        trade_offs = self.analyze_trade_offs()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'system_type': 'DER Energy Monitoring System',
            'target_devices': 1000,
            'success_criteria': self.success_criteria,
            'recommendations': {
                'vpp_aggregation': vpp_config,
                'p2p_trading': p2p_config,
                'grid_support': grid_config
            },
            'trade_offs': trade_offs,
            'summary': {
                'optimal_batch_config': '64KB_0ms для максимального throughput',
                'optimal_compression': 'zstd для battery_soc циклічних даних',
                'optimal_partitioning': '15 партицій, round_robin для балансу',
                'meets_criteria': self.check_success_criteria()
            }
        }
        
        return report
    
    def check_success_criteria(self) -> Dict[str, bool]:
        """Перевіряє чи виконуються критерії успіху"""
        criteria_check = {}
        
        # Throughput > 2000 rec/sec
        max_throughput = 0
        if self.batch_results:
            max_throughput = max(r['Records/sec'] for r in self.batch_results)
        criteria_check['throughput_target'] = max_throughput >= self.success_criteria['target_throughput']
        
        # Latency < 10ms
        min_latency = float('inf')
        if self.batch_results:
            min_latency = min(r['Avg Latency (ms)'] for r in self.batch_results)
        criteria_check['latency_target'] = min_latency <= self.success_criteria['max_latency']
        
        # Compression > 65%
        max_compression = 0
        if self.compression_results:
            max_compression = max(r['Compression Ratio'] for r in self.compression_results)
        criteria_check['compression_target'] = max_compression >= self.success_criteria['min_compression']
        
        # Scaling > 1.5x
        max_scaling = 1.0
        if self.partitioning_results:
            # Розраховуємо scaling
            by_partitions = {}
            for result in self.partitioning_results:
                num_partitions = result['num_partitions']
                if num_partitions not in by_partitions:
                    by_partitions[num_partitions] = []
                by_partitions[num_partitions].append(result)
            
            if 10 in by_partitions and 20 in by_partitions:
                baseline_throughput = sum(r['throughput_records_per_sec'] for r in by_partitions[10]) / len(by_partitions[10])
                max_throughput_20 = sum(r['throughput_records_per_sec'] for r in by_partitions[20]) / len(by_partitions[20])
                max_scaling = max_throughput_20 / baseline_throughput if baseline_throughput > 0 else 1.0
        
        criteria_check['scaling_target'] = max_scaling >= self.success_criteria['min_scaling']
        
        return criteria_check

def main():
    """Основна функція для аналізу DER системи"""
    print("=== АНАЛІЗ DER СИСТЕМИ ТА ГЕНЕРАЦІЯ РЕКОМЕНДАЦІЙ ===")
    print()
    
    analyzer = DERSystemAnalyzer()
    
    # Завантажуємо результати тестів (приклад даних)
    batch_results = [
        {'Конфігурація': '4KB_0ms', 'Records/sec': 421.72, 'Avg Latency (ms)': 2.35, 'P50 Latency (ms)': 2.13, 'P95 Latency (ms)': 3.96, 'Success Rate (%)': 100.0, 'Використання': 'Ultra-low'},
        {'Конфігурація': '64KB_0ms', 'Records/sec': 502.85, 'Avg Latency (ms)': 1.97, 'P50 Latency (ms)': 1.86, 'P95 Latency (ms)': 2.95, 'Success Rate (%)': 100.0, 'Використання': 'Real-time'},
        {'Конфігурація': '256KB_50ms', 'Records/sec': 18.65, 'Avg Latency (ms)': 53.6, 'P50 Latency (ms)': 53.31, 'P95 Latency (ms)': 55.58, 'Success Rate (%)': 100.0, 'Використання': 'Batch'}
    ]
    
    compression_results = [
        {'Алгоритм': 'none', 'Records/sec': 277.99, 'Avg Latency (ms)': 3.58, 'P50 Latency (ms)': 3.28, 'P95 Latency (ms)': 5.17, 'Compression Ratio': 0.0, 'Рекомендація': 'Real-time критичні'},
        {'Алгоритм': 'snappy', 'Records/sec': 272.05, 'Avg Latency (ms)': 3.66, 'P50 Latency (ms)': 3.48, 'P95 Latency (ms)': 4.91, 'Compression Ratio': 55.0, 'Рекомендація': 'SCADA баланс'},
        {'Алгоритм': 'lz4', 'Records/sec': 283.59, 'Avg Latency (ms)': 3.51, 'P50 Latency (ms)': 3.35, 'P95 Latency (ms)': 4.24, 'Compression Ratio': 60.0, 'Рекомендація': 'DER aggregation'},
        {'Алгоритм': 'gzip', 'Records/sec': 350.16, 'Avg Latency (ms)': 2.84, 'P50 Latency (ms)': 3.51, 'P95 Latency (ms)': 4.32, 'Compression Ratio': 65.0, 'Рекомендація': 'Bulk обробка'},
        {'Алгоритм': 'zstd', 'Records/sec': 277.52, 'Avg Latency (ms)': 3.59, 'P50 Latency (ms)': 3.48, 'P95 Latency (ms)': 4.29, 'Compression Ratio': 70.0, 'Рекомендація': 'Максимальне стиснення'}
    ]
    
    partitioning_results = [
        {'num_partitions': 10, 'strategy': 'unit_type', 'throughput_records_per_sec': 291.58, 'avg_latency_ms': 3.41, 'partition_balance_score': 1.00},
        {'num_partitions': 15, 'strategy': 'round_robin', 'throughput_records_per_sec': 359.16, 'avg_latency_ms': 2.77, 'partition_balance_score': 0.99},
        {'num_partitions': 20, 'strategy': 'unit_type', 'throughput_records_per_sec': 290.19, 'avg_latency_ms': 3.43, 'partition_balance_score': 0.95}
    ]
    
    analyzer.load_batch_results(batch_results)
    analyzer.load_compression_results(compression_results)
    analyzer.load_partitioning_results(partitioning_results)
    
    # Генеруємо фінальний звіт
    report = analyzer.generate_final_report()
    
    # Виводимо результати
    print_report(report)
    
    # Зберігаємо звіт
    save_report(report)

def print_report(report: Dict[str, Any]):
    """Виводить звіт у консоль"""
    print("\n" + "="*100)
    print("📊 ФІНАЛЬНИЙ ЗВІТ: РЕКОМЕНДАЦІЇ ДЛЯ DER СИСТЕМИ")
    print("="*100)
    
    print(f"\n🎯 КРИТЕРІЇ УСПІХУ:")
    criteria = report['success_criteria']
    print(f"   Target Throughput: {criteria['target_throughput']} rec/sec")
    print(f"   Max Latency: {criteria['max_latency']} ms")
    print(f"   Min Compression: {criteria['min_compression']}%")
    print(f"   Min Scaling: {criteria['min_scaling']}x")
    
    print(f"\n✅ ВИКОНАННЯ КРИТЕРІЇВ:")
    criteria_check = report['summary']['meets_criteria']
    for criterion, met in criteria_check.items():
        status = "✅" if met else "❌"
        print(f"   {status} {criterion}: {'Виконано' if met else 'Не виконано'}")
    
    print(f"\n🏭 СПЕЦИФІЧНІ КОНФІГУРАЦІЇ:")
    
    # VPP Aggregation
    vpp = report['recommendations']['vpp_aggregation']
    print(f"\n📈 VPP Aggregation:")
    if vpp['batch_config']:
        print(f"   Batch: {vpp['batch_config']['Конфігурація']} → {vpp['batch_config']['Records/sec']} rec/sec")
    if vpp['compression_config']:
        print(f"   Compression: {vpp['compression_config']['Алгоритм']} → {vpp['compression_config']['Compression Ratio']}%")
    if vpp['partitioning_config']:
        print(f"   Partitioning: {vpp['partitioning_config']['num_partitions']} партицій, {vpp['partitioning_config']['strategy']}")
    
    # P2P Trading
    p2p = report['recommendations']['p2p_trading']
    print(f"\n🤝 P2P Trading:")
    if p2p['batch_config']:
        print(f"   Batch: {p2p['batch_config']['Конфігурація']} → {p2p['batch_config']['Records/sec']} rec/sec")
    if p2p['compression_config']:
        print(f"   Compression: {p2p['compression_config']['Алгоритм']} → {p2p['compression_config']['Compression Ratio']}%")
    if p2p['partitioning_config']:
        print(f"   Partitioning: {p2p['partitioning_config']['num_partitions']} партицій, {p2p['partitioning_config']['strategy']}")
    
    # Grid Support
    grid = report['recommendations']['grid_support']
    print(f"\n⚡ Grid Support:")
    if grid['batch_config']:
        print(f"   Batch: {grid['batch_config']['Конфігурація']} → {grid['batch_config']['Records/sec']} rec/sec")
    if grid['compression_config']:
        print(f"   Compression: {grid['compression_config']['Алгоритм']} → {grid['compression_config']['Compression Ratio']}%")
    if grid['partitioning_config']:
        print(f"   Partitioning: {grid['partitioning_config']['num_partitions']} партицій, {grid['partitioning_config']['strategy']}")
    
    print(f"\n⚖️ TRADE-OFF АНАЛІЗ:")
    trade_offs = report['trade_offs']
    print(f"   Головний trade-off: {trade_offs['main_trade_off']}")
    print(f"   Критичний параметр: {trade_offs['critical_parameter']}")
    print(f"   Рекомендована стратегія: {trade_offs['recommended_strategy']}")
    
    print(f"\n🎯 ПІДСУМОК:")
    summary = report['summary']
    print(f"   Оптимальна batch конфігурація: {summary['optimal_batch_config']}")
    print(f"   Оптимальне стиснення: {summary['optimal_compression']}")
    print(f"   Оптимальне партиціонування: {summary['optimal_partitioning']}")

def save_report(report: Dict[str, Any]):
    """Зберігає звіт у файл"""
    try:
        import os
        os.makedirs("data", exist_ok=True)
        
        with open("data/der_system_final_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Звіт збережено у файл: data/der_system_final_report.json")
    except Exception as e:
        print(f"❌ Помилка збереження звіту: {e}")

if __name__ == "__main__":
    main()
