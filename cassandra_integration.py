"""
Інтеграція з Apache Cassandra для збереження даних
Таблиці: der_event_log, portfolio_state
"""
from datetime import datetime, timezone
from uuid import UUID
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import DCAwareRoundRobinPolicy
from typing import List, Optional
from config import CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, CASSANDRA_PORT
from models import DEREvent, PortfolioAggregate, Anomaly


class CassandraIntegration:
    """Інтеграція з Cassandra для збереження результатів обробки"""
    
    def __init__(self):
        self.cluster = None
        self.session = None
        self._connect()
        self._create_schema()
    
    def _connect(self):
        """Підключення до Cassandra кластера"""
        try:
            self.cluster = Cluster(
                CASSANDRA_HOSTS,
                port=CASSANDRA_PORT,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
            )
            self.session = self.cluster.connect()
            print(f"Підключено до Cassandra: {CASSANDRA_HOSTS}")
        except Exception as e:
            print(f"Помилка підключення до Cassandra: {e}")
            raise
    
    def _create_schema(self):
        """Створення keyspace та таблиць"""
        # Створення keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        
        self.session.set_keyspace(CASSANDRA_KEYSPACE)
        
        # Таблиця для Event Store
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS der_event_log (
                event_id UUID PRIMARY KEY,
                event_type TEXT,
                asset_id TEXT,
                timestamp TIMESTAMP,
                payload MAP<TEXT, TEXT>,
                metadata MAP<TEXT, TEXT>
            )
        """)
        
        # Індекс для пошуку по asset_id та event_type
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS idx_asset_id ON der_event_log (asset_id)
        """)
        
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_type ON der_event_log (event_type)
        """)
        
        # Таблиця для portfolio state
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_state (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                total_capacity DOUBLE,
                available_capacity DOUBLE,
                dispatch_margin DOUBLE,
                total_output DOUBLE,
                asset_count INT,
                asset_type_breakdown MAP<TEXT, INT>,
                PRIMARY KEY (window_start)
            )
        """)
        
        # Таблиця для аномалій
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS der_anomalies (
                anomaly_id UUID PRIMARY KEY,
                asset_id TEXT,
                timestamp TIMESTAMP,
                anomaly_type TEXT,
                severity TEXT,
                description TEXT,
                value DOUBLE,
                expected_range_min DOUBLE,
                expected_range_max DOUBLE,
                z_score DOUBLE
            )
        """)
        
        # Індекс для пошуку аномалій по asset_id
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS idx_anomaly_asset_id ON der_anomalies (asset_id)
        """)
        
        # Таблиця для forecast accuracy tracking
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS forecast_accuracy (
                asset_id TEXT,
                timestamp TIMESTAMP,
                actual_output DOUBLE,
                forecasted_output DOUBLE,
                error_pct DOUBLE,
                PRIMARY KEY (asset_id, timestamp)
            )
        """)
        
        print("Схема Cassandra створена успішно")
    
    def save_event(self, event: DEREvent):
        """Збереження події в Event Store"""
        try:
            # Конвертація payload та metadata в формат Cassandra MAP
            payload_map = {k: str(v) for k, v in event.payload.items()}
            metadata_map = {k: str(v) for k, v in (event.metadata or {}).items()}
            
            # Конвертація event_id в UUID якщо це рядок
            event_id = UUID(event.event_id) if isinstance(event.event_id, str) else event.event_id
            
            query = SimpleStatement(
                """
                INSERT INTO der_event_log (event_id, event_type, asset_id, timestamp, payload, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            )
            self.session.execute(query, (
                event_id,
                event.event_type,
                event.asset_id,
                event.timestamp,
                payload_map,
                metadata_map
            ))
        except Exception as e:
            print(f"Помилка збереження події: {e}")
            import traceback
            traceback.print_exc()
    
    def save_portfolio_state(self, aggregate: PortfolioAggregate):
        """Збереження стану portfolio"""
        try:
            # Конвертація asset_type_breakdown в формат Cassandra MAP
            breakdown_map = {k: int(v) for k, v in aggregate.asset_type_breakdown.items()}
            
            query = SimpleStatement(
                """
                INSERT INTO portfolio_state (
                    window_start, window_end, total_capacity, available_capacity,
                    dispatch_margin, total_output, asset_count, asset_type_breakdown
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
            )
            self.session.execute(query, (
                aggregate.window_start,
                aggregate.window_end,
                aggregate.total_capacity,
                aggregate.available_capacity,
                aggregate.dispatch_margin,
                aggregate.total_output,
                aggregate.asset_count,
                breakdown_map
            ))
        except Exception as e:
            print(f"Помилка збереження portfolio state: {e}")
            import traceback
            traceback.print_exc()
    
    def save_anomaly(self, anomaly: Anomaly):
        """Збереження виявленої аномалії"""
        try:
            # Конвертація anomaly_id в UUID якщо це рядок
            anomaly_id = UUID(anomaly.anomaly_id) if isinstance(anomaly.anomaly_id, str) else anomaly.anomaly_id
            
            query = SimpleStatement(
                """
                INSERT INTO der_anomalies (
                    anomaly_id, asset_id, timestamp, anomaly_type, severity,
                    description, value, expected_range_min, expected_range_max, z_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            )
            self.session.execute(query, (
                anomaly_id,
                anomaly.asset_id,
                anomaly.timestamp,
                anomaly.anomaly_type,
                anomaly.severity,
                anomaly.description,
                anomaly.value,
                anomaly.expected_range[0],
                anomaly.expected_range[1],
                anomaly.z_score
            ))
        except Exception as e:
            print(f"Помилка збереження аномалії: {e}")
            import traceback
            traceback.print_exc()
    
    def get_events_by_asset(self, asset_id: str, limit: int = 100) -> List[dict]:
        """Отримання подій по asset_id (для replay)"""
        try:
            # Cassandra не підтримує ORDER BY з вторинними індексами
            # Отримуємо всі події та сортуємо в Python
            query = SimpleStatement(
                """
                SELECT * FROM der_event_log
                WHERE asset_id = %s
                """,
                fetch_size=limit * 2  # Отримуємо більше для сортування
            )
            rows = self.session.execute(query, (asset_id,))
            
            events = []
            for row in rows:
                event_dict = {
                    'event_id': str(row.event_id),
                    'event_type': row.event_type,
                    'asset_id': row.asset_id,
                    'timestamp': row.timestamp,
                    'payload': dict(row.payload) if row.payload else {},
                    'metadata': dict(row.metadata) if row.metadata else {}
                }
                events.append(event_dict)
            
            # Сортування в Python за timestamp (від нового до старого)
            events.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return events[:limit]
        except Exception as e:
            print(f"Помилка отримання подій: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_events_by_type(self, event_type: str, limit: int = 100) -> List[dict]:
        """Отримання подій по типу"""
        try:
            # Cassandra не підтримує ORDER BY з вторинними індексами
            # Отримуємо всі події та сортуємо в Python
            query = SimpleStatement(
                """
                SELECT * FROM der_event_log
                WHERE event_type = %s
                """,
                fetch_size=limit * 2  # Отримуємо більше для сортування
            )
            rows = self.session.execute(query, (event_type,))
            
            events = []
            for row in rows:
                event_dict = {
                    'event_id': str(row.event_id),
                    'event_type': row.event_type,
                    'asset_id': row.asset_id,
                    'timestamp': row.timestamp,
                    'payload': dict(row.payload) if row.payload else {},
                    'metadata': dict(row.metadata) if row.metadata else {}
                }
                events.append(event_dict)
            
            # Сортування в Python за timestamp (від нового до старого)
            events.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return events[:limit]
        except Exception as e:
            print(f"Помилка отримання подій: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_portfolio_state(self, window_start: datetime) -> Optional[dict]:
        """Отримання стану portfolio за вікно"""
        try:
            query = SimpleStatement(
                """
                SELECT * FROM portfolio_state
                WHERE window_start = %s
                """
            )
            row = self.session.execute(query, (window_start,)).one()
            
            if row:
                return {
                    'window_start': row.window_start,
                    'window_end': row.window_end,
                    'total_capacity': row.total_capacity,
                    'available_capacity': row.available_capacity,
                    'dispatch_margin': row.dispatch_margin,
                    'total_output': row.total_output,
                    'asset_count': row.asset_count,
                    'asset_type_breakdown': dict(row.asset_type_breakdown) if row.asset_type_breakdown else {}
                }
            return None
        except Exception as e:
            print(f"Помилка отримання portfolio state: {e}")
            return None
    
    def get_latest_portfolio_state(self) -> Optional[dict]:
        """Отримання останнього стану portfolio"""
        try:
            # Отримуємо всі записи та знаходимо останній в Python
            # Оскільки window_start є PRIMARY KEY, можемо використати ORDER BY
            query = SimpleStatement(
                """
                SELECT * FROM portfolio_state
                """
            )
            rows = list(self.session.execute(query))
            
            if not rows:
                return None
            
            # Сортування в Python за window_start (від нового до старого)
            rows.sort(key=lambda x: x.window_start, reverse=True)
            row = rows[0]
            
            return {
                'window_start': row.window_start,
                'window_end': row.window_end,
                'total_capacity': row.total_capacity,
                'available_capacity': row.available_capacity,
                'dispatch_margin': row.dispatch_margin,
                'total_output': row.total_output,
                'asset_count': row.asset_count,
                'asset_type_breakdown': dict(row.asset_type_breakdown) if row.asset_type_breakdown else {}
            }
        except Exception as e:
            print(f"Помилка отримання останнього portfolio state: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_anomalies_by_asset(self, asset_id: str, limit: int = 100) -> List[dict]:
        """Отримання аномалій по asset_id"""
        try:
            # Cassandra не підтримує ORDER BY з вторинними індексами
            # Отримуємо всі аномалії та сортуємо в Python
            query = SimpleStatement(
                """
                SELECT * FROM der_anomalies
                WHERE asset_id = %s
                """,
                fetch_size=limit * 2  # Отримуємо більше для сортування
            )
            rows = self.session.execute(query, (asset_id,))
            
            anomalies = []
            for row in rows:
                anomaly_dict = {
                    'anomaly_id': str(row.anomaly_id),
                    'asset_id': row.asset_id,
                    'timestamp': row.timestamp,
                    'anomaly_type': row.anomaly_type,
                    'severity': row.severity,
                    'description': row.description,
                    'value': row.value,
                    'expected_range': (row.expected_range_min, row.expected_range_max),
                    'z_score': row.z_score
                }
                anomalies.append(anomaly_dict)
            
            # Сортування в Python за timestamp (від нового до старого)
            anomalies.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return anomalies[:limit]
        except Exception as e:
            print(f"Помилка отримання аномалій: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def close(self):
        """Закриття з'єднання"""
        if self.cluster:
            self.cluster.shutdown()


if __name__ == '__main__':
    # Тест підключення
    cassandra = CassandraIntegration()
    print("Cassandra інтеграція готова")
    cassandra.close()

