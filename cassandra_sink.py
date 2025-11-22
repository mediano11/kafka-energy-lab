"""
Kafka Connect Sink для запису даних в Cassandra
Читає дані з Kafka topics та записує в Cassandra
"""
import json
from datetime import datetime
from confluent_kafka import Consumer
from cassandra_integration import CassandraIntegration
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_AGGREGATES,
    KAFKA_TOPIC_ANOMALIES,
    KAFKA_CONSUMER_CONFIG
)
from models import DEREvent, PortfolioAggregate, Anomaly


class CassandraSink:
    """Sink для запису даних з Kafka в Cassandra"""
    
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'cassandra-sink-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        
        self.cassandra = CassandraIntegration()
        
        # Підписка на topics
        self.consumer.subscribe([
            KAFKA_TOPIC_EVENTS,
            KAFKA_TOPIC_AGGREGATES,
            KAFKA_TOPIC_ANOMALIES
        ])
    
    def _process_event(self, message: bytes):
        """Обробка події з topic der-events"""
        try:
            data = json.loads(message.decode('utf-8'))
            event = DEREvent(**data)
            self.cassandra.save_event(event)
            print(f"Збережено подію: {event.event_type} для {event.asset_id}")
        except Exception as e:
            print(f"Помилка обробки події: {e}")
    
    def _process_aggregate(self, message: bytes):
        """Обробка агрегату з topic der-aggregates"""
        try:
            data = json.loads(message.decode('utf-8'))
            # Конвертація timestamp strings в datetime
            data['window_start'] = datetime.fromisoformat(data['window_start'].replace('Z', '+00:00'))
            data['window_end'] = datetime.fromisoformat(data['window_end'].replace('Z', '+00:00'))
            
            aggregate = PortfolioAggregate(**data)
            self.cassandra.save_portfolio_state(aggregate)
            print(f"Збережено portfolio state: {aggregate.window_start}")
        except Exception as e:
            print(f"Помилка обробки агрегату: {e}")
    
    def _process_anomaly(self, message: bytes):
        """Обробка аномалії з topic der-anomalies"""
        try:
            data = json.loads(message.decode('utf-8'))
            # Конвертація timestamp string в datetime
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            # Конвертація expected_range tuple
            if isinstance(data['expected_range'], list):
                data['expected_range'] = tuple(data['expected_range'])
            
            anomaly = Anomaly(**data)
            self.cassandra.save_anomaly(anomaly)
            print(f"Збережено аномалію: {anomaly.anomaly_type} для {anomaly.asset_id}")
        except Exception as e:
            print(f"Помилка обробки аномалії: {e}")
    
    def run(self):
        """Запуск sink"""
        print("Запуск Cassandra Sink...")
        print(f"Підписка на topics: {KAFKA_TOPIC_EVENTS}, {KAFKA_TOPIC_AGGREGATES}, {KAFKA_TOPIC_ANOMALIES}")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"Помилка consumer: {msg.error()}")
                    continue
                
                topic = msg.topic()
                
                if topic == KAFKA_TOPIC_EVENTS:
                    self._process_event(msg.value())
                elif topic == KAFKA_TOPIC_AGGREGATES:
                    self._process_aggregate(msg.value())
                elif topic == KAFKA_TOPIC_ANOMALIES:
                    self._process_anomaly(msg.value())
        
        except KeyboardInterrupt:
            print("\nЗупинка Cassandra Sink...")
        finally:
            self.consumer.close()
            self.cassandra.close()


if __name__ == '__main__':
    sink = CassandraSink()
    sink.run()

