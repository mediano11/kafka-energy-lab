"""
Конфігурація для лабораторної роботи №4
Kafka Streams для обробки DER/VPP даних
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka конфігурація
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC_RAW_DATA = 'der-raw-data'
KAFKA_TOPIC_EVENTS = 'der-events'
KAFKA_TOPIC_AGGREGATES = 'der-aggregates'
KAFKA_TOPIC_ANOMALIES = 'der-anomalies'

# Exactly-once семантика
KAFKA_PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',  # Очікування підтвердження від всіх реплік
    'retries': 3,
    'max.in.flight.requests.per.connection': 1,  # Для exactly-once
    'enable.idempotence': True,  # Ідемпотентність
    'transactional.id': 'der-producer-transactional',  # Транзакційний ID
    'compression.type': 'snappy',
    'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'
}

KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'der-streams-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Для exactly-once
    'isolation.level': 'read_committed',  # Читання тільки закомічених транзакцій
    'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'
}

# DER конфігурація
DER_CONFIG = {
    'total_assets': 1000,
    'update_interval_seconds': 30,
    'window_size_minutes': 1,
    'asset_types': ['solar', 'wind', 'battery', 'diesel'],
    'solar_capacity_range': (10, 500),  # кВт
    'wind_capacity_range': (50, 2000),  # кВт
    'battery_capacity_range': (100, 1000),  # кВт·год
    'diesel_capacity_range': (200, 5000),  # кВт
}

# Cassandra конфігурація
CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', 'localhost').split(',')
CASSANDRA_KEYSPACE = 'der_vpp'
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', 9042))

# API конфігурація
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', 5000))

# Dashboard конфігурація
DASHBOARD_HOST = os.getenv('DASHBOARD_HOST', '0.0.0.0')
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', 8050))

