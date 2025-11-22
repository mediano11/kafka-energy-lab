"""
Kafka Streams додаток для обробки DER/VPP даних
Реалізує tumbling windows, агрегації та Event Sourcing
"""
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_DATA,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_AGGREGATES,
    KAFKA_TOPIC_ANOMALIES,
    DER_CONFIG,
    KAFKA_CONSUMER_CONFIG,
    KAFKA_PRODUCER_CONFIG
)
from models import (
    DERReading,
    DEREvent,
    PortfolioAggregate,
    Anomaly,
    EventType
)


class DERStreamsApp:
    """Kafka Streams додаток для обробки DER даних"""
    
    def __init__(self):
        # Перевірка підключення до Kafka перед ініціалізацією
        if not self._check_kafka_connection():
            raise ConnectionError(f"Не вдалося підключитися до Kafka на {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Consumer для читання raw data
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'der-streams-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
        })
        
        # Producer для запису результатів
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            'transactional.id': 'der-streams-transactional',
        })
        
        # Ініціалізація транзакційного producer
        try:
            self.producer.init_transactions()
        except Exception as e:
            print(f"❌ Помилка ініціалізації транзакцій: {e}")
            print("   Переконайтеся, що Kafka повністю запущено")
            raise
        
        # State для windowing (5-хвилинні tumbling windows)
        self.window_size = timedelta(minutes=DER_CONFIG['window_size_minutes'])
        self.current_window_start = None
        self.window_data: Dict[str, list] = defaultdict(list)  # asset_id -> readings
        
        # State для Event Sourcing
        self.event_log: list[DEREvent] = []
        
        # State для forecasting accuracy tracking
        self.forecast_accuracy: Dict[str, list] = defaultdict(list)  # asset_id -> (actual, forecast)
        
        # State для portfolio aggregation
        self.portfolio_state: Dict[str, Any] = {}
    
    def _check_kafka_connection(self):
        """Перевірка підключення до Kafka"""
        import socket
        host, port = KAFKA_BOOTSTRAP_SERVERS.split(':')
        port = int(port)
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                print(f"✅ Підключення до Kafka ({KAFKA_BOOTSTRAP_SERVERS}) успішне")
                return True
            else:
                print(f"❌ Не вдалося підключитися до Kafka на {KAFKA_BOOTSTRAP_SERVERS}")
                return False
        except Exception as e:
            print(f"❌ Помилка перевірки підключення: {e}")
            return False
    
    def create_topics(self):
        """Створює необхідні Kafka topics"""
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
        topics = [
            NewTopic(KAFKA_TOPIC_EVENTS, num_partitions=3, replication_factor=1),
            NewTopic(KAFKA_TOPIC_AGGREGATES, num_partitions=3, replication_factor=1),
            NewTopic(KAFKA_TOPIC_ANOMALIES, num_partitions=3, replication_factor=1),
        ]
        
        futures = admin_client.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic {topic} створено успішно")
            except Exception as e:
                print(f"Topic {topic} вже існує або помилка: {e}")
    
    def _get_window_start(self, timestamp: datetime) -> datetime:
        """Визначає початок tumbling window для timestamp"""
        # Округлення до 5 хвилин
        minutes = (timestamp.minute // DER_CONFIG['window_size_minutes']) * DER_CONFIG['window_size_minutes']
        window_start = timestamp.replace(minute=minutes, second=0, microsecond=0)
        return window_start
    
    def _detect_anomalies(self, reading: DERReading) -> list[Anomaly]:
        """Виявлення аномалій в телеметричних даних"""
        anomalies = []
        
        # Перевірка прогнозу vs фактичне значення
        forecast_error = abs(reading.power_output - reading.forecasted_output)
        forecast_error_pct = (forecast_error / max(reading.forecasted_output, 1)) * 100
        
        if forecast_error_pct > 30:  # Помилка прогнозу > 30%
            anomalies.append(Anomaly(
                anomaly_id=str(uuid.uuid4()),
                asset_id=reading.asset_id,
                timestamp=reading.timestamp,
                anomaly_type='forecast_accuracy',
                severity='medium' if forecast_error_pct < 50 else 'high',
                description=f"Велика розбіжність між прогнозом та фактичним значенням: {forecast_error_pct:.1f}%",
                value=reading.power_output,
                expected_range=(reading.forecasted_output * 0.7, reading.forecasted_output * 1.3),
                z_score=None
            ))
        
        # Перевірка доступної потужності
        if reading.available_capacity < 0:
            anomalies.append(Anomaly(
                anomaly_id=str(uuid.uuid4()),
                asset_id=reading.asset_id,
                timestamp=reading.timestamp,
                anomaly_type='negative_capacity',
                severity='critical',
                description="Негативна доступна потужність",
                value=reading.available_capacity,
                expected_range=(0, float('inf')),
                z_score=None
            ))
        
        # Перевірка для батарей
        if reading.asset_type == 'battery' and reading.soc is not None:
            if reading.soc < 5:
                anomalies.append(Anomaly(
                    anomaly_id=str(uuid.uuid4()),
                    asset_id=reading.asset_id,
                    timestamp=reading.timestamp,
                    anomaly_type='low_soc',
                    severity='high',
                    description=f"Критично низький рівень заряду: {reading.soc}%",
                    value=reading.soc,
                    expected_range=(10, 100),
                    z_score=None
                ))
        
        # Перевірка для генераторів
        if reading.asset_type == 'diesel' and reading.fuel_level is not None:
            if reading.fuel_level < 10:
                anomalies.append(Anomaly(
                    anomaly_id=str(uuid.uuid4()),
                    asset_id=reading.asset_id,
                    timestamp=reading.timestamp,
                    anomaly_type='low_fuel',
                    severity='high',
                    description=f"Критично низький рівень палива: {reading.fuel_level}%",
                    value=reading.fuel_level,
                    expected_range=(20, 100),
                    z_score=None
                ))
        
        return anomalies
    
    def _create_event(self, event_type: EventType, asset_id: str, payload: dict) -> DEREvent:
        """Створює подію для Event Sourcing"""
        return DEREvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            asset_id=asset_id,
            timestamp=datetime.now(timezone.utc),
            payload=payload,
            metadata={'source': 'streams_app'}
        )
    
    def _process_reading(self, reading: DERReading):
        """Обробка одного телеметричного читання"""
        # Визначення поточного вікна
        window_start = self._get_window_start(reading.timestamp)
        
        # Перевірка чи потрібно закрити попереднє вікно
        if self.current_window_start is not None and window_start > self.current_window_start:
            self._close_window(self.current_window_start)
        
        # Додавання читання до поточного вікна
        self.current_window_start = window_start
        self.window_data[reading.asset_id].append(reading)
        
        # Оновлення стану прогнозування
        self.forecast_accuracy[reading.asset_id].append((
            reading.power_output,
            reading.forecasted_output
        ))
        # Зберігаємо тільки останні 100 значень
        if len(self.forecast_accuracy[reading.asset_id]) > 100:
            self.forecast_accuracy[reading.asset_id].pop(0)
        
        # Виявлення аномалій
        anomalies = self._detect_anomalies(reading)
        for anomaly in anomalies:
            self._send_anomaly(anomaly)
        
        # Створення події CAPACITY_CHANGED якщо є зміни
        if reading.asset_id in self.portfolio_state:
            old_capacity = self.portfolio_state[reading.asset_id].get('available_capacity', 0)
            if old_capacity > 0 and abs(reading.available_capacity - old_capacity) > old_capacity * 0.1:  # Зміна > 10%
                event = self._create_event(
                    'CAPACITY_CHANGED',
                    reading.asset_id,
                    {
                        'old_capacity': old_capacity,
                        'new_capacity': reading.available_capacity,
                        'change_pct': ((reading.available_capacity - old_capacity) / max(old_capacity, 1)) * 100
                    }
                )
                self._send_event(event)
        
        # Симуляція диспетчеризації (якщо dispatch_margin достатній)
        # Це імітує рішення про використання активу
        if reading.asset_id not in self.portfolio_state:
            # Перше читання - можлива диспетчеризація
            if reading.power_output > 0:
                event = self._create_event(
                    'ASSET_DISPATCHED',
                    reading.asset_id,
                    {
                        'dispatched': True,
                        'power_output': reading.power_output,
                        'available_capacity': reading.available_capacity
                    }
                )
                self._send_event(event)
        
        # Оновлення portfolio state
        self.portfolio_state[reading.asset_id] = {
            'asset_type': reading.asset_type,
            'power_output': reading.power_output,
            'available_capacity': reading.available_capacity,
            'timestamp': reading.timestamp
        }
    
    def _close_window(self, window_start: datetime):
        """Закриває вікно та генерує агрегати"""
        window_end = window_start + self.window_size
        
        # Агрегація даних по всіх активах
        total_capacity = 0
        available_capacity = 0
        total_output = 0
        asset_count = len(self.window_data)
        asset_type_breakdown = defaultdict(int)
        
        for asset_id, readings in self.window_data.items():
            if not readings:
                continue
            
            # Останнє читання в вікні
            latest = readings[-1]
            # Total capacity = максимальна можлива потужність
            max_capacity = max(latest.available_capacity, abs(latest.power_output))
            total_capacity += max_capacity
            available_capacity += latest.available_capacity
            total_output += latest.power_output if latest.power_output > 0 else 0
            asset_type_breakdown[latest.asset_type] += 1
        
        # Розрахунок dispatch margin
        dispatch_margin = available_capacity - total_output
        
        # Створення агрегату
        aggregate = PortfolioAggregate(
            window_start=window_start,
            window_end=window_end,
            total_capacity=round(total_capacity, 2),
            available_capacity=round(available_capacity, 2),
            dispatch_margin=round(dispatch_margin, 2),
            total_output=round(total_output, 2),
            asset_count=asset_count,
            asset_type_breakdown=dict(asset_type_breakdown)
        )
        
        # Відправка агрегату
        self._send_aggregate(aggregate)
        
        # Створення події BID_SUBMITTED на основі агрегату
        # Імітує подачу заявки на ринок на основі доступної потужності
        if dispatch_margin > 100:  # Якщо є достатня маржа
            event = self._create_event(
                'BID_SUBMITTED',
                'portfolio',  # Portfolio-level event
                {
                    'bid_amount': round(dispatch_margin * 0.8, 2),  # 80% від маржі
                    'available_capacity': available_capacity,
                    'window_start': window_start.isoformat(),
                    'window_end': window_end.isoformat()
                }
            )
            self._send_event(event)
        
        # Очищення даних вікна
        self.window_data.clear()
    
    def _send_event(self, event: DEREvent):
        """Відправка події в Event Store"""
        message = event.model_dump_json()
        self.producer.produce(
            KAFKA_TOPIC_EVENTS,
            key=event.asset_id.encode('utf-8'),
            value=message.encode('utf-8')
        )
        self.event_log.append(event)
        print(f"Подія {event.event_type} для {event.asset_id}")
    
    def _send_aggregate(self, aggregate: PortfolioAggregate):
        """Відправка агрегату portfolio"""
        message = aggregate.model_dump_json()
        window_key = aggregate.window_start.isoformat()
        self.producer.produce(
            KAFKA_TOPIC_AGGREGATES,
            key=window_key.encode('utf-8'),
            value=message.encode('utf-8')
        )
        print(f"Агрегат portfolio: {aggregate.window_start} - {aggregate.total_capacity} кВт")
    
    def _send_anomaly(self, anomaly: Anomaly):
        """Відправка виявленої аномалії"""
        message = anomaly.model_dump_json()
        self.producer.produce(
            KAFKA_TOPIC_ANOMALIES,
            key=anomaly.asset_id.encode('utf-8'),
            value=message.encode('utf-8')
        )
        print(f"Аномалія виявлена: {anomaly.anomaly_type} для {anomaly.asset_id}")
    
    def _commit_transaction(self):
        """Коміт транзакції для exactly-once семантики"""
        try:
            # Commit транзакції (всі produce виклики вже виконані)
            self.producer.commit_transaction()
            # Commit consumer offset
            self.consumer.commit()
        except Exception as e:
            try:
                self.producer.abort_transaction()
            except:
                pass
            print(f"Помилка транзакції: {e}")
            raise
    
    def run(self):
        """Запуск потокової обробки"""
        print("Запуск Kafka Streams додатку...")
        self.create_topics()
        
        self.consumer.subscribe([KAFKA_TOPIC_RAW_DATA])
        
        batch_size = 100
        batch = []
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Помилка consumer: {msg.error()}")
                        continue
                
                try:
                    # Десеріалізація повідомлення
                    data = json.loads(msg.value().decode('utf-8'))
                    reading = DERReading(**data)
                    
                    batch.append(reading)
                    
                    # Обробка батчу для транзакційної семантики
                    if len(batch) >= batch_size:
                        try:
                            self.producer.begin_transaction()
                            for r in batch:
                                self._process_reading(r)
                            self._commit_transaction()
                        except Exception as e:
                            print(f"Помилка обробки батчу: {e}")
                            try:
                                self.producer.abort_transaction()
                            except:
                                pass
                        finally:
                            batch.clear()
                
                except Exception as e:
                    print(f"Помилка обробки повідомлення: {e}")
                    continue
        
        except KeyboardInterrupt:
            print("\nЗупинка streams додатку...")
            # Обробка залишкового батчу
            if batch:
                self.producer.begin_transaction()
                for r in batch:
                    self._process_reading(r)
                self._commit_transaction()
            # Закриття останнього вікна
            if self.current_window_start:
                self._close_window(self.current_window_start)
        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == '__main__':
    app = DERStreamsApp()
    app.run()

