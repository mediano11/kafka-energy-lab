"""
REST API для market operator queries
Надає доступ до portfolio state, events та аномалій
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timezone
from typing import Optional
from cassandra_integration import CassandraIntegration
from config import API_HOST, API_PORT

app = Flask(__name__)
CORS(app)

# Глобальний об'єкт інтеграції з Cassandra
cassandra = None


def init_cassandra():
    """Ініціалізація підключення до Cassandra"""
    global cassandra
    if cassandra is None:
        cassandra = CassandraIntegration()
    return cassandra


@app.route('/health', methods=['GET'])
def health():
    """Перевірка здоров'я API"""
    return jsonify({'status': 'healthy', 'service': 'DER VPP API'})


@app.route('/api/portfolio/latest', methods=['GET'])
def get_latest_portfolio():
    """Отримання останнього стану portfolio"""
    try:
        db = init_cassandra()
        state = db.get_latest_portfolio_state()
        
        if state:
            # Конвертація datetime в ISO format
            if 'window_start' in state:
                state['window_start'] = state['window_start'].isoformat()
            if 'window_end' in state:
                state['window_end'] = state['window_end'].isoformat()
            
            return jsonify({
                'success': True,
                'data': state
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Portfolio state not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/portfolio/window', methods=['GET'])
def get_portfolio_by_window():
    """Отримання стану portfolio за конкретне вікно"""
    try:
        window_start_str = request.args.get('window_start')
        if not window_start_str:
            return jsonify({
                'success': False,
                'error': 'window_start parameter is required'
            }), 400
        
        window_start = datetime.fromisoformat(window_start_str.replace('Z', '+00:00'))
        
        db = init_cassandra()
        state = db.get_portfolio_state(window_start)
        
        if state:
            if 'window_start' in state:
                state['window_start'] = state['window_start'].isoformat()
            if 'window_end' in state:
                state['window_end'] = state['window_end'].isoformat()
            
            return jsonify({
                'success': True,
                'data': state
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Portfolio state not found for this window'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/events/asset/<asset_id>', methods=['GET'])
def get_events_by_asset(asset_id: str):
    """Отримання подій по asset_id (для replay)"""
    try:
        limit = int(request.args.get('limit', 100))
        
        db = init_cassandra()
        events = db.get_events_by_asset(asset_id, limit)
        
        # Конвертація timestamp в ISO format
        for event in events:
            if 'timestamp' in event:
                event['timestamp'] = event['timestamp'].isoformat()
        
        return jsonify({
            'success': True,
            'data': events,
            'count': len(events)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/events/type/<event_type>', methods=['GET'])
def get_events_by_type(event_type: str):
    """Отримання подій по типу"""
    try:
        limit = int(request.args.get('limit', 100))
        
        db = init_cassandra()
        events = db.get_events_by_type(event_type, limit)
        
        # Конвертація timestamp в ISO format
        for event in events:
            if 'timestamp' in event:
                event['timestamp'] = event['timestamp'].isoformat()
        
        return jsonify({
            'success': True,
            'data': events,
            'count': len(events)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/anomalies/asset/<asset_id>', methods=['GET'])
def get_anomalies_by_asset(asset_id: str):
    """Отримання аномалій по asset_id"""
    try:
        limit = int(request.args.get('limit', 100))
        
        db = init_cassandra()
        anomalies = db.get_anomalies_by_asset(asset_id, limit)
        
        # Конвертація timestamp в ISO format
        for anomaly in anomalies:
            if 'timestamp' in anomaly:
                anomaly['timestamp'] = anomaly['timestamp'].isoformat()
        
        return jsonify({
            'success': True,
            'data': anomalies,
            'count': len(anomalies)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/replay/<asset_id>', methods=['POST'])
def replay_asset_events(asset_id: str):
    """
    Replay подій для аналізу dispatch decisions
    Відтворює послідовність подій для конкретного активу
    """
    try:
        limit = int(request.json.get('limit', 1000) if request.json else 1000)
        
        db = init_cassandra()
        events = db.get_events_by_asset(asset_id, limit)
        
        # Сортування по часу (від старого до нового)
        events.sort(key=lambda x: x['timestamp'])
        
        # Симуляція replay
        replay_result = {
            'asset_id': asset_id,
            'events_count': len(events),
            'timeline': []
        }
        
        current_state = {}
        for event in events:
            event_type = event['event_type']
            payload = event['payload']
            
            # Оновлення стану на основі події
            if event_type == 'CAPACITY_CHANGED':
                current_state['available_capacity'] = payload.get('new_capacity', 0)
            elif event_type == 'ASSET_DISPATCHED':
                current_state['dispatched'] = payload.get('dispatched', False)
            elif event_type == 'BID_SUBMITTED':
                current_state['last_bid'] = payload.get('bid_amount', 0)
            
            replay_result['timeline'].append({
                'timestamp': event['timestamp'].isoformat() if isinstance(event['timestamp'], datetime) else event['timestamp'],
                'event_type': event_type,
                'state_snapshot': current_state.copy()
            })
        
        return jsonify({
            'success': True,
            'data': replay_result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    print(f"Запуск API сервера на {API_HOST}:{API_PORT}")
    app.run(host=API_HOST, port=API_PORT, debug=True)

