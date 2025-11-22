"""
Dashboard для візуалізації результатів обробки DER/VPP даних
Використовує Dash для інтерактивної візуалізації
"""
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta
import requests
from config import API_HOST, API_PORT, DASHBOARD_PORT

# URL API
API_BASE_URL = f"http://{API_HOST}:{API_PORT}"

# Ініціалізація Dash додатку
app = dash.Dash(__name__)
app.title = "DER/VPP Dashboard"

# Стилі
app.layout = html.Div([
    html.H1("DER/VPP Portfolio Dashboard", style={'textAlign': 'center', 'color': '#2c3e50'}),
    
    # Статус системи
    html.Div(id='system-status', style={'margin': '20px'}),
    
    # Останній стан portfolio
    html.Div([
        html.H2("Portfolio State", style={'color': '#34495e'}),
        html.Div(id='portfolio-metrics'),
        dcc.Graph(id='portfolio-chart')
    ], style={'margin': '20px', 'padding': '20px', 'backgroundColor': '#ecf0f1', 'borderRadius': '10px'}),
    
    # Аномалії
    html.Div([
        html.H2("Anomalies", style={'color': '#34495e'}),
        html.Div(id='anomalies-summary'),
        dcc.Graph(id='anomalies-chart')
    ], style={'margin': '20px', 'padding': '20px', 'backgroundColor': '#ecf0f1', 'borderRadius': '10px'}),
    
    # Події
    html.Div([
        html.H2("Recent Events", style={'color': '#34495e'}),
        html.Div(id='events-list')
    ], style={'margin': '20px', 'padding': '20px', 'backgroundColor': '#ecf0f1', 'borderRadius': '10px'}),
    
    # Оновлення даних
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Оновлення кожні 5 секунд
        n_intervals=0
    )
])


@app.callback(
    [Output('system-status', 'children'),
     Output('portfolio-metrics', 'children'),
     Output('portfolio-chart', 'figure'),
     Output('anomalies-summary', 'children'),
     Output('anomalies-chart', 'figure'),
     Output('events-list', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    """Оновлення dashboard даних"""
    
    # Статус системи
    try:
        health_response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        status_color = '#27ae60' if health_response.status_code == 200 else '#e74c3c'
        status_text = "Online" if health_response.status_code == 200 else "Offline"
    except:
        status_color = '#e74c3c'
        status_text = "Offline"
    
    system_status = html.Div([
        html.Span("System Status: ", style={'fontWeight': 'bold'}),
        html.Span(status_text, style={'color': status_color, 'fontWeight': 'bold'})
    ])
    
    # Portfolio metrics
    portfolio_metrics = html.Div()
    portfolio_chart = go.Figure()
    
    try:
        portfolio_response = requests.get(f"{API_BASE_URL}/api/portfolio/latest", timeout=2)
        if portfolio_response.status_code == 200:
            data = portfolio_response.json().get('data', {})
            
            if data:
                portfolio_metrics = html.Div([
                    html.Div([
                        html.H3(f"{data.get('total_capacity', 0):,.0f} кВт", style={'color': '#3498db'}),
                        html.P("Total Capacity")
                    ], style={'display': 'inline-block', 'margin': '20px', 'textAlign': 'center'}),
                    html.Div([
                        html.H3(f"{data.get('available_capacity', 0):,.0f} кВт", style={'color': '#2ecc71'}),
                        html.P("Available Capacity")
                    ], style={'display': 'inline-block', 'margin': '20px', 'textAlign': 'center'}),
                    html.Div([
                        html.H3(f"{data.get('dispatch_margin', 0):,.0f} кВт", style={'color': '#f39c12'}),
                        html.P("Dispatch Margin")
                    ], style={'display': 'inline-block', 'margin': '20px', 'textAlign': 'center'}),
                    html.Div([
                        html.H3(f"{data.get('asset_count', 0)}", style={'color': '#9b59b6'}),
                        html.P("Assets")
                    ], style={'display': 'inline-block', 'margin': '20px', 'textAlign': 'center'})
                ])
                
                # Графік breakdown по типах активів
                if 'asset_type_breakdown' in data:
                    breakdown = data['asset_type_breakdown']
                    portfolio_chart = go.Figure(data=[
                        go.Bar(
                            x=list(breakdown.keys()),
                            y=list(breakdown.values()),
                            marker_color=['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
                        )
                    ])
                    portfolio_chart.update_layout(
                        title="Asset Type Breakdown",
                        xaxis_title="Asset Type",
                        yaxis_title="Count",
                        template="plotly_white"
                    )
    except Exception as e:
        portfolio_metrics = html.P(f"Помилка завантаження portfolio: {str(e)}")
    
    # Аномалії
    anomalies_summary = html.Div()
    anomalies_chart = go.Figure()
    
    try:
        # Отримуємо аномалії для прикладу (перший актив)
        anomalies_response = requests.get(f"{API_BASE_URL}/api/anomalies/asset/solar_0000?limit=50", timeout=2)
        if anomalies_response.status_code == 200:
            anomalies_data = anomalies_response.json().get('data', [])
            
            if anomalies_data:
                # Підрахунок по типах
                anomaly_types = {}
                for anomaly in anomalies_data:
                    anomaly_type = anomaly.get('anomaly_type', 'unknown')
                    anomaly_types[anomaly_type] = anomaly_types.get(anomaly_type, 0) + 1
                
                anomalies_summary = html.Div([
                    html.P(f"Total Anomalies: {len(anomalies_data)}"),
                    html.P(f"Types: {', '.join(anomaly_types.keys())}")
                ])
                
                # Графік аномалій по типах
                if anomaly_types:
                    anomalies_chart = go.Figure(data=[
                        go.Pie(
                            labels=list(anomaly_types.keys()),
                            values=list(anomaly_types.values()),
                            hole=0.4
                        )
                    ])
                    anomalies_chart.update_layout(
                        title="Anomalies by Type",
                        template="plotly_white"
                    )
    except Exception as e:
        anomalies_summary = html.P(f"Помилка завантаження аномалій: {str(e)}")
    
    # Події - спробуємо отримати події різних типів
    events_list = html.Div()
    
    try:
        all_events = []
        
        # Отримуємо події різних типів
        event_types = ['CAPACITY_CHANGED', 'ASSET_DISPATCHED', 'BID_SUBMITTED']
        for event_type in event_types:
            try:
                events_response = requests.get(f"{API_BASE_URL}/api/events/type/{event_type}?limit=5", timeout=2)
                if events_response.status_code == 200:
                    events_data = events_response.json().get('data', [])
                    all_events.extend(events_data)
            except:
                continue
        
        # Сортуємо за timestamp (від нового до старого)
        if all_events:
            all_events.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            events_items = []
            for event in all_events[:10]:  # Останні 10 подій
                event_type = event.get('event_type', 'UNKNOWN')
                asset_id = event.get('asset_id', 'N/A')
                timestamp = event.get('timestamp', 'N/A')
                
                # Форматування timestamp
                if isinstance(timestamp, str):
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                events_items.append(html.Div([
                    html.P([
                        html.Strong(f"{event_type}"),
                        f" - {asset_id} - {timestamp}"
                    ], style={'margin': '5px', 'padding': '5px', 'backgroundColor': '#fff', 'borderRadius': '3px'})
                ]))
            
            events_list = html.Div(events_items) if events_items else html.P("Події не знайдено")
        else:
            events_list = html.P("Події ще не створені. Зачекайте, поки система обробить дані.")
    except Exception as e:
        events_list = html.P(f"Помилка завантаження подій: {str(e)}")
    
    return (
        system_status,
        portfolio_metrics,
        portfolio_chart,
        anomalies_summary,
        anomalies_chart,
        events_list
    )


if __name__ == '__main__':
    print(f"Запуск Dashboard на порту {DASHBOARD_PORT}")
    app.run_server(host='0.0.0.0', port=DASHBOARD_PORT, debug=True)

