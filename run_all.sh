#!/bin/bash
# Скрипт для запуску всіх компонентів системи

echo "Запуск системи DER/VPP..."

# Перевірка віртуального середовища
if [ ! -d "venv" ]; then
    echo "Створення віртуального середовища..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Запуск компонентів в окремих терміналах
echo "Запуск Producer..."
gnome-terminal -- bash -c "python producer.py; exec bash" 2>/dev/null || \
xterm -e "python producer.py" 2>/dev/null || \
echo "Запустіть producer.py вручну"

sleep 5

echo "Запуск Streams App..."
gnome-terminal -- bash -c "python streams_app.py; exec bash" 2>/dev/null || \
xterm -e "python streams_app.py" 2>/dev/null || \
echo "Запустіть streams_app.py вручну"

sleep 5

echo "Запуск Cassandra Sink..."
gnome-terminal -- bash -c "python cassandra_sink.py; exec bash" 2>/dev/null || \
xterm -e "python cassandra_sink.py" 2>/dev/null || \
echo "Запустіть cassandra_sink.py вручну"

sleep 5

echo "Запуск API..."
gnome-terminal -- bash -c "python api.py; exec bash" 2>/dev/null || \
xterm -e "python api.py" 2>/dev/null || \
echo "Запустіть api.py вручну"

sleep 5

echo "Запуск Dashboard..."
gnome-terminal -- bash -c "python dashboard.py; exec bash" 2>/dev/null || \
xterm -e "python dashboard.py" 2>/dev/null || \
echo "Запустіть dashboard.py вручну"

echo "Всі компоненти запущено!"
echo "API: http://localhost:5000"
echo "Dashboard: http://localhost:8050"

