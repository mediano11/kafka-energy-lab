#!/usr/bin/env python3
"""
Скрипт для перевірки налаштування системи перед запуском
Перевіряє доступність Kafka, Cassandra та Docker
"""
import sys
import socket
import subprocess
from config import KAFKA_BOOTSTRAP_SERVERS, CASSANDRA_HOSTS, CASSANDRA_PORT


def check_port(host, port, service_name):
    """Перевірка доступності порту"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            print(f"✅ {service_name} доступний на {host}:{port}")
            return True
        else:
            print(f"❌ {service_name} недоступний на {host}:{port}")
            return False
    except Exception as e:
        print(f"❌ Помилка перевірки {service_name}: {e}")
        return False


def check_docker():
    """Перевірка доступності Docker"""
    try:
        result = subprocess.run(
            ['docker', 'ps'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print("✅ Docker доступний")
            return True
        else:
            print("❌ Docker недоступний (спробуйте 'sudo docker ps' або додайте користувача до групи docker)")
            print(f"   Помилка: {result.stderr}")
            return False
    except FileNotFoundError:
        print("❌ Docker не встановлено")
        return False
    except subprocess.TimeoutExpired:
        print("❌ Docker не відповідає (перевірте права доступу)")
        return False
    except Exception as e:
        print(f"❌ Помилка перевірки Docker: {e}")
        print("   Спробуйте: sudo usermod -aG docker $USER")
        print("   Після цього потрібно вийти та увійти знову")
        return False


def check_docker_compose():
    """Перевірка чи запущені контейнери"""
    try:
        result = subprocess.run(
            ['docker', 'compose', 'ps'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            output = result.stdout
            if 'kafka' in output.lower() and 'cassandra' in output.lower():
                print("✅ Docker Compose контейнери запущені")
                print("\nСтатус контейнерів:")
                print(output)
                return True
            else:
                print("⚠️  Docker Compose контейнери не запущені")
                print("   Запустіть: docker-compose up -d")
                return False
        else:
            print("⚠️  Не вдалося перевірити статус контейнерів")
            print(f"   Помилка: {result.stderr}")
            return False
    except Exception as e:
        print(f"⚠️  Помилка перевірки Docker Compose: {e}")
        return False


def check_kafka_connection():
    """Перевірка підключення до Kafka"""
    kafka_host = KAFKA_BOOTSTRAP_SERVERS.split(':')[0]
    kafka_port = int(KAFKA_BOOTSTRAP_SERVERS.split(':')[1])
    return check_port(kafka_host, kafka_port, "Kafka")


def check_cassandra_connection():
    """Перевірка підключення до Cassandra"""
    cassandra_host = CASSANDRA_HOSTS[0]
    return check_port(cassandra_host, CASSANDRA_PORT, "Cassandra")


def main():
    """Основна функція перевірки"""
    print("=" * 60)
    print("Перевірка налаштування системи DER/VPP")
    print("=" * 60)
    print()
    
    all_ok = True
    
    # Перевірка Docker
    print("1. Перевірка Docker...")
    docker_ok = check_docker()
    if not docker_ok:
        all_ok = False
        print("\n💡 Рішення:")
        print("   - Додайте користувача до групи docker: sudo usermod -aG docker $USER")
        print("   - Або використовуйте sudo для docker команд")
        print("   - Після додавання до групи - вийдіть та увійдіть знову")
        print()
    
    # Перевірка Docker Compose
    if docker_ok:
        print("2. Перевірка Docker Compose контейнерів...")
        compose_ok = check_docker_compose()
        if not compose_ok:
            all_ok = False
            print("\n💡 Рішення:")
            print("   - Запустіть контейнери: docker-compose up -d")
            print("   - Дочекайтеся 30-60 секунд для повного запуску")
            print()
    
    # Перевірка Kafka
    print("3. Перевірка підключення до Kafka...")
    kafka_ok = check_kafka_connection()
    if not kafka_ok:
        all_ok = False
        print("\n💡 Рішення:")
        print("   - Переконайтеся, що Kafka запущено: docker-compose ps")
        print("   - Запустіть Kafka: docker-compose up -d kafka")
        print("   - Дочекайтеся повного запуску (30-60 секунд)")
        print(f"   - Перевірте налаштування в config.py: {KAFKA_BOOTSTRAP_SERVERS}")
        print()
    
    # Перевірка Cassandra
    print("4. Перевірка підключення до Cassandra...")
    cassandra_ok = check_cassandra_connection()
    if not cassandra_ok:
        all_ok = False
        print("\n💡 Рішення:")
        print("   - Переконайтеся, що Cassandra запущено: docker-compose ps")
        print("   - Запустіть Cassandra: docker-compose up -d cassandra")
        print("   - Дочекайтеся повного запуску (1-2 хвилини)")
        print(f"   - Перевірте налаштування в config.py: {CASSANDRA_HOSTS[0]}:{CASSANDRA_PORT}")
        print()
    
    # Підсумок
    print("=" * 60)
    if all_ok:
        print("✅ Всі перевірки пройдено успішно!")
        print("   Система готова до запуску компонентів")
        return 0
    else:
        print("❌ Виявлено проблеми з налаштуванням")
        print("   Виправте проблеми перед запуском компонентів")
        return 1


if __name__ == '__main__':
    sys.exit(main())

