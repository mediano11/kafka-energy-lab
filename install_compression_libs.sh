#!/bin/bash

echo "🔧 Встановлення бібліотек для compression алгоритмів"
echo "=================================================="

# Перевіряємо чи встановлений pip
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 не знайдено. Встановлюємо..."
    sudo apt update
    sudo apt install -y python3-pip
fi

# Встановлюємо системні бібліотеки
echo "📦 Встановлення системних бібліотек..."

# LZ4
echo "Встановлення lz4..."
sudo apt update
sudo apt install -y liblz4-dev

# ZSTD
echo "Встановлення zstd..."
sudo apt install -y libzstd-dev

# Snappy (якщо не встановлено)
echo "Встановлення snappy..."
sudo apt install -y libsnappy-dev

echo ""
echo "🐍 Встановлення Python бібліотек..."

# Встановлюємо Python пакети
pip3 install --upgrade pip

# LZ4 для Python
echo "Встановлення lz4 для Python..."
pip3 install lz4

# ZSTD для Python
echo "Встановлення zstd для Python..."
pip3 install zstandard

# Snappy для Python (якщо не встановлено)
echo "Встановлення snappy для Python..."
pip3 install python-snappy

# Перевіряємо встановлення
echo ""
echo "✅ Перевірка встановлених бібліотек..."

python3 -c "
try:
    import lz4
    print('✅ lz4: OK')
except ImportError:
    print('❌ lz4: НЕ ВСТАНОВЛЕНО')

try:
    import zstandard
    print('✅ zstd: OK')
except ImportError:
    print('❌ zstd: НЕ ВСТАНОВЛЕНО')

try:
    import snappy
    print('✅ snappy: OK')
except ImportError:
    print('❌ snappy: НЕ ВСТАНОВЛЕНО')
"

echo ""
echo "🎉 Встановлення завершено!"
echo "Тепер можна запускати compression тести з усіма алгоритмами."
