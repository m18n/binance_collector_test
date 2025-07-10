FROM python:3.10-slim

# Встановлюємо необхідні системні залежності
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Створюємо некореневого користувача
RUN useradd -m myuser

# Створюємо робочу директорію та встановлюємо права доступу
RUN mkdir -p /app && chown myuser:myuser /app

# Перемикаємося на користувача
USER myuser

# Встановлюємо робочу директорію
WORKDIR /app

# Створюємо віртуальне середовище
RUN python -m venv venv

# Активуємо віртуальне середовище
ENV PATH="/app/venv/bin:$PATH"

# Оновлюємо pip
RUN pip install --upgrade pip

# Встановлюємо необхідні пакети
RUN pip install --no-cache-dir numpy statsmodels


COPY --chown=myuser:myuser ./binance_collector_test /app/binance_collector_test

# Робимо бінарний файл виконуваним
RUN chmod +x /app/binance_collector_test

# Встановлюємо точку входу
CMD ["./binance_collector_test"]
