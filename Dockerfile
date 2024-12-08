# Используем официальный Python образ
FROM python:3.9-slim

# Устанавливаем зависимости
WORKDIR /app
COPY requirements.txt .

RUN pip install -r requirements.txt

# Копируем весь проект в контейнер
COPY . .

# Команда для запуска сервера
CMD ["python", "server.py"]
