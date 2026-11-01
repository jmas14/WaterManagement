FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Carpeta de datos (SQLite de Central). En AWS se monta como volumen.
RUN mkdir -p /data

# 5000: sockets de Central | 8080: panel Central | 8081: panel FO | 8082: panel Engine
EXPOSE 5000 8080 8081 8082

# Por defecto arranca Central. FO, Monitor y Engine sobrescriben el comando.
CMD ["python", "WM_Central.py", "5000", "kafka:29092", "--db", "/data/wm.db"]