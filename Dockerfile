FROM python:3.11-slim

# native deps for shapely + pyproj (GEOS/PROJ)
RUN apt-get update && apt-get install -y \
    g++ proj-bin proj-data libproj-dev libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

ENV PORT=10000
EXPOSE 10000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10000"]
