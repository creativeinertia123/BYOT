FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ libsqlite3-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    QUANTPILOT_HOST=0.0.0.0 \
    QUANTPILOT_PORT=3000 \
    QUANTPILOT_HOME=/tmp/quantpilot
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN g++ -O3 -march=native -std=c++17 -pthread app/rotation/FF_Rotation.cpp -o app/rotation/ff_rotation -lcurl -lsqlite3
COPY assets ./assets
COPY pilot.py ./pilot.py
COPY public ./public
EXPOSE 3000
CMD ["python3", "pilot.py", "--no-browser"]
