# Worker image (Python). Built by the app through the Docker API the first time Python workers are used.
# Unit tests have their own file: docker build -f workers/python/Dockerfile.test workers/python
FROM python:3.14-slim-trixie
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.16.1
COPY lib.py cpu.py settings.py reporting.py producer.py consumer.py worker.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "worker.py"]