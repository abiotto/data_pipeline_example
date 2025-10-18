FROM apache/beam_python3.10_sdk:2.54.0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline.py .
COPY postgresql-42.7.3.jar .

CMD ["python", "pipeline.py"]
