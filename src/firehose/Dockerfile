FROM python:3.12
WORKDIR /usr/local/app

COPY src/firehose .
COPY src/config.py .
COPY src/config.yml .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "./firehose.py"]
