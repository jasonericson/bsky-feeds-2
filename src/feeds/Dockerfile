FROM python:3.12
WORKDIR /usr/local/app

COPY src/feeds .
COPY src/config.py .
COPY src/config.yml .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "-u", "./feeds.py"]
