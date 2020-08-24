# Dockerfile
FROM python:3.5-buster as eventstream

ENV LOG_FILE="eventstream.log"

WORKDIR /app
COPY . extlinks
COPY requirements/django.txt /app/

RUN mkdir logs

RUN pip install -r django.txt

CMD ["python", "manage.py", "linkevents_collect"]

FROM eventstream as externallinks
ENV LOG_FILE="extlinks.log"
WORKDIR /app
RUN pip install gunicorn && apt update && apt install -y default-mysql-client && rm -rf /var/lib/apt/lists/*
COPY ./manage.py .
COPY ./gunicorn.sh /

ENTRYPOINT ["/gunicorn.sh"]
