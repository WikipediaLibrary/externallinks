# Dockerfile
FROM python:3.8-buster as eventstream

WORKDIR /app
COPY requirements/django.txt /app/
RUN pip install -r django.txt
RUN apt update && apt install -y default-mysql-client && rm -rf /var/lib/apt/lists/*
# This file only exists once the code directory is mounted by docker-compose.
ENTRYPOINT ["/app/bin/django_wait_for_db.sh"]

FROM eventstream as externallinks
RUN pip install gunicorn
