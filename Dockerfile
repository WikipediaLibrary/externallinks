# Dockerfile
FROM quay.io/wikipedialibrary/python:3.11-bullseye-updated as eventstream

WORKDIR /app
ARG REQUIREMENTS_FILE
ENV REQUIREMENTS_FILE=${REQUIREMENTS_FILE:-django.txt}
COPY requirements/* /app/requirements/
RUN echo "Installing $REQUIREMENTS_FILE" && pip install -r /app/requirements/$REQUIREMENTS_FILE
RUN apt update && apt install -y default-mysql-client && rm -rf /var/lib/apt/lists/* && rm -f /var/log/apt/*
# This file only exists once the code directory is mounted by docker-compose.
ENTRYPOINT ["/app/bin/django_wait_for_db.sh"]

FROM eventstream as externallinks
RUN pip install gunicorn

from eventstream as cron
RUN apt update && apt install -y cron && rm -rf /var/lib/apt/lists/* && rm -f /var/log/apt/*
