# Dockerfile
FROM python:3.5-buster as eventstream

ENV LOG_FILE="eventstream.log"

WORKDIR /app
COPY . extlinks
COPY requirements/django.txt /app/

RUN mkdir logs

RUN pip install -r django.txt

RUN apt update && apt install -y default-mysql-client && rm -rf /var/lib/apt/lists/*
COPY ./manage.py .
COPY ./django_wait_for_migrations.py .
COPY ./django_wait_for_db.sh /

ENTRYPOINT ["/django_wait_for_db.sh"]

CMD ["python", "django_wait_for_migrations.py", "linkevents_collect", "--historical"]

FROM eventstream as externallinks
ENV LOG_FILE="extlinks.log"
RUN pip install gunicorn
COPY ./gunicorn.sh /

CMD ["/gunicorn.sh"]
