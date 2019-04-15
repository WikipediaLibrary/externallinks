# Dockerfile
FROM python:3.5.2

ENV DJANGO_SETTINGS_MODULE=extlinks.settings

WORKDIR /app
COPY . extlinks
COPY requirements/django.txt /app/

RUN mkdir logs

RUN pip install -r django.txt
RUN pip install gunicorn

WORKDIR /app
COPY ./gunicorn.sh /

ENTRYPOINT ["/gunicorn.sh"]