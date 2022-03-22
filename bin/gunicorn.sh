#!/bin/sh

python manage.py migrate
python manage.py collectstatic --noinput

exec gunicorn extlinks.wsgi:application \
    --name extlinks_django \
    --bind 0.0.0.0:8000 \
    --worker-class gthread \
    --workers 7 \
    --threads 1 \
    --timeout 30 \
    --backlog 2048 \
    --log-level=info \
    --reload \
"$@"
