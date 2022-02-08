#!/bin/sh

python manage.py migrate
python manage.py collectstatic --noinput

exec gunicorn extlinks.wsgi:application \
    --name extlinks_django \
    --bind 0.0.0.0:8000 \
    --workers 3 \
    --timeout 300 \
    --backlog 2048 \
    --log-level=info \
    --reload \
"$@"
