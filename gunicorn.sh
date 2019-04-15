#!/bin/sh

python manage.py migrate
python manage.py collectstatic --noinput

# Prepare log files and start outputting logs to stdout
touch ./gunicorn.log
touch ./gunicorn-access.log
tail -n 0 -f ./gunicorn*.log &

exec gunicorn extlinks.wsgi:application \
    --name extlinks_django \
    --bind 0.0.0.0:8000 \
    --workers 5 \
    --log-level=info \
    --log-file=./gunicorn.log \
    --access-logfile=./gunicorn-access.log \
    --reload \
"$@"