#!/bin/sh

# Try to get a db shell
db_init_wait=0
db_init_timeout=60
until echo 'exit' | python manage.py dbshell 2>/dev/null || [ $db_init_wait -eq $db_init_timeout ]
do
    >&2 echo "Waiting for DB."
    sleep 1
    db_init_wait=$(( $db_init_wait + 1 ))
done

if [ $db_init_wait -lt $db_init_timeout ]
then
    >&2 echo "DB up."
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
else
    echo "DB timeout."
    exit 1
fi
