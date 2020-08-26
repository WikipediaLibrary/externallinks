#!/bin/sh

# Try to get a db shell
db_init_wait=0
db_init_timeout=60
until echo 'exit' | python manage.py dbshell 2>/dev/null || [ $db_init_wait -eq $db_init_timeout ]
do
    >&2 echo "Waiting for DB."
    sleep 1
    db_init_wait=$(( ++db_init_wait ))
done

if [ $db_init_wait -lt $db_init_timeout ]
then
    >&2 echo "DB up."
	python manage.py migrate
	python manage.py collectstatic --noinput
else
    echo "DB timeout."
    exit 1
fi
