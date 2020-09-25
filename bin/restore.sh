#!/usr/bin/env bash

set -eo pipefail

PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH

restore_file=${1}

if /app/bin/django_wait_for_db.sh
then

    echo "Dropping existing DB."
    mysql -h db -u root -p"${MYSQL_ROOT_PASSWORD}" -D "${MYSQL_DATABASE}" -e "DROP DATABASE ${MYSQL_DATABASE}; CREATE DATABASE ${MYSQL_DATABASE};" | :

    echo "Importing backup DB."
    gunzip -c "${restore_file}" | mysql -h db -u root -p"${MYSQL_ROOT_PASSWORD}" -D "${MYSQL_DATABASE}"

    ## Run any necessary DB operations.
    python /app/manage.py migrate

    echo "Finished restore."
else
    exit 1
fi

