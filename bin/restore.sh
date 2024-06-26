#!/usr/bin/env bash

set -eo pipefail

PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH

restore_file=${1}

if /app/bin/django_wait_for_db.sh
then

    echo "This may drop the DB. Proceed [y/N]?"
    read -p "This may drop the DB. Proceed [y/N]?" -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        echo "Exiting..."
        exit
    fi

    echo "Importing backup DB."
    nice -n 5 bash -c "gunzip -c ${restore_file} | mysql -h db -u root -p${MYSQL_ROOT_PASSWORD} -D ${MYSQL_DATABASE}"

    ## Run any necessary DB operations.
    python /app/manage.py migrate

    echo "Finished restore."
else
    exit 1
fi

