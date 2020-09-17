#!/usr/bin/env bash

set -eo pipefail

PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH

restore_file=${1}

if /app/bin/django_wait_for_db.sh
then

    ## Extract tarball
    tar -xvzf  "${restore_file}" -C "/app" --no-overwrite-dir

    ## Import DB
    python /app/manage.py loaddata /app/db.json

    ## Don't leave an extra DB dump laying out.
    rm -f /app/db.json

    ## Run any necessary DB operations.
    python /app/manage.py migrate

    echo "Finished restore."
else
    exit 1
fi

