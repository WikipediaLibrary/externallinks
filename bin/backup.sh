#!/usr/bin/env bash

set -eo pipefail

# Use a lockfile to prevent overruns.
self=$(basename ${0})
exec {lockfile}>/var/lock/${self}
flock -n ${lockfile}
{

    PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH

    date=$(date +'%Y%m%d%H%M')

    ## Dump DB
    if /app/bin/django_wait_for_db.sh
    then
        echo "Backing up database."
        python /app/manage.py dumpdata > /app/db.json

        tar -czf "/app/backup/${date}.tar.gz" -C "/app/backup" "./db.json"

        ## Root only
        chmod 0600 "/app/backup/${date}.tar.gz"

        ## Don't leave an extra DB dump laying out.
        rm -f /app/db.json

        echo "Finished backup."
    else
        exit 1
    fi
} {lockfile}>&-
