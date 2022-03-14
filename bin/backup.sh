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
        mysqldump -h db -u root -p"${MYSQL_ROOT_PASSWORD}" "${MYSQL_DATABASE}" > /app/backup/${date}.sql
        gzip /app/backup/${date}.sql

        ## Root only
        chmod 0600 "/app/backup/${date}.sql.gz"

        echo "Finished backup."

        # Retain backups for 30 days.
        find /app/backup -name "*.sql.gz" -mtime +30 -delete || :

        echo "Removed backups created 30 days ago or more."
    else
        exit 1
    fi
} {lockfile}>&-
