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
        filename="/app/backup/${date}.sql.gz"
        extra_opts=""
        if [ "${1}" = "missing-only" ]
        then
            extra_opts="--insert-ignore --no-create-info --skip-opt"
            filename="/app/backup/${date}.missing-only.sql.gz"
        fi
        nice -n 5 bash -c "mysqldump ${extra_opts} --skip-comments -h db -u root -p${MYSQL_ROOT_PASSWORD} ${MYSQL_DATABASE} | gzip > ${filename}"

        ## `root:wikidev` only; using IDs instead of names to avoid problems in localdev
        chown 0:500 ${filename}
        chmod 0640 ${filename}

        echo "Finished backup."

        # Retain backups for 14 days.
        find /app/backup -name "*.sql.gz" -mtime +14 -delete || :

        echo "Removed backups created 14 days ago or more."
    else
        exit 1
    fi
} {lockfile}>&-
