#!/usr/bin/env bash
# copy crontab, set permissions, and start cron
set -eo pipefail
PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH
if /app/bin/django_wait_for_db.sh
then
    cp /app/crontab /etc/crontab
    # `root:wikidev` only; using IDs instead of names to avoid problems in localdev
    chown 0:500 /etc/crontab
    chmod 640 /etc/crontab
    echo "Starting cron."
    cron -f -L 8
else
    echo "ERROR: couldn't start cron."
    exit 1
fi
