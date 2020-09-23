#!/usr/bin/env bash

# Try to get a db shell
db_init_wait=0
db_init_timeout=60
function connect() {
    connect=$(echo 'exit' | python manage.py dbshell 2>&1 >/dev/null)
    if ${connect} 2>/dev/null
    then
        true
    else
        echo ${connect} | sed -e "s/'--\(user\|password\)=[^']*'/'--\1=******'/g" >/tmp/externallink_db_connect
        false
    fi
}

until connect || [ $db_init_wait -eq $db_init_timeout ]
do
    >&2 echo "Waiting for DB."
    sleep 1
    db_init_wait=$(( $db_init_wait + 1 ))
done

if [ $db_init_wait -lt $db_init_timeout ]
then
    >&2 echo "DB up."
    rm /tmp/externallink_db_connect 2>/dev/null || :
	exec "$@"
else
    cat /tmp/externallink_db_connect
    rm /tmp/externallink_db_connect 2>/dev/null || :
    exit 1
fi
