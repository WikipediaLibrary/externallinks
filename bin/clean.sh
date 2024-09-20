#!/usr/bin/env bash

set -eo pipefail

PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/app/bin:$PATH

if /app/bin/django_wait_for_db.sh
then
    echo "This will drop all tables in ${MYSQL_DATABASE}. Proceed [y/N]?"
    read -p "This will drop all tables in ${MYSQL_DATABASE}. Proceed [y/N]?" -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        echo "Exiting..."
        exit
    fi
    mysql_cmd="mysql -h db -u root -p${MYSQL_ROOT_PASSWORD} -D ${MYSQL_DATABASE}"
    # Build an SQL statement for dropping every table
    concat_fragment="GROUP_CONCAT('DROP TABLE IF EXISTS ', table_name SEPARATOR ';')"
    get_tables_query="SELECT ${concat_fragment} FROM information_schema.tables WHERE table_schema = '${MYSQL_DATABASE}';"
    drop_query=$(echo ${get_tables_query} | ${mysql_cmd})
    drop_query=${drop_query/$concat_fragment/}
    drop_query=${drop_query//[$'\r\n']}
    if [ "$drop_query" == "NULL" ]
    then
        echo "No tables to drop."
        exit
    fi
    drop_query="SET FOREIGN_KEY_CHECKS = 0;${drop_query};SET FOREIGN_KEY_CHECKS = 1;"
    echo "Dropping tables."
    echo ${drop_query}
    nice -n 5 bash -c "echo \"${drop_query}\" | ${mysql_cmd}"

    echo "Tables dropped."
else
    exit 1
fi

