#!/usr/bin/env python
import argparse
from datetime import datetime
from dotenv import load_dotenv
from filelock import FileLock
import subprocess
import os


def backup(args):
    ## Dump and gzip DB
    date = datetime.today().strftime("%Y%m%d")
    print("Backing up database.")
    filename = "/app/backup/{}.sql.gz".format(date)
    extra_opts = ""
    if args.missing_only:
        extra_opts = "--insert-ignore --no-create-info --skip-opt"
        filename = "/app/backup/{}.missing-only.sql.gz".format(date)
    command = 'nice -n 5 bash -c "mysqldump {extra_opts} --skip-comments -h db -u root -p{mysql_root_password} {mysql_database} | gzip > {filename}"'.format(
        extra_opts=extra_opts,
        mysql_root_password=os.environ["MYSQL_ROOT_PASSWORD"],
        mysql_database=os.environ["MYSQL_DATABASE"],
        filename=filename,
    )
    subprocess.run(command, shell=True, check=True)

    ## `root:wikidev` only; using IDs instead of names to avoid problems in localdev
    os.chown(filename, 0, 500)
    os.chmod(filename, 0o640)

    print("Finished backup.")


def clean():
    # Retain backups for 14 days.
    subprocess.run(
        'find /app/backup -name "*.sql.gz" -mtime +14 -delete || :',
        shell=True,
        check=True,
    )
    print("Removed backups created 14 days ago or more.")


def main():
    load_dotenv(".env")
    parser = argparse.ArgumentParser(description="externallinks compressed backup")
    parser.add_argument("--missing_only", action="store_true")
    args = parser.parse_args()

    # Use a lockfile to prevent overruns.
    lockfile = "/tmp/backup.lock"
    lock = FileLock(lockfile)
    lock.acquire()
    try:
        backup(args)
        clean()
    finally:
        lock.release()
        os.remove(lockfile)


if __name__ == "__main__":
    main()
