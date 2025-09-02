import datetime
import gzip
import logging
import os

from abc import ABC, abstractmethod
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from typing import List, Optional, Type, cast

from django.core import serializers
from django.core.management import call_command
from django.core.management.base import CommandError, CommandParser
from django.db import models, close_old_connections

from extlinks.common import swift
from extlinks.common.management.commands import BaseCommand

logger = logging.getLogger("django")

CHUNK_SIZE = 10_000


class AggregateArchiveCommand(ABC, BaseCommand):
    """
    AggregateArchiveCommand is a helper class for creating archival commands
    for all forms of aggregate data in extlinks.

    It can be used through inheritance and implementing the 'get_model' method
    to return the model corresponding to the table that you want to archive.
    This class works with any table that has a column named 'full_date' with
    the DATE type.
    """

    help = "Dump & delete or load data from aggregate tables"
    name = "aggregate"

    def log_msg(self, msg, *args, level="info"):
        """
        Logs and prints messages so they are visible in both Docker
        logs and cron job logs

        Parameters
        ----------
        msg : str
            The message to log

        *args : tuple
            Arguments to be lazily formatted into msg.

        level : str
            The log level ('info' or 'error'), defaults to 'info'

        Returns
        -------
        None
        """
        if level == "error":
            logger.error(msg, *args)
            formatted_msg = msg % args if args else msg
            self.stderr.write(formatted_msg)
            self.stderr.flush()
        else:
            logger.info(msg, *args)
            formatted_msg = msg % args if args else msg
            self.stdout.write(formatted_msg)
            self.stdout.flush()

    def add_arguments(self, parser: CommandParser) -> None:
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        dump_parser = subparsers.add_parser(
            "dump",
            help=f"Dump {self.name} data to gzipped JSON files, then delete them from the database.",
        )
        dump_parser.add_argument(
            "-f",
            "--from",
            nargs="?",
            type=lambda arg: datetime.datetime.strptime(arg, "%Y-%m").date(),
            help="A date formatted as YYYY-MM to begin archiving from.",
            required=False,
        )
        dump_parser.add_argument(
            "-t",
            "--to",
            nargs="?",
            type=lambda arg: datetime.datetime.strptime(arg, "%Y-%m").date(),
            help="An optional date formatted as YYYY-MM to end archiving from.",
            required=False,
        )
        dump_parser.add_argument(
            "-o",
            "--output",
            nargs="?",
            type=str,
            help=f"The directory that the archives containing the {self.name} data should be written to.",
        )
        dump_parser.add_argument(
            "-c",
            "--container",
            nargs="?",
            type=str,
            help=f"The Swift container to upload {self.name} archives to. If left unspecified then nothing will be uploaded.",
        )
        dump_parser.add_argument(
            "--object-storage-only",
            action="store_true",
            help="If enabled, archives will only be stored in Swift and deleted from local storage after upload.",
        )

        load_parser = subparsers.add_parser(
            "load",
            help=f"Import {self.name} data into the database from gzipped JSON files.",
        )
        load_parser.add_argument(
            "filenames",
            nargs="*",
            type=str,
            help=f"{self.name} archive filenames to load.",
        )

        upload_parser = subparsers.add_parser(
            "upload",
            help=f"Upload {self.name} data to Swift object storage from the local filesystem.",
        )
        upload_parser.add_argument(
            "-c",
            "--container",
            nargs="?",
            type=str,
            help=f"The Swift container to upload {self.name} archives to.",
        )
        upload_parser.add_argument(
            "filenames",
            nargs="*",
            type=str,
            help=f"{self.name} archive filenames to upload.",
        )

    def handle(self, *args, **options):
        subcommand = options["subcommand"]

        if subcommand == "dump":
            self.dump(
                start=options["from"],
                end=options["to"],
                output=options["output"],
                container=options["container"],
                object_storage_only=options["object_storage_only"],
            )
        elif subcommand == "load":
            self.load(filenames=options["filenames"])
        elif subcommand == "upload":
            self.upload(container=options["container"], filenames=options["filenames"])

        close_old_connections()

    def dump(
        self,
        start: Optional[datetime.date] = None,
        end: Optional[datetime.date] = None,
        output: Optional[str] = None,
        container: Optional[str] = None,
        object_storage_only=False,
    ):
        """
        Dump aggregate data to gzipped JSON files that are grouped by month,
        and then delete them from the database.

        Parameters
        ----------
        start : datetime.date
            The start date for the dump (inclusive).

        end : datetime.date, optional
            The end date for the dump (inclusive).

        output : str, optional
            The directory to write the archive files to.

        container : str, optional
            The Swift container to upload the archive files to.

        object_storage_only : bool, optional
            If enabled, archives will only be stored in Swift and deleted from
            local storage after upload.
        """

        # Pick the earliest possible date if one is not provided on the CLI.
        if not start:
            oldest = self.get_model().objects.order_by("full_date").first()
            if not oldest:
                raise CommandError(
                    f"There are not {self.name} records to archive. Stopping..."
                )

            start = datetime.date(oldest.year, oldest.month, 1)

            # If both start and end were not provided (happens with cron) then
            # only archive aggregates that are older than a year. It's possible
            # that end will be before start for fresh installs, but this is
            # fine as the command will do nothing in that scenario.
            if not end:
                end = (datetime.date.today() - relativedelta(years=1)).replace(day=1)

        if not container:
            container = os.environ.get(
                "SWIFT_CONTAINER_AGGREGATES", "archive-aggregates"
            )

        if end:
            cursor = start

            while cursor <= end:
                archives = self.archive(cursor, output=output)
                cursor += relativedelta(months=1)

                # Upload archives to object storage if a container was specified.
                if container and len(archives) > 0:
                    self.upload(container, archives)

                    if object_storage_only:
                        self._remove_archives(archives)
        else:
            archives = self.archive(start, output=output)

            # Upload archives to object storage if a container was specified.
            if container and len(archives) > 0:
                self.upload(container, archives)

                if object_storage_only:
                    self._remove_archives(archives)

        # Delete only after everything is archived as some archives need to
        # contain duplicate records for use by the frontend's filters and
        # deleting them immediately will result in incomplete archives.
        if end:
            cursor = start

            while cursor <= end:
                self.delete(cursor)
                cursor += relativedelta(months=1)
        else:
            self.delete(start)

    def load(self, filenames: List[str]):
        """
        Import data from gzipped JSON files.

        Parameters
        ----------
        filenames : List[str]
            The list of archive filenames to load into the database.
        """

        if not filenames:
            self.log_msg("No %s archives specified", self.name)
            return

        for filename in sorted(filenames):
            self.log_msg("Loading %s...", filename)

            # loaddata supports gzipped fixtures and handles relationships properly.
            call_command("loaddata", filename)

    def upload(self, container: str, filenames: List[str]):
        """
        Upload the given files to object storage.

        Parameters
        ----------
        container : str
            The name of the Swift container to upload to.

        filenames : List[str]
            The paths of the files to upload.
        """

        if len(filenames) == 0:
            raise CommandError("Filenames must be provided to the upload command")

        self.log_msg("Uploading %d archives to object storage", len(filenames))

        try:
            conn = swift.swift_connection()
        except RuntimeError:
            self.log_msg(
                "Swift credentials not provided. Skipping upload.", level="error"
            )
            return False

        try:
            # Ensure the container exists before uploading.
            try:
                was_created = swift.ensure_container_exists(conn, container)
                if was_created:
                    self.log_msg(f"Created new container: {container}")
            except RuntimeError as e:
                self.log_msg(str(e), level="error")
                return False

            successful, failed = swift.batch_upload_files(conn, container, filenames)

            self.log_msg(
                "Uploaded %d/%d archives to object storage",
                len(successful),
                len(filenames),
            )

            if len(failed) > 0:
                raise CommandError(
                    f"The following {failed} archives failed to upload: {','.join(failed)}"
                )
        except Exception as e:
            self.log_msg(f"Failed to upload to Swift: {e}", level="error")
            return False

    def archive(
        self,
        date: datetime.date,
        output: Optional[str] = None,
    ) -> List[str]:
        """
        Archives a month's worth of data defined by 'date' and returns a list
        of archives that were generated.

        Parameters
        ----------
        date : datetime.date
            The date to archive aggregates for.

        output : str, optional
            The directory to output the archives to. If not provided, the
            archives will be output to $HOST_BACKUP_DIR.
        """

        AggregateModel = self.get_model()

        output_dir = output if output and os.path.isdir(output) else "backup"
        archives: List[str] = []

        results = list(
            AggregateModel.objects.filter(
                full_date__gte=date,
                full_date__lt=date + relativedelta(months=1),
            ).all()
        )
        if len(results) == 0:
            self.log_msg(
                "Unable to find aggregate data for the month of %s",
                date.strftime("%Y-%m"),
            )
            return archives

        # Split by: organisation, collection, full_date, on_user_list (limit only)
        splits = defaultdict(list)

        for record in cast(List, results):
            all_users_key = (
                record.organisation_id,
                record.collection_id,
                record.full_date,
                "0",
            )
            on_user_list_key = (
                record.organisation_id,
                record.collection_id,
                record.full_date,
                "1",
            )

            splits[all_users_key].append(record)

            if record.on_user_list:
                splits[on_user_list_key].append(record)

        for k, v in splits.items():
            params = "_".join(map(lambda x: str(x), k))
            filename = os.path.join(
                output_dir,
                f"aggregates_{self.name.lower()}_{params}.json.gz",
            )
            self.log_msg(
                "Dumping %d %s records into %s",
                len(v),
                self.name,
                filename,
            )
            # Serialize the records directly in the writer to conserve memory.
            with gzip.open(filename, "wt", encoding="utf-8") as archive:
                archive.write(serializers.serialize("json", v))
            archives.append(filename)

        return archives

    def delete(self, date: datetime.date):
        """
        Deletes the given month's aggregates.

        Parameters
        ----------
        date : datetime.date
            The date of the month to delete aggregates for in the database.
        """

        AggregateModel = self.get_model()

        query_set = AggregateModel.objects.filter(
            full_date__gte=date, full_date__lt=date + relativedelta(months=1)
        )
        if query_set.exists():
            self.log_msg(
                "Deleting %s records for the month of %s from the database",
                self.name,
                date.strftime("%Y-%m"),
            )

        while query_set.exists():
            delete_query_set = query_set[:CHUNK_SIZE].values_list("id", flat=True)
            AggregateModel.objects.filter(pk__in=list(delete_query_set)).delete()

    @abstractmethod
    def get_model(self) -> Type[models.Model]:
        """
        Returns the model containing aggregate data.

        Returns
        -------
        Type[models.Model]
            The model representing the aggregate data being archived.
        """

        raise NotImplementedError

    def _remove_archives(self, paths: List[str]):
        """
        Deletes all archives in the given list of paths.

        Parameters
        ----------
        paths : List[str]
            A list of paths to delete.
        """

        for path in paths:
            self.log_msg("Deleting local archive: %s", path)
            os.remove(path)
