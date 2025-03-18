import datetime
import gzip
import logging
import os
import swiftclient
import swiftclient.exceptions

from abc import ABC, abstractmethod
from dateutil.relativedelta import relativedelta
from django.core import serializers
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import models
from extlinks.aggregates.management.helpers.swift import (
    swift_connection,
    batch_upload_files,
)
from typing import List, Optional, Type

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

    def add_arguments(self, parser: CommandParser) -> None:
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        dump_parser = subparsers.add_parser(
            "dump",
            help=f"Dump {self.name} data to gzipped JSON files, then delete them from the database.",
        )
        dump_parser.add_argument(
            "-d",
            "--date",
            nargs="?",
            type=lambda arg: datetime.datetime.strptime(arg, "%Y-%m").date(),
            help="A maximum date formatted as YYYY-MM-DD to begin archiving from.",
            required=True,
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
            required=True,
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
                date=options["date"],
                output=options["output"],
                container=options["container"],
            )
        elif subcommand == "load":
            self.load(filenames=options["filenames"])
        elif subcommand == "upload":
            self.upload(container=options["container"], filenames=options["filenames"])

    def dump(
        self,
        date: datetime.date,
        output: Optional[str] = None,
        container: Optional[str] = None,
    ):
        """
        Dump aggregate data to gzipped JSON files that are grouped by month,
        and then delete them from the database.
        """

        AggregateModel = self.get_model()

        output_dir = output if output and os.path.isdir(output) else "backup"

        start = date
        total = 0
        iteration = 0
        archives: List[str] = []

        while True:
            limit = (iteration + 1) * CHUNK_SIZE
            offset = iteration * CHUNK_SIZE

            results = list(
                AggregateModel.objects.filter(
                    full_date__gte=start,
                    full_date__lt=start + relativedelta(months=1),
                ).all()[offset : limit + 1]
            )
            if len(results) == 0:
                break

            # Remove the overfetched record before saving the archive.
            results_by_date = results[:CHUNK_SIZE]

            filename = os.path.join(
                output_dir,
                f"aggregates_{self.name.lower()}_{start.strftime('%Y%m')}_{iteration}.json.gz",
            )
            logger.info(
                "Dumping %d %s records into %s",
                len(results_by_date),
                self.name,
                filename,
            )

            # Serialize the records directly in the writer to conserve memory.
            with gzip.open(filename, "wt", encoding="utf-8") as archive:
                archive.write(serializers.serialize("json", results_by_date))
                archives.append(filename)

            if len(results) > CHUNK_SIZE:
                iteration += 1
            else:
                start -= relativedelta(months=1)
                iteration = 0

            total += len(results_by_date)

        logger.info(
            "Deleting %d %s records prior to %s from the database",
            total,
            self.name,
            date.strftime("%Y-%m"),
        )

        query_set = AggregateModel.objects.filter(
            full_date__lt=date + relativedelta(months=1)
        )
        while query_set.exists():
            delete_query_set = query_set[:CHUNK_SIZE].values_list("id", flat=True)
            AggregateModel.objects.filter(pk__in=list(delete_query_set)).delete()

        # Upload the archives to object storage if a container was specified.
        if container and len(archives) > 0:
            self.upload(container, archives)

    def load(self, filenames: List[str]):
        """
        Import data from gzipped JSON files.
        """

        if not filenames:
            logger.info("No %s archives specified", self.name)
            return

        for filename in sorted(filenames):
            logger.info("Loading %s...", filename)

            # loaddata supports gzipped fixtures and handles relationships properly.
            call_command("loaddata", filename)

    def upload(self, container: str, filenames: List[str]):
        """
        Upload the given files to object storage.
        """

        if len(filenames) == 0:
            raise CommandError("Filenames must be provided to the upload command")

        logger.info("Uploading %d archives to object storage", len(filenames))
        conn = swift_connection()

        try:
            conn.get_container(container)
        except swiftclient.exceptions.ClientException as exc:
            if exc.http_status == 404:
                logger.error(
                    "Cannot upload archives to Swift. The container '%s' "
                    "doesn't exist",
                    container,
                )

            raise

        successful, _ = batch_upload_files(conn, container, filenames)

        logger.info(
            "Successfully uploaded %d/%d filenames to object storage",
            len(successful),
            len(filenames),
        )

    @abstractmethod
    def get_model(self) -> Type[models.Model]:
        """
        Returns the model containing aggregate data.
        """

        raise NotImplementedError
