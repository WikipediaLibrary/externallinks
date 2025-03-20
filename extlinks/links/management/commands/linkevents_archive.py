import gzip, logging, datetime, os

from typing import List, Optional

from django.core import serializers
from django.core.management import BaseCommand, call_command
from django.db import close_old_connections
from django.db.models import Q, Max
from django_cron.models import CronJobLog

from extlinks.links.models import LinkEvent

logger = logging.getLogger("django")

CHUNK_SIZE = 10_000


class Command(BaseCommand):
    help = "dump & delete or load LinkEvents"

    def dump(self, date: Optional[datetime.date] = None, output: Optional[str] = None):
        """
        Export LinkEvents to gzipped JSON files that are grouped by day, and
        then delete them from the database.

        This command only archives LinkEvents that have been aggregated by
        checking the cron job log. Optionally a date (YYYY-MM-DD) can be passed
        as a parameter to override this behavior.
        """

        output_dir = output if output and os.path.isdir(output) else "backup"

        if date is None:
            # We don't want to archive link events that haven't been processed by
            # the aggregate jobs yet. Find the start time for the most recent
            # successful aggregate job run grouped by the job name.
            most_recent_aggregates = list(
                CronJobLog.objects.values("code")
                .annotate(last_run=Max("start_time"))
                .filter(
                    Q(
                        code__in=[
                            "aggregates.link_aggregates_cron",
                            "aggregates.user_aggregates_cron",
                            "aggregates.pageproject_aggregates_cron",
                        ]
                    )
                    & Q(is_success=True)
                )
            )
            if len(most_recent_aggregates) != 3:
                logger.info("All of the aggregate jobs have not been run yet")
                return

            # Find the oldest start time of the 3 jobs start datetimes we have. All
            # link events before this date have already been aggregated and are
            # safe to be archived.
            archive_start_time = min(
                map(
                    lambda result: result["last_run"],
                    most_recent_aggregates,
                )
            ).date()
        else:
            archive_start_time = date + datetime.timedelta(days=1)

        start = archive_start_time - datetime.timedelta(days=1)
        total = 0
        iteration = 0

        # Page through LinkEvents for all days prior to the day that all of the
        # most recent aggregation jobs have started. This should be yesterday's
        # date, but if the jobs haven't all been completed yet then it will
        # probably be the day before yesterday.
        while True:
            limit = (iteration + 1) * CHUNK_SIZE
            offset = iteration * CHUNK_SIZE

            # Paginate through LinkEvents in chunks of 10k at a time. Overfetch
            # an extra record so we can determine if we need to paginate more.
            results = list(
                LinkEvent.objects.filter(
                    timestamp__gte=start,
                    timestamp__lt=start + datetime.timedelta(days=1),
                ).all()[offset : limit + 1]
            )
            if len(results) == 0:
                break

            # Remove the overfetched record before saving the archive.
            linkevents_by_date = results[:CHUNK_SIZE]

            filename = os.path.join(
                output_dir,
                f"links_linkevent_{start.strftime('%Y%m%d')}_{iteration}.json.gz",
            )
            logger.info(
                "Dumping %d LinkEvents into %s", len(linkevents_by_date), filename
            )

            # Serialize the records directly in the writer to conserve memory.
            with gzip.open(filename, "wt", encoding="utf-8") as archive:
                archive.write(serializers.serialize("json", linkevents_by_date))

            if len(results) > CHUNK_SIZE:
                iteration += 1
            else:
                start -= datetime.timedelta(days=1)
                iteration = 0

            total += len(linkevents_by_date)

        logger.info(
            "Deleting %d LinkEvents before %s from the database",
            total,
            archive_start_time.strftime("%Y-%m-%d"),
        )

        # Delete the objects from the database after all passes are complete.
        # Do this in batches of 10k as well as this has the possibility of
        # failing when dealing with a lot of records.
        query_set = LinkEvent.objects.filter(timestamp__lt=archive_start_time)
        while query_set.exists():
            delete_query_set = query_set[:CHUNK_SIZE].values_list("id", flat=True)
            LinkEvent.objects.filter(pk__in=list(delete_query_set)).delete()

    def load(self, filenames: List[str]):
        """
        Import LinkEvents from gzipped JSON files.
        """

        if not filenames:
            logger.info("No link event archives specified")
            return

        for filename in sorted(filenames):
            logger.info("Loading " + filename)
            # loaddata supports gzipped fixtures and handles relationships properly
            call_command("loaddata", filename)

    def add_arguments(self, parser):
        parser.add_argument(
            "action",
            nargs=1,
            type=str,
            choices=["dump", "load"],
            help="dump: Export LinkEvents to gzipped JSON files, then delete them from the database. load: Import LinkEvents from gzipped JSON files.",
        )
        parser.add_argument(
            "filenames",
            nargs="*",
            type=str,
            help="LinkEvent archive filenames to load.",
        )
        parser.add_argument(
            "-d",
            "--date",
            nargs="?",
            type=lambda arg: datetime.datetime.strptime(arg, "%Y-%m-%d").date(),
            help="A maximum date formatted as YYYY-MM-DD to begin archiving from.",
        )
        parser.add_argument(
            "-o",
            "--output",
            nargs="?",
            type=str,
            help="The directory that the archives containing the LinkEvents should be written to.",
        )

    def handle(self, *args, **options):
        action = options["action"][0]
        if action == "dump":
            self.dump(date=options["date"], output=options["output"])
        if action == "load":
            self.load(filenames=options["filenames"])
        close_old_connections()
