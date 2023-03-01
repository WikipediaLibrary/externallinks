from datetime import datetime
import glob, gzip, logging, math

from django.core import serializers
from django.core.management import call_command, BaseCommand
from django.db import IntegrityError

from extlinks.links.models import LinkEvent, URLPattern

logger = logging.getLogger("django")
chunk = 100000
this_year = datetime.now().year


class Command(BaseCommand):
    help = "dump & delete or load LinkEvents for a given year"

    def dump(self, year):
        """
        Export LinkEvents for `year` to gzipped JSON files, then delete them from the database.
        Breaks the events up into chunks to limit memory usage.
        """
        # Loop through each month
        for m in range(1, 13):
            # pad out month for clear filenames
            month = f"{m:02}"
            yyyymm = str(year) + str(month)
            # queryset for this year and month
            linkevents = LinkEvent.objects.filter(
                timestamp__year=year, timestamp__month=month
            )
            if linkevents.count() == 0:
                logger.info("No link events found for " + yyyymm)
                continue
            # Determine the number of passes to make by dividng linkevent count by chunk size, rounding up.
            passes = math.ceil((linkevents.count()) / chunk)
            logger.info("Dumping " + yyyymm + " in " + str(passes) + " passes")
            for p in range(passes):
                # Slice the queryset as needed for each pass
                offset = chunk * p
                limit = chunk * (p + 1)
                logger.info("query offset: " + str(offset))
                logger.info("query limit: " + str(limit))
                filename = (
                    "backup/links_linkevent_" + yyyymm + "." + str(p) + ".json.gz"
                )
                logger.info("dumping " + filename)
                linkevents_chunk = linkevents.all()[offset:limit]

                # Serialize the records directly in the writer to conserve memory
                with gzip.open(filename, "wt", encoding="utf-8") as archive:
                    archive.write(serializers.serialize("json", linkevents_chunk))
            # Delete the objects from the database after all passes are complete
            logger.info("Deleting " + yyyymm + " events from ORM")
            linkevents.delete()

    def load(self, year):
        """
        Import LinkEvents for `year` from gzipped JSON files.
        Breaks the events up into chunks to limit memory usage.
        """
        # Glob matching the expected file names
        pathname = "backup/links_linkevent_" + str(year) + "??.?.json.gz"
        filenames = sorted(glob.glob(pathname))
        ThroughModel = LinkEvent.url.through
        if not filenames:
            logger.info("No link event archives found for " + str(year))
            return
        for filename in sorted(glob.glob(pathname)):
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
            "year",
            nargs="+",
            action="extend",
            type=int,
            help="Space delimited list of years to act upon; past years only",
        )

    def handle(self, *args, **options):
        action = options["action"][0]
        for year in options["year"]:
            if year >= this_year:
                logger.warning("Skipping events for " + str(this_year))
                continue
            if action == "dump":
                self.dump(year)
            if action == "load":
                self.load(year)
