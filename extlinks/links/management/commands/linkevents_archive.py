from datetime import datetime
import glob, gzip, logging, math

from django.core import serializers
from django.core.management import BaseCommand
from django.db import IntegrityError

from extlinks.links.models import LinkEvent

logger = logging.getLogger("django")
chunk = 100000


class Command(BaseCommand):
    help = "dump + delete or load LinkEvents for a given year"

    def dump(self, year):
        if year < datetime.now().year:
            for m in range(1, 13):
                # pad out month for clear filenames
                month = f"{m:02}"
                yyyymm = str(year) + str(month)
                linkevents = LinkEvent.objects.filter(
                    timestamp__year=year, timestamp__month=month
                )
                if linkevents.count() == 0:
                    logger.info("no events found for " + yyyymm)
                    continue
                passes = math.ceil((linkevents.count()) / chunk)
                logger.info("dumping " + yyyymm + " in " + str(passes) + " passes")
                for p in range(passes):
                    offset = chunk * p
                    limit = chunk * (p + 1)
                    logger.info("query offset: " + str(offset))
                    logger.info("query limit: " + str(limit))
                    filename = (
                        "backup/links_linkevent_" + yyyymm + "." + str(p) + ".json.gz"
                    )
                    logger.info("dumping " + filename)
                    linkevents_chunk = linkevents.all()[offset:limit]
                    with gzip.open(filename, "wt", encoding="utf-8") as archive:
                        archive.write(serializers.serialize("json", linkevents_chunk))
                logger.info("deleting " + yyyymm + " events from ORM")
                linkevents.delete()

    def load(self, year):
        pathname = "backup/links_linkevent_" + str(year) + "??.?.json.gz"
        for filename in sorted(glob.glob(pathname)):
            logger.info("loading " + filename)
            with gzip.open(filename, "rt", encoding="utf-8") as archive:
                data = (
                    deserialized.object
                    for deserialized in serializers.deserialize("json", archive)
                )
                LinkEvent.objects.bulk_create(data, chunk)

    def add_arguments(self, parser):
        parser.add_argument("action", nargs=1, type=str)
        parser.add_argument("year", nargs="+", action="extend", type=int)

    def handle(self, *args, **options):
        action = options["action"][0]
        if action:
            for year in options["year"]:
                if action == "dump":
                    self.dump(year)
                if action == "load":
                    self.load(year)
