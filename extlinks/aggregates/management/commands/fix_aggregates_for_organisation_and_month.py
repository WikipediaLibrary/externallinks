import gzip
import json
import os
import logging
from datetime import datetime

from django.core.management import call_command

from extlinks.common.management.commands import BaseCommand
from extlinks.links.models import URLPattern, LinkEvent
from extlinks.organisations.models import Organisation

logger = logging.getLogger("django")
class Command(BaseCommand):
    help = "Loads, parses, and fixes monthly link aggregates for a given organisation."

    def add_arguments(self, parser):
        parser.add_argument(
            "--month",
            help="The date (YYYYMM) of the monthly archive to be fixed.",
            type=str,
        )
        parser.add_argument(
            "--organisation",
            help="The organisation id to fix link aggregates for.",
            type=str,
        )
        parser.add_argument(
            "--dir", help="The directory from which to parse archives.", type=str
        )
        parser.add_argument(
            "--skip-monthly", help="Skip the monthly aggregation and only fix daily", type=bool, default=False
        )


    def _handle(self, *args, **options):
        directory = options["dir"]
        month_to_fix = options["month"]
        organisation = Organisation.objects.filter(id=options["organisation"]).first()
        collections = organisation.collection_set.all()
        skip_monthly = options["skip_monthly"]
        if not month_to_fix or not organisation or not collections or not directory:
            return
        url_pattern_strings = [i.url for i in URLPattern.objects.filter(collection__in=collections)]
        org_only_events = []
        for filename in os.listdir(directory):
            if (
                filename.endswith(".json.gz")
                and filename.startswith("links_linkevent_")
                and month_to_fix in filename
            ):
                file_path = os.path.join(directory, filename)
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
                    for event in data:
                        link = event["fields"]["link"]
                        for url_pattern in url_pattern_strings:
                            if url_pattern in link:
                                org_only_events.append(event)
                filtered_file = f"link_events_filtered_{month_to_fix}_organisation_{organisation.id}.json.gz"
                filtered_file_path = os.path.join(
                    directory,
                    filtered_file,
                )
                if len(org_only_events) > 0:
                    try:
                        with gzip.open(
                            filtered_file_path, "wt", encoding="utf-8"
                        ) as new_archive:
                            json.dump(org_only_events, new_archive)
                        logger.info(f"Aggregating {len(org_only_events)} filtered events for {filtered_file}")
                        # load filtered records into the database
                        call_command("archive_link_aggregates", "load", filtered_file_path)
                        # run aggregate command
                        call_command(
                            "fill_link_aggregates",
                            collections=[i.id for i in collections.all()],
                        )
                        # run monthly aggregate command if we're not skipping it
                        if not skip_monthly:
                            call_command(
                                "fill_monthly_link_aggregates",
                                collections=collections,
                                year_month=datetime.strptime(month_to_fix, "%Y%m").strftime("%Y-%m"),
                            )
                        # delete the records from the database, as we do not need to re-archive or re-upload them
                        LinkEvent.objects.filter(pk__in=[i['pk'] for i in org_only_events]).delete()
                    except Exception as e:
                        logger.error(e)
