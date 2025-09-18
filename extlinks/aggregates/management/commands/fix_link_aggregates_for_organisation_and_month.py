import gzip
import json
import os
import logging
from datetime import datetime, timedelta, date

from django.core.management import call_command
from django.db import transaction

from extlinks.aggregates.models import LinkAggregate
from extlinks.common.management.commands import BaseCommand
from extlinks.links.models import URLPattern
from extlinks.organisations.models import Organisation

logger = logging.getLogger("django")


class Command(BaseCommand):
    help = ("Loads, parses, and fixes daily link aggregates for a given month and organisation. "
            "Only run this command if the month's link events have not been already been aggregated.")

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
        url_patterns = URLPattern.objects.filter(collection__in=collections)
        events_split_by_url_pattern = self.load_events_from_archives(
            directory, month_to_fix, [i.url for i in url_patterns]
        )
        first_day_of_month = self.get_first_day_of_month(month_to_fix)
        last_day_of_month = self.get_last_day_of_month(first_day_of_month)
        try:
            for i in range(
                first_day_of_month.day, last_day_of_month.day+1
            ):
                for collection in collections:
                    collection_url_pattern_strings = [
                        i.url for i in url_patterns.filter(collection=collection)
                    ]
                    for collection_url_string in collection_url_pattern_strings:
                        for link_event in self.get_link_events_for_day(
                            collection_url_string, events_split_by_url_pattern, i
                        ):
                            self.fill_aggregate_from_archived_link_event(collection, link_event)
            # run monthly aggregate command if we're not skipping it
            if not skip_monthly:
                # fill monthly aggregate for the subset of link events we just aggregated
                call_command(
                    "fill_monthly_link_aggregates",
                    collections=collections,
                    year_month=datetime.strptime(month_to_fix, "%Y%m").strftime("%Y-%m"),
                )
        except Exception as e:
            logger.info(f"Unexpected exception occurred: {e}")

    def fill_aggregate_from_archived_link_event(self, collection, link_event):
        change_number = link_event["fields"]["change"]
        existing_link_aggregate = (
            LinkAggregate.objects.filter(
                organisation=collection.organisation.id,
                collection=collection.id,
                full_date=datetime.fromisoformat(link_event["fields"]["timestamp"]),
                on_user_list=link_event["fields"]["on_user_list"],
            )
            .exclude(day=0)
            .first()
        )
        if existing_link_aggregate is not None:
            if change_number == 0:
                existing_link_aggregate.total_links_removed += 1
            else:
                existing_link_aggregate.total_links_added += 1
            existing_link_aggregate.save()
        else:
            # Create a new link aggregate
            links_added = change_number if change_number > 0 else 0
            links_removed = 1 if change_number == 0 else 0
            try:
                with transaction.atomic():
                    LinkAggregate.objects.create(
                        organisation=collection.organisation,
                        collection=collection,
                        full_date=datetime.fromisoformat(
                            link_event["fields"]["timestamp"]
                        ).date(),
                        total_links_added=links_added,
                        total_links_removed=links_removed,
                        on_user_list=link_event["fields"]["on_user_list"],
                    )
            except Exception as e:
                logger.info(
                    f"Unexpected exception occurred filling aggregate: {e}"
                )

    def get_link_events_for_day(
        self, collection_url: str, events_split_by_url_pattern, i: int
    ):
        link_events_for_day = [
            j
            for j in events_split_by_url_pattern[collection_url]
            if datetime.fromisoformat(j["fields"]["timestamp"]).date().day == i
        ]
        return link_events_for_day

    def get_last_day_of_month(self, first_day_of_month: date) -> date:
        if first_day_of_month.month == 12:
            return first_day_of_month.replace(day=31)
        replace = first_day_of_month.replace(month=first_day_of_month.month + 1)
        return replace - timedelta(days=1)

    def get_first_day_of_month(self, month_to_fix: str) -> date:
        return datetime.strptime(month_to_fix, "%Y%m").date().replace(day=1)

    def load_events_from_archives(self, directory: object, month_to_fix: str, url_pattern_strings) -> object:
        events_split_by_url_pattern = dict.fromkeys(url_pattern_strings)
        # initialize empty array for each url pattern in the org
        for key, value in events_split_by_url_pattern.items():
            events_split_by_url_pattern[key] = []
        for filename in os.listdir(directory):
            if (
                filename.endswith(".json.gz")
                and filename.startswith("links_linkevent_")
                and month_to_fix in filename
            ):
                try:
                    file_path = os.path.join(directory, filename)
                    with gzip.open(file_path, "rt", encoding="utf-8") as f:
                        data = json.load(f)
                        for event in data:
                            link = event["fields"]["link"]
                            for url_pattern in url_pattern_strings:
                                if url_pattern in link:
                                    events_split_by_url_pattern[url_pattern].append(
                                        event
                                    )
                except Exception as e:
                    logger.info(
                        f"Unexpected exception occurred loading events from archive: {e}"
                    )
        return events_split_by_url_pattern
