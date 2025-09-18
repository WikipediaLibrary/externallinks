import gzip
import json
import os
import logging
from datetime import datetime, timedelta, date

from django.db import transaction

from extlinks.aggregates.models import LinkAggregate
from extlinks.common import swift
from extlinks.common.management.commands import BaseCommand
from extlinks.links.models import URLPattern, LinkEvent
from extlinks.organisations.models import Organisation

logger = logging.getLogger("django")


class Command(BaseCommand):
    help = (
        "Loads, parses, and fixes daily or monthly link aggregates for a given organisation. "
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--month",
            help="If provided, will fix a monthly aggregate. The date (YYYYMM) of the monthly archive to be fixed.",
            type=str,
        )
        parser.add_argument(
            "--day",
            help="If provided, will fix a daily aggregate. The date (YYYYMMDD) of the daily archive to be fixed.",
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

    def _handle(self, *args, **options):
        directory = options["dir"]
        month_to_fix = options["month"]
        day_to_fix = options["day"]
        organisation = Organisation.objects.filter(id=options["organisation"]).first()
        collections = organisation.collection_set.all()

        if not month_to_fix and not day_to_fix:
            logger.warning("Please provide a month or day to fix.")
            return
        if month_to_fix and day_to_fix:
            logger.warning("Please only provide a month or a day to fix-not both.")
            return
        if not directory:
            logger.warning("Please provide a directory from which to parse archives.")
            return
        if not organisation:
            logger.warning(
                "Please provide an organisation for which to parse archives."
            )
            return
        if not collections:
            logger.warning(
                "Please provide an organisation which has collections for which to fix archives."
            )
            return
        try:
            conn = swift.swift_connection()
        except RuntimeError:
            logger.info("Swift credentials not provided. Skipping.")
            return False

        # get existing archives to ensure we have not already aggregated
        existing_link_aggregates_in_object_storage = self._get_existing_link_aggregates(
            conn
        )
        # get all URLPatterns for an organisation
        url_patterns = URLPattern.objects.filter(collection__in=collections)

        if month_to_fix:
            first_day_of_month = self._get_first_day_of_month(month_to_fix)
            last_day_of_month = self._get_last_day_of_month(first_day_of_month)
            # if we already have aggregates for this month uploaded, don't try to re-aggregate
            # or if we have not archived all events for the given timeframe, don't try to re-aggregate
            if self._has_aggregates_for_month(
                existing_link_aggregates_in_object_storage, month_to_fix
            ) or self._has_link_events_for_month(first_day_of_month, last_day_of_month):
                return
            # otherwise, attempt re-aggregation
            with transaction.atomic():
                self._process_monthly_aggregates(
                    directory, month_to_fix, organisation, url_patterns, last_day_of_month
                )
        else:
            # if we already have aggregates for this day uploaded, don't try to re-aggregate
            # or if we have not archived all events for the given timeframe, don't try to re-aggregate
            if self._has_aggregates_for_day(
                existing_link_aggregates_in_object_storage, day_to_fix
            ) or self._has_link_events_for_day(day_to_fix):
                return
            # otherwise, attempt re-aggregation
            with transaction.atomic():
                self._process_daily_aggregates(
                    collections, day_to_fix, directory, url_patterns
                )

    def _get_existing_link_aggregates(self, conn):
        """
        This function gets existing link aggregates from object storage.
        Parameters
        ----------
        conn : swiftclient.Connection
            A connection to the Swift object storage.

        Returns
        -------
        An array of existing link aggregates from object storage.
        """
        existing_link_aggregates_in_object_storage = [
            i["name"]
            for i in swift.get_object_list(
                conn,
                os.environ.get("SWIFT_CONTAINER_AGGREGATES", "archive-aggregates"),
                "aggregates_linkaggregate_",
            )
        ]
        return existing_link_aggregates_in_object_storage

    def _has_aggregates_for_month(
        self, existing_link_aggregates_in_object_storage, month_to_fix
    ):
        """
        This function checks whether there are existing aggregates for the month to fix.
        Parameters
        ----------
        existing_link_aggregates_in_object_storage :  An array of existing link aggregates from object storage.

        month_to_fix :  str

        Returns
        -------
        bool: whether there are existing aggregates for a given month in object storage
        """
        return (
            len(
                [
                    i
                    for i in existing_link_aggregates_in_object_storage
                    if self._get_first_day_of_month(month_to_fix).strftime("%Y-%m") in i
                ]
            )
            > 0
        )

    def _has_aggregates_for_day(
        self, existing_link_aggregates_in_object_storage, day_to_fix
    ):
        """
        This function checks whether there are existing aggregates for the day to fix.
        Parameters
        ----------
        existing_link_aggregates_in_object_storage :  An array of existing link aggregates from object storage.

        day_to_fix :  str

        Returns
        -------
        bool: whether there are existing aggregates for a given day in object storage
        """
        day_to_fix_formatted = (
            datetime.fromisoformat(day_to_fix).date().strftime("%Y-%m-%d")
        )
        return (
            len(
                [
                    i
                    for i in existing_link_aggregates_in_object_storage
                    if day_to_fix_formatted in i
                ]
            )
            > 0
        )

    def _has_link_events_for_month(self, first_day_of_month, last_day_of_month):
        return LinkEvent.objects.filter(timestamp__gte=first_day_of_month, timestamp__lte=last_day_of_month).count() > 0

    def _has_link_events_for_day(self, day_to_fix):
        day = datetime.fromisoformat(day_to_fix)
        return LinkEvent.objects.filter(timestamp__gte=day, timestamp__lte=day + timedelta(days=1)).count() > 0


    def _process_daily_aggregates(
        self, collections, day_to_fix, directory, url_patterns
    ):
        """
        This function loops through each url pattern and link event to fill the daily aggregates.
        Parameters
        ----------
        collections :  An array of collections

        day_to_fix :  str

        directory :  str

        url_patterns :  An array of url patterns

        Returns
        -------
        None
        """
        # pull month string from day input parameter
        month_to_fix = day_to_fix[:-2]
        # load and split link events by url pattern
        events_split_by_url_pattern = self._load_events_from_archives(
            directory, month_to_fix, [i.url for i in url_patterns]
        )
        # loop through each collection
        for collection in collections:
            collection_url_pattern_strings = [
                i.url for i in url_patterns.filter(collection=collection)
            ]
            # loop through each collection's URLPatterns
            for collection_url_string in collection_url_pattern_strings:
                # get the link events for the collection and day
                for link_event in self._get_link_events_for_day(
                    collection_url_string,
                    events_split_by_url_pattern,
                    int(day_to_fix[-2:]),
                ):
                    # create daily aggregates
                    self._fill_daily_aggregate(collection, link_event)

    def _fill_daily_aggregate(self, collection, link_event):
        """
        This function updates or creates a daily LinkAggregate for a collection and a parsed JSON object(LinkEvent).
        Parameters
        ----------
        collection :  Collection

        link_event :  obj

        Returns
        -------
        None
        """
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
        if existing_link_aggregate is None:
            # Create a new link aggregate
            links_added = change_number if change_number > 0 else 0
            links_removed = 1 if change_number == 0 else 0
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
        else:
            if change_number == 0:
                existing_link_aggregate.total_links_removed += 1
            else:
                existing_link_aggregate.total_links_added += 1
            existing_link_aggregate.save()

    def _process_monthly_aggregates(
        self, directory, month_to_fix, organisation, url_patterns, last_day_of_month
    ):
        """
        This function loops through each url pattern and link events to fill the monthly aggregates.
        Parameters
        ----------
        directory :  str

        month_to_fix :  str

        organisation :  Organisation

        url_patterns :  An array of url patterns

        Returns
        -------
        None
        """
        # load and split link events by url pattern
        events_split_by_url_pattern = self._load_events_from_archives(
            directory, month_to_fix, [i.url for i in url_patterns]
        )
        # get the first and last day of the month to fix
        for url_pattern, link_events in events_split_by_url_pattern.items():
            # create monthly aggregates
            self._fill_monthly_aggregate(
                url_pattern, last_day_of_month, organisation, url_patterns, link_events
            )

    def _fill_monthly_aggregate(
        self, url_pattern, last_day_of_month, organisation, url_patterns, link_events
    ):
        """
        This function fills monthly LinkAggregates for an organisation and a parsed JSON object(LinkEvent).
        Parameters
        ----------
        url_pattern :  str

        last_day_of_month :  date

        organisation :  Organisation

        url_patterns :  An array of url patterns

        link_events :  an array of link event JSON objects

        Returns
        -------
        None
        """
        # find the collection associated with this url
        collection = url_patterns.filter(url=url_pattern).first().collection
        self._process_monthly_events(
            True, link_events, collection, organisation, last_day_of_month
        )
        self._process_monthly_events(
            False, link_events, collection, organisation, last_day_of_month
        )

    def _process_monthly_events(
        self,
        on_user_list_flag,
        link_events,
        collection,
        organisation,
        last_day_of_month,
    ):
        """
        This function updates or creates a monthly LinkAggregate for a collection and parsed JSON objects(LinkEvents).
        Parameters
        ----------
        on_user_list_flag :  bool, whether the aggregate should save with on_user_list flag or not

        link_events :  an array of link event JSON objects

        collection: a Collection

        organisation: Organisation

        last_day_of_month:  date

        Returns
        -------
        None
        """
        events = [
            i for i in link_events if i["fields"]["on_user_list"] is on_user_list_flag
        ]
        if not events:
            return

        total_added = sum(1 for i in events if i["fields"]["change"] == 1)
        total_removed = sum(1 for i in events if i["fields"]["change"] == 0)

        existing_aggregate = LinkAggregate.objects.filter(
            organisation_id=organisation.id,
            collection_id=collection.id,
            on_user_list=on_user_list_flag,
            full_date=last_day_of_month,
            day=0,
        )

        if existing_aggregate.exists():
            existing_aggregate.update(
                total_links_added=total_added,
                total_links_removed=total_removed,
            )
        else:
            LinkAggregate.objects.create(
                organisation_id=organisation.id,
                collection_id=collection.id,
                on_user_list=on_user_list_flag,
                full_date=last_day_of_month,
                day=0,
                total_links_added=total_added,
                total_links_removed=total_removed,
            )

    def _get_link_events_for_day(
        self, collection_url: str, events_split_by_url_pattern, day: int
    ):
        """
        This function splits parsed JSON objects(LinkEvent) by collection url pattern.
        Parameters
        ----------
        collection_url :  str

        events_split_by_url_pattern :  an array of link event JSON objects

        Returns
        -------
        link_events_for_day :  an array of link event JSON objects filtered by day
        """
        link_events_for_day = [
            j
            for j in events_split_by_url_pattern[collection_url]
            if datetime.fromisoformat(j["fields"]["timestamp"]).date().day == day
        ]
        return link_events_for_day

    def _get_last_day_of_month(self, first_day_of_month: date) -> date:
        """
        This function gets the last day of the month from the first day of the input month
        Parameters
        ----------
        first_day_of_month :  date

        Returns
        -------
        date
        """
        if first_day_of_month.month == 12:
            return first_day_of_month.replace(day=31)
        replace = first_day_of_month.replace(month=first_day_of_month.month + 1)
        return replace - timedelta(days=1)

    def _get_first_day_of_month(self, month_to_fix: str) -> date:
        """
        This function gets the first day of the month from the input month
        Parameters
        ----------
        month_to_fix :  str

        Returns
        -------
        date
        """
        return datetime.strptime(month_to_fix, "%Y%m").date().replace(day=1)

    def _load_events_from_archives(
        self, directory: object, month_to_fix: str, url_pattern_strings
    ) -> object:
        """Parse archived .json.gz files and split the link events by URL pattern.
        Parameters
        ----------
        directory :  str

        month_to_fix :  str

        url_pattern_strings :  an array of str

        Returns
        -------
        parsed JSON link event objects
        """
        events_split_by_url_pattern = {url: [] for url in url_pattern_strings}
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
