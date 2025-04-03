from datetime import date, timedelta, datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, close_old_connections
from django.db.models import Count, Q
from django.db.models.functions import Cast
from django.db.models.fields import DateField

from ...models import LinkAggregate
from extlinks.links.models import LinkEvent
from extlinks.organisations.models import Collection


class Command(BaseCommand):
    help = "Adds aggregated data into the LinkAggregate table"

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument(
            "--collections",
            nargs="+",
            type=int,
            help="A list of collection IDs that will be processed instead of every collection",
        )

    def handle(self, *args, **options):
        if options["collections"]:
            for col_id in options["collections"]:
                collection = (
                    Collection.objects.filter(pk=col_id, organisation__isnull=False)
                    .prefetch_related("url")
                    .first()
                )
                if collection is None:
                    raise CommandError(f"Collection '{col_id}' does not exist")

                link_event_filter = self._get_linkevent_filter(collection)
                self._process_single_collection(link_event_filter, collection)
        else:
            # Looping through all collections
            link_event_filter = self._get_linkevent_filter()
            collections = Collection.objects.exclude(
                organisation__isnull=True
            ).prefetch_related("url")

            for collection in collections:
                self._process_single_collection(link_event_filter, collection)

        close_old_connections()

    def _get_linkevent_filter(self, collection=None):
        """
        This function checks if there is information in the LinkAggregate table
        to see what filters it should apply to the link events further on in the
        process

        Parameters
        ----------
        collection : Collection|None
            A collection to filter the LinkAggregate table. Is None by default

        Returns
        -------
        Q object
        """
        today = date.today()
        yesterday = today - timedelta(days=1)

        if collection is not None:
            linkaggregate_filter = Q(collection=collection)
        else:
            linkaggregate_filter = Q()

        latest_aggregated_link_date = (
            LinkAggregate.objects.filter(linkaggregate_filter)
            .order_by("full_date")
            .last()
        )

        if latest_aggregated_link_date is not None:
            latest_datetime = datetime(
                latest_aggregated_link_date.full_date.year,
                latest_aggregated_link_date.full_date.month,
                latest_aggregated_link_date.full_date.day,
                0,
                0,
                0,
            )
            link_event_filter = Q(
                timestamp__lte=today,
                timestamp__gte=latest_datetime,
            )
        else:
            # There are no link aggregates, getting all LinkEvents from yesterday and backwards
            link_event_filter = Q(timestamp__lte=yesterday)

        return link_event_filter

    def _process_single_collection(self, link_event_filter, collection):
        """
        This function loops through all url patterns in a collection to check on
        new link events filtered by the dates passed in link_event_filter

        Parameters
        ----------
        link_event_filter : Q
            A Q query object to filter LinkEvents by. If the LinkAggregate table
            is empty, it will query all LinkEvents. If it has data, it will query
            by the latest date in the table and today

        collection: Collection
            A specific collection to fetch all link events

        Returns
        -------
        None
        """
        url_patterns = collection.get_url_patterns()
        for url_pattern in url_patterns:
            link_events_with_annotated_timestamp = url_pattern.link_events.annotate(
                timestamp_date=Cast("timestamp", DateField())
            ).distinct()
            link_events = (
                link_events_with_annotated_timestamp.values(
                    "timestamp_date", "on_user_list"
                )
                .filter(link_event_filter)
                .annotate(
                    links_added=Count(
                        "pk",
                        filter=Q(change=LinkEvent.ADDED),
                        distinct=True,
                    ),
                    links_removed=Count(
                        "pk", filter=Q(change=LinkEvent.REMOVED), distinct=True
                    ),
                )
            )
            self._fill_link_aggregates(link_events, collection)

    def _fill_link_aggregates(self, link_events, collection):
        """
        This function loops through all link events in a URLPattern of a collection
        to check if a LinkAggregate with prior information exists.
        If a LinkAggregate exists, it checks if there have been any changes to the
        links added and links removed sums. If there are any changes, then the
        LinkAggregate row is updated.

        Parameters
        ----------
        link_events : list(LinkEvent)
            A list of filtered and annotated LinkEvents that contains the sum of
            all links added and removed on a certain date
        collection: Collection
            The collection the LinkEvents came from. Will be used to fill the
            LinkAggregate table

        Returns
        -------
        None
        """
        for link_event in link_events:
            # Granulation level for the daily aggregation.
            # Changing this filter should also impact the monthly
            # aggregation in `fill_monthly_link_aggregates.py`
            existing_link_aggregate = (
                LinkAggregate.objects.filter(
                    organisation=collection.organisation,
                    collection=collection,
                    full_date=link_event["timestamp_date"],
                    on_user_list=link_event["on_user_list"],
                )
                .exclude(day=0)
                .first()
            )
            if existing_link_aggregate is not None:
                if (
                    existing_link_aggregate.total_links_added
                    != link_event["links_added"]
                    or existing_link_aggregate.total_links_removed
                    != link_event["links_removed"]
                ):
                    # Updating the total links added and removed
                    existing_link_aggregate.total_links_added = link_event[
                        "links_added"
                    ]
                    existing_link_aggregate.total_links_removed = link_event[
                        "links_removed"
                    ]
                    existing_link_aggregate.save()
            else:
                # Create a new link aggregate
                with transaction.atomic():
                    LinkAggregate.objects.create(
                        organisation=collection.organisation,
                        collection=collection,
                        full_date=link_event["timestamp_date"],
                        total_links_added=link_event["links_added"],
                        total_links_removed=link_event["links_removed"],
                        on_user_list=link_event["on_user_list"],
                    )
