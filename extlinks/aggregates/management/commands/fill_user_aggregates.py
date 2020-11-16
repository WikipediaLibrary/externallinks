from datetime import date, timedelta, datetime

from django.core.management.base import BaseCommand
from django.core.exceptions import ValidationError
from django.db.models import Count, Q, Prefetch
from django.db.models.functions import Cast
from django.db.models.fields import DateField

from ...models import UserAggregate
from extlinks.links.models import LinkEvent, URLPattern
from extlinks.organisations.models import Collection


class Command(BaseCommand):
    help = "Adds aggregated data into the UserAggregate table"

    def handle(self, *args, **options):
        today = date.today()
        last_day_of_last_month = today.replace(day=1) - timedelta(days=1)
        yesterday = today - timedelta(days=1)

        # Check if the UserAggregate table is empty
        if UserAggregate.objects.exists():
            latest_aggregated_link_date = UserAggregate.objects.latest("full_date")
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
            # There are no page aggregates, getting all LinkEvents from yesterday and backwards
            link_event_filter = Q(timestamp__lte=yesterday)

        self._process_collections(link_event_filter)

    def _process_collections(self, link_event_filter):
        """
        This function loops through all collections to check on new link events
        filtered by the dates passed in link_event_filter

        Parameters
        ----------
        link_event_filter : Q
            A Q query object to filter LinkEvents by. If the UserAggregate table
            is empty, it will query all LinkEvents. If it has data, it will query
            by the latest date in the table and today

        Returns
        -------
        None
        """
        collections = Collection.objects.all().prefetch_related("url")

        for collection in collections:
            url_patterns = collection.url.all()
            for url_pattern in url_patterns:
                link_events_with_annotated_timestamp = url_pattern.linkevent.annotate(
                    timestamp_date=Cast("timestamp", DateField())
                )
                link_events = (
                    link_events_with_annotated_timestamp.values(
                        "username__username", "timestamp_date"
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
                self._fill_user_aggregates(link_events, collection)

    def _fill_user_aggregates(self, link_events, collection):
        """
        This function loops through all link events in a URLPattern of a collection
        to check if a UserAggregate with prior information exists.
        If a UserAggregate exists, it checks if there have been any changes to the
        links added and links removed sums. If there are any changes, then the
        UserAggregate row is updated.

        Parameters
        ----------
        link_events : list(LinkEvent)
            A list of filtered and annotated LinkEvents that contains the sum of
            all links added and removed on a certain date
        collection: Collection
            The collection the LinkEvents came from. Will be used to fill the
            UserAggregate table

        Returns
        -------
        None
        """
        for link_event in link_events:
            if UserAggregate.objects.filter(
                organisation=collection.organisation,
                collection=collection,
                username=link_event["username__username"],
                full_date=link_event["timestamp_date"],
            ).exists():
                # Query UserAggregate for the existing field
                existing_link_aggregate = UserAggregate.objects.get(
                    organisation=collection.organisation,
                    collection=collection,
                    username=link_event["username__username"],
                    full_date=link_event["timestamp_date"],
                )
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
                UserAggregate.objects.create(
                    organisation=collection.organisation,
                    collection=collection,
                    username=link_event["username__username"],
                    full_date=link_event["timestamp_date"],
                    total_links_added=link_event["links_added"],
                    total_links_removed=link_event["links_removed"],
                )
