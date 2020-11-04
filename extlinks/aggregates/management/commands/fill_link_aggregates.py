from datetime import date, timedelta, datetime

from django.core.management.base import BaseCommand
from django.core.exceptions import ValidationError
from django.db.models import Count, Q, Prefetch
from django.db.models.functions import Cast
from django.db.models.fields import DateField

from ...models import LinkAggregate
from extlinks.links.models import LinkEvent, URLPattern
from extlinks.organisations.models import Collection


class Command(BaseCommand):
    help = "Adds aggregated data into the LinkAggregate table"

    def handle(self, *args, **options):
        today = date.today()
        last_day_of_last_month = today.replace(day=1) - timedelta(days=1)
        yesterday = today - timedelta(days=1)

        if LinkAggregate.objects.exists():
            latest_aggregated_link_date = LinkAggregate.objects.latest("full_date")
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

        self._process_collections(link_event_filter)

    def _process_collections(self, link_event_filter):
        collections = Collection.objects.all().prefetch_related("url")

        for collection in collections:
            url_patterns = collection.url.all()
            for url_pattern in url_patterns:
                link_events_with_annotated_timestamp = url_pattern.linkevent.annotate(
                    timestamp_date=Cast("timestamp", DateField())
                )
                link_events = (
                    link_events_with_annotated_timestamp.values("timestamp_date")
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
        for link_event in link_events:
            if LinkAggregate.objects.filter(
                organisation=collection.organisation,
                collection=collection,
                full_date=link_event["timestamp_date"],
            ).exists():
                # Query LinkAggregate for the existing field
                existing_link_aggregate = LinkAggregate.objects.get(
                    organisation=collection.organisation,
                    collection=collection,
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
                LinkAggregate.objects.create(
                    organisation=collection.organisation,
                    collection=collection,
                    full_date=link_event["timestamp_date"],
                    total_links_added=link_event["links_added"],
                    total_links_removed=link_event["links_removed"],
                )
