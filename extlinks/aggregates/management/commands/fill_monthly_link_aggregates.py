from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max, Q

from ...models import LinkAggregate

logger = logging.getLogger("django")


class Command(BaseCommand):
    help = "Adds monthly aggregated data into the LinkAggregate table"

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument(
            "--collections",
            nargs="+",
            type=int,
            help="A list of collection IDs that will be processed instead of every collection",
        )

    def handle(self, *args, **options):
        logger.info("Monthly LinkAggregate job started")

        if options["collections"]:
            today = date.today()
            first_day_of_month = today.replace(day=1)
            last_day_of_last_month = first_day_of_month - timedelta(days=1)
            for col_id in options["collections"]:
                # Let's just ensure this collection is eligible for aggregation
                start_date_filter = self._get_start_date_filter(col_id)
                filter_query = (
                    Q(
                        collection_id=col_id,
                        full_date__lte=last_day_of_last_month,
                    )
                    & start_date_filter
                )
                daily_aggregation_exists = (
                    LinkAggregate.objects.filter(filter_query).exclude(day=0).exists()
                )
                if daily_aggregation_exists:
                    self._process_aggregation(col_id, start_date_filter)
                else:
                    logger.info(
                        f"Collection '{col_id}' has no need to aggregate monthly data"
                    )
        else:
            start_date_filter = self._get_start_date_filter()
            self._process_aggregation(start_date_filter=start_date_filter)

        logger.info("Monthly LinkAggregate job ended")

    def _get_start_date_filter(self, collection_id=None):
        """
        This function gets the latest full_date reported in the monthly
        aggregation. It is used to filter LinkAggregate later in the main
        process so we don't go through all dates every time.

        Parameters
        ----------
        collection_id : int|None
            The collection ID for getting the max full_date of the
            monthly aggregation

        Returns
        -------
        Q object
        """
        result = Q()

        base_query = Q(day=0)
        if collection_id is not None:
            base_query &= Q(collection_id=collection_id)

        latest_date_aggregation = LinkAggregate.objects.filter(base_query).aggregate(
            Max("full_date")
        )["full_date__max"]
        if latest_date_aggregation is not None:
            first_day_of_next_month = latest_date_aggregation.replace(
                day=1
            ) + relativedelta(months=1)
            result &= Q(full_date__gte=first_day_of_next_month)

        return result

    def _process_aggregation(self, collection_id=None, start_date_filter=Q()):
        """
        Process all daily aggregations from last month (including) and
        backwards.

        Monthly aggregation sums total_links_added and total_links_removed
        for the entire month for each group of collection_id,
        organisation_id, and on_user_list, which is the same granularity
        in the daily aggregation (fill_link_aggregates.py).

        Parameters
        ---------
        collection_id : int|None
            The collection ID this process is focused on, or None if it
            should run for all collections

        start_date_filter : Q|None
            The starting full_date in which this process should start
            scanning, or None for a full scan

        Returns
        -------
        None
        """
        today = date.today()
        first_day_of_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_month - timedelta(days=1)

        daily_aggregations_query = Q(full_date__lte=last_day_of_last_month)
        daily_aggregations_query &= start_date_filter
        if collection_id is not None:
            daily_aggregations_query &= Q(collection_id=collection_id)

        daily_aggregations = (
            LinkAggregate.objects.filter(daily_aggregations_query)
            .exclude(day=0)
            .order_by("organisation_id", "collection_id", "on_user_list", "full_date")
            .iterator()  # This may be a big dataset - let's prevent caching
        )

        total_links_added = 0
        total_links_removed = 0
        aggregated_items = []  # For storing the items to be deleted
        prev_item = None

        for aggregation in daily_aggregations:
            # Similar granulation to the daily process, except this is
            # considering the whole month
            # Changing this should also impact the daily aggregation
            if prev_item is not None and (
                prev_item.organisation_id != aggregation.organisation_id
                or prev_item.collection_id != aggregation.collection_id
                or prev_item.on_user_list != aggregation.on_user_list
                or prev_item.year != aggregation.year
                or prev_item.month != aggregation.month
            ):
                self._save_aggregation(
                    aggregated_items, total_links_added, total_links_removed
                )
                aggregated_items = []
                total_links_added = 0
                total_links_removed = 0

                if (
                    prev_item.organisation_id != aggregation.organisation_id
                    or prev_item.collection_id != aggregation.collection_id
                ):
                    logger.info(
                        f"Monthly LinkAggregate for organisation {prev_item.organisation_id} "
                        f"collection {prev_item.collection_id} processed successfully"
                    )

            total_links_added += aggregation.total_links_added
            total_links_removed += aggregation.total_links_removed

            aggregated_items.append(aggregation)
            prev_item = aggregation

        if prev_item is not None and aggregated_items:
            self._save_aggregation(
                aggregated_items, total_links_added, total_links_removed
            )
            logger.info(
                f"Monthly LinkAggregate for organisation {prev_item.organisation_id} "
                f"collection {prev_item.collection_id} processed successfully"
            )

    def _save_aggregation(
        self, aggregated_items, total_links_added, total_links_removed
    ):
        """
        Saves the monthly aggregation and delete the daily ones.

        - If a monthly aggregation already exists, it will be deleted
        - The last day of the group will be used to store the monthly
        aggregation, with the day set to 0

        Parameters
        ----------
        aggregated_items: List[LinkAggregate]
            The list of daily LinkAggregate objects to be deleted in
            favor of the monthly aggregation

        total_links_added: int
            The summed total of links added from the daily aggregation

        total_links_removed: int
            The summed total of links removed from the daily aggregation

        Returns
        -------
        None
        """
        if not aggregated_items:
            return

        aggregation_last_day = aggregated_items.pop()
        aggregation_last_day.day = 0
        aggregation_last_day.total_links_added = total_links_added
        aggregation_last_day.total_links_removed = total_links_removed

        # Use a slice (LIMIT 1) instead of .first() to prevent
        # Django from adding an 'ORDER BY id ASC' clause that can
        # potentially slow down this query for some collections.
        existing_monthly_aggregate = LinkAggregate.objects.filter(
            collection_id=aggregation_last_day.collection_id,
            organisation_id=aggregation_last_day.organisation_id,
            on_user_list=aggregation_last_day.on_user_list,
            year=aggregation_last_day.year,
            month=aggregation_last_day.month,
            day=0,
        )[:1]
        existing_monthly_aggregate = (
            existing_monthly_aggregate[0]
            if len(existing_monthly_aggregate) > 0
            else None
        )

        with transaction.atomic():
            if existing_monthly_aggregate is not None:
                existing_monthly_aggregate.delete()

            # New monthly aggregation
            aggregation_last_day.save()

            for delete_item in aggregated_items:
                delete_item.delete()
