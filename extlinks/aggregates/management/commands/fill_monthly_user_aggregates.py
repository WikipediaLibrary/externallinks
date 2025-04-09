import calendar
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import logging

from extlinks.common.management.commands import BaseCommand
from django.core.management.base import CommandError
from django.db import transaction, close_old_connections
from django.db.models import Count, Q, Sum

from ...models import UserAggregate
from extlinks.common.helpers import batch_iterator

logger = logging.getLogger("django")


class Command(BaseCommand):
    help = "Adds monthly aggregated data into the UserAggregate table"

    def info(self, msg):
        # Log and print so that messages are visible
        # in docker logs (log) and cron job logs (print)
        logger.info(msg)
        self.stdout.write(msg)
        self.stdout.flush()

    def add_arguments(self, parser):
        # Option to filter by specific collection(s)
        parser.add_argument(
            "--collections",
            nargs="+",
            type=int,
            help="A list of collection IDs that will be processed instead of every collection",
        )

        # Option to filter by specific YYYY-MM
        parser.add_argument(
            "--year-month",
            type=str,
            help="A specific year-month (YYYY-MM) to aggregate data for. Example: '2024-01'",
        )

    def _handle(self, *args, **options):
        self.info("Monthly UserAggregate job started")

        if options["year_month"]:
            try:
                selected_year, selected_month = map(
                    int, options["year_month"].split("-")
                )
                first_day_of_month = date(selected_year, selected_month, 1)
                last_day_of_month = (
                    first_day_of_month + relativedelta(months=1) - timedelta(days=1)
                )
            except ValueError:
                raise CommandError(
                    "Invalid format for --year-month. Use YYYY-MM (e.g., 2024-01)."
                )
        else:
            today = date.today()
            try:
                oldest_agg = UserAggregate.objects.exclude(day=0).earliest("full_date")
            except UserAggregate.DoesNotExist:
                self.info("No data to process.")
                return
            oldest_date = oldest_agg.full_date
            monthrange = calendar.monthrange(oldest_date.year, oldest_date.month)
            first_day_of_month = oldest_date.replace(day=1)
            last_day_of_month = oldest_date.replace(day=monthrange[1])
            no_later_than_date = today - timedelta(days=10)
            if last_day_of_month > no_later_than_date:
                self.info(
                    f"No data within allowed date range: {no_later_than_date} falls within the month of {oldest_date}"
                )
                return

        self.info(f"Processing data from {first_day_of_month} to {last_day_of_month}")
        month_filter = Q(
            full_date__gte=first_day_of_month, full_date__lte=last_day_of_month
        )

        if options["collections"]:
            filter_query = Q(collection_id__in=options["collections"]) & month_filter
            self._process_aggregation(filter_query)
        else:
            self._process_aggregation(month_filter)

        self.info("Monthly UserAggregate job ended")
        close_old_connections()

    def _process_aggregation(self, main_filter_query):
        """
        Process all daily aggregations in UserAggregate into
        monthly aggregations.

        Monthly aggregation sums total_links_added and total_links_removed
        for the entire month for each group of collection_id,
        organisation_id, username, and on_user_list, which is the same
        granularity in the daily aggregation (fill_user_aggregates.py).

        Parameters
        ---------
        main_filter_query : Q
            The main filter in which this process should start scanning.

        Returns
        -------
        None
        """
        self.info("Fetching the main query")

        aggregated_data = (
            UserAggregate.objects.filter(main_filter_query)
            .exclude(day=0)
            .values(
                "organisation_id",
                "collection_id",
                "username",
                "on_user_list",
                "year",
                "month",
            )
            .annotate(
                monthly_total_links_added=Sum("total_links_added"),
                monthly_total_links_removed=Sum("total_links_removed"),
                count=Count("id"),  # For logging and double-checking
            )
            .iterator()  # This may be a big dataset - let's prevent caching
        )

        total_aggregations = 0
        # Batch transactions for memory efficiency
        for batch_index, batch in enumerate(batch_iterator(aggregated_data), start=1):
            with transaction.atomic():
                total_aggregations += len(batch)
                self.info(f"Processing batch {batch_index} (size: {len(batch)})")
                for monthly_aggregation in batch:
                    self._verify_and_save_aggregation(monthly_aggregation)

        self.info(f"Processed a total of {total_aggregations} monthly aggregations")

    def _verify_and_save_aggregation(self, monthly_aggregation):
        """
        Saves the monthly aggregation and delete the daily ones.
        It also verifies if expected deletion count and actual deleted
        daily aggregations count match.

        Parameters
        ----------
        monthly_aggregation : UserAggregate
            The calculated monthly aggregation

        Returns
        -------
        None
        """
        expected_delete_count = monthly_aggregation["count"]
        deleted_count, _ = (
            UserAggregate.objects.filter(
                organisation_id=monthly_aggregation["organisation_id"],
                collection_id=monthly_aggregation["collection_id"],
                username=monthly_aggregation["username"],
                on_user_list=monthly_aggregation["on_user_list"],
                year=monthly_aggregation["year"],
                month=monthly_aggregation["month"],
            )
            .exclude(day=0)
            .delete()
        )

        if deleted_count != expected_delete_count:
            raise CommandError(
                f"Delete count mismatch: Expected to delete {expected_delete_count} records, "
                f"but actually deleted {deleted_count} - organisation_id={monthly_aggregation['organisation_id']}, "
                f"collection_id={monthly_aggregation['collection_id']}, username={monthly_aggregation['username']}, "
                f"year={monthly_aggregation['year']}, month={monthly_aggregation['month']}",
            )

        existing_monthly_aggregate = UserAggregate.objects.filter(
            organisation_id=monthly_aggregation["organisation_id"],
            collection_id=monthly_aggregation["collection_id"],
            username=monthly_aggregation["username"],
            on_user_list=monthly_aggregation["on_user_list"],
            year=monthly_aggregation["year"],
            month=monthly_aggregation["month"],
            day=0,
        )[:1]
        existing_monthly_aggregate = (
            existing_monthly_aggregate[0]
            if len(existing_monthly_aggregate) > 0
            else None
        )

        if existing_monthly_aggregate:
            existing_monthly_aggregate.total_links_added += monthly_aggregation[
                "monthly_total_links_added"
            ]
            existing_monthly_aggregate.total_links_removed += monthly_aggregation[
                "monthly_total_links_removed"
            ]
            existing_monthly_aggregate.save()
        else:
            last_day_of_month = (
                date(monthly_aggregation["year"], monthly_aggregation["month"], 1)
                + relativedelta(months=1)
                - timedelta(days=1)
            )
            UserAggregate.objects.create(
                organisation_id=monthly_aggregation["organisation_id"],
                collection_id=monthly_aggregation["collection_id"],
                username=monthly_aggregation["username"],
                on_user_list=monthly_aggregation["on_user_list"],
                full_date=last_day_of_month,
                day=0,
                total_links_added=monthly_aggregation["monthly_total_links_added"],
                total_links_removed=monthly_aggregation["monthly_total_links_removed"],
            )
