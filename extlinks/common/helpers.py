from datetime import date, timedelta

from django.db.models import Avg
from django.db.models.functions import TruncMonth

from logging import getLogger

logger = getLogger("django")


def get_month_average(average_data, check_date):
    for avg_data in average_data:
        if avg_data["month"] == check_date:
            return avg_data["average"]

    return 0


def get_linksearchtotal_data_by_time(queryset):
    """
    Calculates per-unit-time data from a queryset of LinkSearchTotal objects

    Given a queryset of LinkSearchTotal objects, returns the totals
    per month.

    Returns two lists: dates and totals
    """

    if queryset:
        earliest_date = queryset.earliest("date").date
        current_date = date.today()

        linksearch_data = []
        dates = []

        average_month_data = (
            queryset.annotate(month=TruncMonth("date"))
            .values("month")
            .annotate(average=Avg("total"))
        )

        while current_date >= earliest_date:
            month_first = current_date.replace(day=1)
            this_month_avg = get_month_average(average_month_data, month_first)

            linksearch_data.append(round(this_month_avg))
            dates.append(month_first.strftime("%Y-%m-%d"))

            # Figure out what the last month is regardless of today's date
            current_date = month_first - timedelta(days=1)

        # If a month has no data for some reason, we should use whatever
        # figure we have for the previous month.
        for i, data in enumerate(linksearch_data):
            if data == 0:
                linksearch_data[i] = linksearch_data[i + 1]

        return dates[::-1], linksearch_data[::-1]
    else:
        return [], []


def filter_linksearchtotals(queryset, filter_dict):
    """
    Adds filter conditions to a LinkSearchTotal queryset based on form results.

    queryset -- a LinkSearchTotal queryset
    filter_dict -- a dictionary of data from the user filter form

    Returns a queryset
    """
    if "start_date" in filter_dict:
        start_date = filter_dict["start_date"]
        if start_date:
            queryset = queryset.filter(date__gte=start_date)

    if "end_date" in filter_dict:
        end_date = filter_dict["end_date"]
        if end_date:
            queryset = queryset.filter(date__lte=end_date)

    return queryset
