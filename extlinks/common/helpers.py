from datetime import date, timedelta
from itertools import islice

from django.db.models import Avg, Q
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
        # figure we have for the previous month, unless it is the current month
        for i, data in enumerate(linksearch_data):
            if data == 0 and i != len(linksearch_data) - 1:
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


def build_queryset_filters(form_data, collection_or_organisations):
    """
    This function parses a filter dictionary and creates Q object to filter
    the aggregates tables by

    Parameters
    ----------
    form_data: dict
        If the filter form has valid filters, then there will be a dictionary
        to filter the aggregates tables by dates or if a user is part of a user
        list

    collection_or_organisations : dict
        A dictionary that will have either a collection or a set of
        organisations to filter by.

    Returns
    -------
    Q : A Q object which will filter the aggregates queries
    """
    start_date = None
    end_date = None
    start_date_filter = Q()
    end_date_filter = Q()
    limit_to_user_list_filter = Q()
    # The aggregates queries will always be filtered by organisation
    if "organisations" in collection_or_organisations:
        collection_or_organisation_filter = Q(
            organisation__in=collection_or_organisations["organisations"]
        )
    elif "program" in collection_or_organisations:
        collection_or_organisation_filter = Q(
            program=collection_or_organisations["program"]
        )
    elif "linkevents" in collection_or_organisations:
        collection_or_organisation_filter = Q()
    else:
        collection_or_organisation_filter = Q(
            collection=collection_or_organisations["collection"]
        )

    if "start_date" in form_data:
        start_date = form_data["start_date"]
        if start_date:
            if "linkevents" in collection_or_organisations:
                start_date_filter = Q(timestamp__gte=start_date)
            else:
                start_date_filter = Q(full_date__gte=start_date)
    if "end_date" in form_data:
        end_date = form_data["end_date"]
        # The end date must not be greater than today's date
        if end_date:
            if "linkevents" in collection_or_organisations:
                end_date_filter = Q(timestamp__lte=end_date)
            else:
                end_date_filter = Q(full_date__lte=end_date)

    if "limit_to_user_list" in form_data:
        limit_to_user_list = form_data["limit_to_user_list"]
        if limit_to_user_list:
            limit_to_user_list_filter = Q(on_user_list=True)

    if start_date and end_date:
        # If the start date is greater tham the end date, it won't filter
        # by date
        if start_date >= end_date:
            return collection_or_organisation_filter & limit_to_user_list_filter

    return (
        collection_or_organisation_filter
        & limit_to_user_list_filter
        & start_date_filter
        & end_date_filter
    )


def batch_iterator(iterable, size=1000):
    """
    This yields successive batches from an iterable (memory-efficient).

    Used for large queries that use `.iterator()` for efficiency.
    Instead of loading all data into memory at once, this function
    retrieves items lazily in fixed-size batches.

    Parameters
    ----------
    iterable : Iterator
        An iterable object, typically a Django QuerySet with `.iterator()`,
        that returns items one by one in a memory-efficient manner.

    size : int
        The maximum number of items to include in each batch.

    Returns
    -------
    Iterator[List]
        An iterator that yields lists containing at most `size` items
        per batch.
    """
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch
