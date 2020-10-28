from datetime import date, timedelta, datetime

from django.db.models import Count, Q, Avg
from django.db.models.functions import TruncMonth

from extlinks.links.models import LinkEvent

from logging import getLogger

logger = getLogger("django")


def get_date_count(dates, check_date):
    """
    Find the number of events on a given date, given a dictionary of counts.

    dates -- a dictionary of date:count pairs
    check_date -- the datetime.date object to look for

    Returns the count corresponding to date if found, and 0 otherwise.
    """
    for date_data in dates:
        if datetime.strptime(date_data["timestamp"], "%Y-%m-%d") == check_date:
            return date_data["event_count"]

    return 0


def num_months_equal(dates, check_date):
    """
    Find the number of events in a given month, given a dictionary of counts.

    dates -- a dictionary of date:count pairs
    check_date -- the datetime.date object to look for

    Returns the summed count corresponding to the date's month and year. Will
    return zero in the case that the list is empty.
    """
    return sum(
        [
            dt["event_count"]
            for dt in dates
            if datetime.strptime(dt["timestamp"], "%Y-%m-%d").month == check_date.month
            and datetime.strptime(dt["timestamp"], "%Y-%m-%d").year == check_date.year
        ]
    )


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


def _get_linkevent_stats(linkevents, context):
    """
    Private function for processing linkevent stats without having to query the
    database.

    Parameters
    ----------
    linkevents: list[LinkEvent]
        The linkevents list of that URLPattern
    context: dict
        The context dictionary that is being filled in the get_context_data view
        function
    Returns
    -------
    dict
        The context dictionary with new stats added
    """
    # Instead querying information again, we will build the dictionary from
    # the raw data by iterating on the prefetched information
    top_pages = {}
    top_projects = {}
    top_users = {}
    latest_links = {}
    dates_dict = {}
    for linkevent in linkevents:
        page_key = f"{linkevent.domain}-{linkevent.page_title}"
        project_key = f"{linkevent.domain}"
        username_key = f"{linkevent.username.username}"
        dates_key = f"{linkevent.timestamp.date()}"

        top_pages = _fill_top_stats(
            linkevent,
            top_pages,
            page_key,
            ["domain", "page_title", "links_added", "links_removed"],
        )
        top_projects = _fill_top_stats(
            linkevent,
            top_projects,
            project_key,
            ["domain", "links_added", "links_removed"],
        )
        top_users = _fill_top_stats(
            linkevent,
            top_users,
            username_key,
            ["username", "links_added", "links_removed"],
        )
        latest_links = _fill_latest_links(linkevent, latest_links)

        dates_count = _fill_top_stats(
            linkevent,
            dates_dict,
            dates_key,
            ["links_added", "links_removed"],
        )

    sorted_page_keys = _sort_results(top_pages, ("links_added", "links_removed"), True)
    sorted_project_keys = _sort_results(
        top_projects, ("links_added", "links_removed"), True
    )
    sorted_user_keys = _sort_results(top_users, ("links_added", "links_removed"), True)
    sorted_link_keys = _sort_results(latest_links, ("timestamp",), True)

    sorted_top_pages = _limit_results(5, top_pages, sorted_page_keys)
    sorted_top_projects = _limit_results(5, top_projects, sorted_project_keys)
    sorted_top_users = _limit_results(5, top_users, sorted_user_keys)
    sorted_links = _limit_results(10, latest_links, sorted_link_keys)

    context = _get_linkevent_chart_stats(context, linkevents, dates_count)
    context["top_pages"] = sorted_top_pages
    context["top_projects"] = sorted_top_projects
    context["top_users"] = sorted_top_users
    context["latest_links"] = sorted_links
    context["total_editors"] = len(sorted_user_keys)
    context["total_projects"] = len(sorted_project_keys)

    # context = get_linkevent_context(context, linkevents)

    return context


def _fill_top_stats(linkevent, top_stat_dict, key_name, values_list):
    """
    Private function that fills the latest links

    Parameters
    ----------
    linkevent: LinkEvent
        A linkevent
    top_stat_dict: dict
        A dictionary that will be filled with the relevant information
    key_name: str
        The unique key name the stat will have
    values_list: list[str]
        A list of the values we need to extract from linkevent

    Returns
    -------
    dict
        A dictionary of the relevant linkevent information
    """
    if key_name in top_stat_dict:
        top_stat_dict[key_name]["links_added"] += linkevent.links_added
        top_stat_dict[key_name]["links_removed"] += linkevent.links_removed
    else:
        top_stat_dict[key_name] = {}
        for value in values_list:
            top_stat_dict[key_name][value] = getattr(linkevent, value)

    return top_stat_dict


def _fill_latest_links(linkevent, latest_links):
    """
    Private function that fills the latest links

    Parameters
    ----------
    linkevent: LinkEvent
        A linkevent
    latest_links: dict
        A dictionary of the latest links

    Returns
    -------
    dict
        A dictionary of the relevant linkevent information
    """
    links_key = f"{linkevent.link}-{linkevent.timestamp}"
    if links_key in latest_links:
        # this is a duplicate, should not happen
        pass
    else:
        latest_links[links_key] = {}
        latest_links[links_key]["link"] = linkevent.link
        latest_links[links_key]["domain"] = linkevent.domain
        latest_links[links_key]["username"] = linkevent.username.username
        latest_links[links_key]["page_title"] = linkevent.page_title
        latest_links[links_key]["rev_id"] = linkevent.rev_id
        latest_links[links_key]["change"] = linkevent.change
        latest_links[links_key]["timestamp"] = linkevent.timestamp

    return latest_links


def _sort_results(info_dict, sort_keys, reverse):
    """
    Private function to sort a dictionary by certain keys

    Parameters
    ----------
    info_dict: dict
        The dictionary that contains the statistics for projects, users or pages
    sort_keys: Tuple(str)
        A tuple of keys of the values the dictionary will be sorted by
    reverse: bool
        Whether the sorting should be reversed or not
    Returns
    -------
    list[str]
        Returns a sorted list of the info_dict keys
    """
    if len(sort_keys) == 2:
        return sorted(
            info_dict,
            key=lambda k: (info_dict[k][sort_keys[0]], info_dict[k][sort_keys[1]]),
            reverse=reverse,
        )
    else:
        return sorted(
            info_dict, key=lambda k: info_dict[k][sort_keys[0]], reverse=reverse
        )


def _limit_results(limit_number, info_dict, sorted_keys):
    """
    Private function to limit the number of elements returned in a list of dictionaries

    Parameters
    ----------
    limit_number: int
        The number of elements in the list you want to limit by
    info_dict: dict
        The dictionary that contains the statistics for projects, users or pages
    sorted_keys List[str]:
        An array of keys sorted by links_added and links_removed or by timestamp
    Returns
    -------
    list[str]
        Returns a limited list of dictionaries containing the top projects,
        users or pages or the most recent linkevents
    """
    top_array = []
    # If the sorted_keys array is less than the limit_number, we will use that as
    # the limit
    if len(sorted_keys) < limit_number:
        limit_number = len(sorted_keys)

    for key in range(limit_number):
        top_array.append(info_dict[sorted_keys[key]])

    return top_array


def _get_linkevent_chart_stats(context, linkevents, dates_count):
    """
    Private function to get context data to fill the charts of a linkevent

    Parameters
    ----------
    context: dict
        The context dictionary that will be filled with the data
    linkevents: LinkEvent
        Linkevent queryset
    dates_count List[dict]:
        An array of dictionaries that contains the links added and removed grouped by date
    Returns
    -------
    dict
        Returns the context dictionary with the filled relevant information
    """
    if linkevents.exists():
        earliest_date = linkevents.earliest().timestamp.date()
        current_date = date.today()

        data_range = current_date - earliest_date

        num_links_added, num_links_removed = [], []
        dates = []

        # We could theoretically do the entire annotation process in one
        # database query, but we want to line up dates between added and
        # removed events and ensure we have intervening months accounted for
        added_dates = []
        removed_dates = []
        for key in dates_count:
            added_dates.append(
                {"timestamp": key, "event_count": dates_count[key]["links_added"]}
            )
            removed_dates.append(
                {"timestamp": key, "event_count": dates_count[key]["links_removed"]}
            )

        # Split data by day
        if data_range.days < 90:
            # Count back from the current date
            while current_date >= earliest_date:

                num_links_added.append(get_date_count(added_dates, current_date))
                num_links_removed.append(get_date_count(removed_dates, current_date))

                dates.append(current_date.strftime("%Y-%m-%d"))
                current_date -= timedelta(days=1)
        else:
            while current_date >= earliest_date:
                num_links_added.append(num_months_equal(added_dates, current_date))
                num_links_removed.append(num_months_equal(removed_dates, current_date))
                dates.append(current_date.replace(day=1).strftime("%Y-%m-%d"))

                # Figure out what the last month is regardless of today's date
                current_date = current_date.replace(day=1) - timedelta(days=1)

        # We want the resulting data to go oldest->newest, so these lists
        # all need reversing
        context["eventstream_dates"] = dates[::-1]
        context["eventstream_added_data"] = num_links_added[::-1]
        context["eventstream_removed_data"] = num_links_removed[::-1]
        return context
    else:
        return context
