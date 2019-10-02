from datetime import date, timedelta

from django.db.models import Count, Q, Avg
from django.db.models.functions import TruncMonth

from extlinks.links.models import LinkEvent

from logging import getLogger

logger = getLogger('django')


def get_date_count(dates, check_date):
    """
    Find the number of events on a given date, given a dictionary of counts.

    dates -- a dictionary of date:count pairs
    check_date -- the datetime.date object to look for

    Returns the count corresponding to date if found, and 0 otherwise.
    """
    for date_data in dates:
        if date_data['timestamp__date'] == check_date:
            return date_data['event_count']

    return 0


def num_months_equal(dates, check_date):
    """
    Find the number of events in a given month, given a dictionary of counts.

    dates -- a dictionary of date:count pairs
    check_date -- the datetime.date object to look for

    Returns the summed count corresponding to the date's month and year. Will
    return zero in the case that the list is empty.
    """
    return sum([dt['event_count'] for dt in dates if
                dt['timestamp__date'].month == check_date.month
                and dt['timestamp__date'].year == check_date.year])


def get_change_data_by_time(queryset):
    """
    Calculates per-unit-time data from a queryset of LinkEvents for graphs.

    Given a queryset of LinkEvent objects, returns the number of events
    per unit time. For less than 90 days data, returns data per day. For more,
    returns data per month.

    Returns three lists: dates, links added, and links removed.
    """

    if queryset.exists():
        earliest_date = queryset.earliest().timestamp.date()
        current_date = date.today()

        data_range = current_date - earliest_date

        num_links_added, num_links_removed = [], []
        dates = []

        # We could theoretically do the entire annotation process in one
        # database query, but we want to line up dates between added and
        # removed events and ensure we have intervening months accounted for
        added_dates = queryset.filter(change=LinkEvent.ADDED).values(
            'timestamp__date').annotate(
            # We need to count on pk with distinct=True to remove duplicate
            # events from multiple collections
            event_count=Count('pk', distinct=True))
        removed_dates = queryset.filter(change=LinkEvent.REMOVED).values(
            'timestamp__date').annotate(
            event_count=Count('pk', distinct=True))

        # Split data by day
        if data_range.days < 90:
            # Count back from the current date
            while current_date >= earliest_date:

                num_links_added.append(
                    get_date_count(added_dates, current_date))
                num_links_removed.append(
                    get_date_count(removed_dates, current_date))

                dates.append(current_date.strftime('%Y-%m-%d'))
                current_date -= timedelta(days=1)
        else:
            while current_date >= earliest_date:
                num_links_added.append(
                    num_months_equal(added_dates, current_date))
                num_links_removed.append(
                    num_months_equal(removed_dates, current_date))
                dates.append(current_date.replace(day=1).strftime('%Y-%m-%d'))

                # Figure out what the last month is regardless of today's date
                current_date = current_date.replace(day=1) - timedelta(days=1)

        # We want the resulting data to go oldest->newest, so these lists
        # all need reversing
        return dates[::-1], num_links_added[::-1], num_links_removed[::-1]
    else:
        return [], [], []


def get_month_average(average_data, check_date):
    for avg_data in average_data:
        if avg_data['month'] == check_date:
            return avg_data['average']

    return 0


def get_linksearchtotal_data_by_time(queryset):
    """
    Calculates per-unit-time data from a queryset of LinkSearchTotal objects

    Given a queryset of LinkSearchTotal objects, returns the totals
    per month.

    Returns two lists: dates and totals
    """

    if queryset:
        earliest_date = queryset.earliest('date').date
        current_date = date.today()

        linksearch_data = []
        dates = []

        average_month_data = queryset.annotate(
            month=TruncMonth('date')).values('month').annotate(
            average=Avg('total')
        )

        while current_date >= earliest_date:
            month_first = current_date.replace(day=1)
            this_month_avg = get_month_average(average_month_data, month_first)

            linksearch_data.append(round(this_month_avg))
            dates.append(month_first.strftime('%Y-%m-%d'))

            # Figure out what the last month is regardless of today's date
            current_date = month_first - timedelta(days=1)

        # If a month has no data for some reason, we should use whatever
        # figure we have for the previous month.
        for i, data in enumerate(linksearch_data):
            if data == 0:
                linksearch_data[i] = linksearch_data[i+1]

        return dates[::-1], linksearch_data[::-1]
    else:
        return [], []


def annotate_top(queryset, order_by, fields, num_results=None):
    """
    Annotates specific values of a queryset with their count.

    queryset -- a LinkEvent queryset
    order_by -- a string denoting which field to order by
    fields -- a string denoting which fields to limit the values() select to
    num_results -- optional integer which limits results to some number

    Returns a queryset
    """
    queryset = queryset.values(
        *fields).annotate(
        links_added=Count('pk',
                          filter=Q(change=LinkEvent.ADDED),
                          distinct=True),
        links_removed=Count('pk',
                            filter=Q(change=LinkEvent.REMOVED),
                            distinct=True)).order_by(
        order_by
    )
    if num_results:
        return queryset[:num_results]
    else:
        return queryset


def top_organisations(org_list, linkevents, num_results=None):
    """
    Annotates the count of link events for each organisation in a queryset.

    org_list -- an Organisation queryset
    linkevents -- a LinkEvent queryset
    num_results -- optional integer which limits results to some number

    Returns a queryset
    """
    annotated_orgs = org_list.annotate(
        links_added=Count(
            'collection__url__linkevent__pk',
            filter=Q(
                collection__url__linkevent__in=linkevents,
                collection__url__linkevent__change=LinkEvent.ADDED),
            distinct=True),
        links_removed=Count(
            'collection__url__linkevent__pk',
            filter=Q(
                collection__url__linkevent__in=linkevents,
                collection__url__linkevent__change=LinkEvent.REMOVED),
            distinct=True),
    ).order_by('-links_added')
    if num_results:
        return annotated_orgs[:5]
    else:
        return annotated_orgs


def filter_queryset(queryset, filter_dict):
    """
    Adds filter conditions to a queryset based on form results.

    queryset -- a LinkEvent queryset
    filter_dict -- a dictionary of data from the user filter form

    Returns a queryset
    """
    if 'start_date' in filter_dict:
        start_date = filter_dict['start_date']
        if start_date:
            queryset = queryset.filter(
                timestamp__gte=start_date
            )

    if 'end_date' in filter_dict:
        end_date = filter_dict['end_date']
        if end_date:
            queryset = queryset.filter(
                timestamp__lte=end_date
            )

    if 'limit_to_user_list' in filter_dict:
        limit_to_user_list = filter_dict['limit_to_user_list']
        if limit_to_user_list:
            queryset = queryset.filter(
                on_user_list=True
            )

    return queryset


def filter_linksearchtotals(queryset, filter_dict):
    """
    Adds filter conditions to a LinkSearchTotal queryset based on form results.

    queryset -- a LinkSearchTotal queryset
    filter_dict -- a dictionary of data from the user filter form

    Returns a queryset
    """
    if 'start_date' in filter_dict:
        start_date = filter_dict['start_date']
        if start_date:
            queryset = queryset.filter(
                date__gte=start_date
            )

    if 'end_date' in filter_dict:
        end_date = filter_dict['end_date']
        if end_date:
            queryset = queryset.filter(
                date__lte=end_date
            )

    return queryset


def get_linkevent_context(context, queryset):
    """
    Given a context dictionary and LinkEvent queryset, add additional context

    Both Organisations and Programs need to add some of the same data when
    building their detail page context dictionaries. This adds that shared
    data to the context dictionary.

    context -- a get_context_data() context dictionary
    queryset -- a LinkEvent queryset

    Returns a dictionary
    """

    context['top_projects'] = annotate_top(queryset,
                                           '-links_added',
                                           ['domain'],
                                           num_results=5)

    all_users = annotate_top(queryset,
                             '-links_added',
                             ['username__username'])
    context['top_users'] = all_users[:5]

    context['latest_links'] = queryset.order_by(
            '-timestamp')[:10]

    # EventStream chart data
    dates, added_data_series, removed_data_series = get_change_data_by_time(
        queryset)

    context['eventstream_dates'] = dates
    context['eventstream_added_data'] = added_data_series
    context['eventstream_removed_data'] = removed_data_series

    # Stat block
    context['total_added'] = sum(added_data_series)
    context['total_removed'] = sum(removed_data_series)
    context['total_diff'] = context['total_added'] - context['total_removed']

    context['total_editors'] = len(all_users)
    context['total_projects'] = queryset.values_list(
        'domain').distinct().count()

    return context
