from datetime import date, timedelta

from django.db.models import Count, Q, Avg

from extlinks.links.models import LinkEvent

from logging import getLogger

logger = getLogger('django')


def get_change_data_by_time(queryset):
    """
    Given a queryset of LinkEvent objects, returns the number of events
    per unit time. Returns lists of dates, links added, links removed.
    """

    if queryset:
        earliest_date = queryset.earliest('timestamp').timestamp.date()
        current_date = date.today()

        data_range = current_date - earliest_date

        num_links_added, num_links_removed = [], []
        dates = []

        # Split data by day
        if data_range.days < 90:
            while current_date >= earliest_date:

                num_links_added.append(queryset.filter(
                    timestamp__date=current_date,
                    change=LinkEvent.ADDED).count())
                num_links_removed.append(queryset.filter(
                    timestamp__date=current_date,
                    change=LinkEvent.REMOVED).count())

                dates.append(current_date.strftime('%Y-%m-%d'))
                current_date -= timedelta(days=1)
        else:
            while current_date >= earliest_date:
                num_links_added.append(queryset.filter(
                    timestamp__date__month=current_date.month,
                    timestamp__date__year=current_date.year,
                    change=LinkEvent.ADDED).count())
                num_links_removed.append(queryset.filter(
                    timestamp__date__month=current_date.month,
                    timestamp__date__year=current_date.year,
                    change=LinkEvent.REMOVED).count())
                dates.append(current_date.replace(day=1).strftime('%Y-%m-%d'))

                # Figure out what the last month is regardless of today's date
                current_date = current_date.replace(day=1) - timedelta(days=1)

        return dates[::-1], num_links_added[::-1], num_links_removed[::-1]
    else:
        return [], [], []


def get_linksearchtotal_data_by_time(queryset):
    """
    Given a queryset of LinkSearchTotal objects, returns the totals
    per month. Returns lists of dates and totals
    """

    if queryset:
        earliest_date = queryset.earliest('date').date
        current_date = date.today()

        linksearch_data = []
        dates = []

        while current_date >= earliest_date:
            # Find the average link count for this month
            average_month_data = queryset.filter(
                date__month=current_date.month,
                date__year=current_date.year,
            ).aggregate(Avg('total'))

            linksearch_data.append(round(average_month_data['total__avg']))
            dates.append(current_date.replace(day=1).strftime('%Y-%m-%d'))

            # Figure out what the last month is regardless of today's date
            current_date = current_date.replace(day=1) - timedelta(days=1)

        return dates[::-1], linksearch_data[::-1]
    else:
        return [], []


def annotate_top(queryset, order_by, fields, num_results=None):
    queryset = queryset.values(
        *fields).annotate(
        links_added=Count('change',
                          filter=Q(change=LinkEvent.ADDED)),
        links_removed=Count('change',
                            filter=Q(change=LinkEvent.REMOVED))).order_by(
        order_by
    )
    if num_results:
        return queryset[:num_results]
    else:
        return queryset


def top_organisations(org_list, linkevents, num_results=None):
    annotated_orgs = org_list.annotate(
        links_added=Count(
            'collection__url__linkevent',
            filter=Q(
                collection__url__linkevent__in=linkevents,
                collection__url__linkevent__change=LinkEvent.ADDED)),
        links_removed=Count(
            'collection__url__linkevent',
            filter=Q(
                collection__url__linkevent__in=linkevents,
                collection__url__linkevent__change=LinkEvent.REMOVED)),
    ).order_by('-links_added')
    if num_results:
        return annotated_orgs[:5]
    else:
        return annotated_orgs


def filter_queryset(queryset, filter_dict):
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
