from datetime import date, timedelta

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
                    change=LinkEvent.ADDED).count())
                num_links_removed.append(queryset.filter(
                    timestamp__date__month=current_date.month,
                    change=LinkEvent.REMOVED).count())
                dates.append(current_date.replace(day=1).strftime('%Y-%m-%d'))

                # Figure out what the last month is regardless of today's date
                current_date = current_date.replace(day=1) - timedelta(days=1)

        return dates[::-1], num_links_added[::-1], num_links_removed[::-1]
    else:
        return [], [], []
