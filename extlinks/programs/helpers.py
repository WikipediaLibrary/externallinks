from datetime import date, timedelta

from extlinks.links.models import LinkEvent

from logging import getLogger

logger = getLogger('django')


def get_change_data_by_time(queryset, group_type='day'):
    """
    Given a queryset of LinkEvent objects, returns the number of events
    per unit time. Returns lists of dates, links added, links removed.
    """
    # TODO: When date form added, adjust day/month divide automatically.

    earliest_date = queryset.earliest('timestamp').timestamp.date()
    current_date = date.today()

    num_links_added, num_links_removed = [], []
    dates = []

    while current_date >= earliest_date:

        if group_type == 'day':
            num_links_added.append(queryset.filter(
                timestamp__date=current_date,
                change=LinkEvent.ADDED).count())
            num_links_removed.append(queryset.filter(
                timestamp__date=current_date,
                change=LinkEvent.REMOVED).count())

            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date -= timedelta(days=1)

    return dates[::-1], num_links_added[::-1], num_links_removed[::-1]
