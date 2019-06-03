# Based heavily on
# https://github.com/Samwalton9/hashtags/blob/master/scripts/collect_hashtags.py

from datetime import datetime
import json
import pytz
from sseclient import SSEClient as EventSource

from django.core.management.base import BaseCommand

from extlinks.links.models import LinkEvent, URLPattern


class Command(BaseCommand):
    help = "Monitors page-links-change for link events"

    def add_arguments(self, parser):
        parser.add_argument('--historical', action='store_true',
                            help='Parse event stream from last logged event')

    def handle(self, *args, **options):
        base_stream_url = 'https://stream.wikimedia.org/v2/stream/page-links-change'

        # Every time this script is started, find the latest entry in the
        # database, and start the eventstream from there. This ensures that in
        # the event of any downtime, we always maintain 100% data coverage (up
        # to the ~30 days that the EventStream historical data is kept anyway).
        if options['historical']:
            url = base_stream_url
        else:
            all_events = LinkEvent.objects.all()
            if all_events.count() > 0:
                latest_datetime = LinkEvent.objects.all().latest().timestamp
                latest_date_formatted = latest_datetime.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')

                url = base_stream_url + '?since={date}'.format(
                    date=latest_date_formatted)
            else:
                url = base_stream_url

        for event in EventSource(url):
            if event.event == 'message':
                try:
                    event_data = json.loads(event.data)
                except ValueError:
                    continue

                if 'added_links' in event_data:
                    self.process_links(event_data['added_links'],
                                       LinkEvent.ADDED, event_data)

                if 'removed_links' in event_data:
                    self.process_links(event_data['removed_links'],
                                       LinkEvent.REMOVED, event_data)

    def process_links(self, link_list, change, event_dict):
        """
        Given a list of links, process them.
        Change = 0: Removed
        Change = 1: Added
        """
        tracked_links = URLPattern.objects.all().values_list('url', flat=True)

        for link in link_list:
            if link['external']:
                if any(links in link['link'] for links in tracked_links):

                    event_id = event_dict['meta']['id']
                    event_objects = LinkEvent.objects.filter(link=link['link'],
                                                             event_id=event_id)
                    if not event_objects.exists():
                        self.add_linkevent_to_db(link['link'], change,
                                                 event_dict)

    def add_linkevent_to_db(self, link, change, event_data):
        datetime_object = datetime.strptime(event_data['meta']['dt'],
                                            '%Y-%m-%dT%H:%M:%S+00:00')
        username = event_data['performer']['user_text']

        # Log actions such as page moves and image uploads have no
        # revision ID.
        try:
            revision_id = event_data['rev_id']
        except KeyError:
            revision_id = None

        # All URL patterns matching this link
        url_patterns = [pattern for pattern in URLPattern.objects.all()
                        if pattern.url in link]

        # We make a hard assumption here that a given link, despite
        # potentially being associated with multiple url patterns, should
        # ultimately only be associated with a single organisation.
        # I can't think of any situation when this wouldn't be the
        # case, but I can't wait to find out why I'm wrong.
        on_user_list = False
        this_link_org = url_patterns[0].collection.organisation
        if this_link_org.limit_by_user:
            username_list = this_link_org.username_list
            if username in username_list:
                on_user_list = True

        try:
            user_id = event_data['performer']['user_id']
        except KeyError:
            # IPs have no user_id
            user_id = None

        new_event = LinkEvent(
            link=link,
            timestamp=pytz.utc.localize(datetime_object),
            domain=event_data['meta']['domain'],
            username=username,
            rev_id=revision_id,
            user_id=user_id,
            page_title=event_data['page_title'],
            page_namespace=event_data['page_namespace'],
            event_id=event_data['meta']['id'],
            change=change,
            on_user_list=on_user_list,
        )
        new_event.save()

        # LinkEvent.url is a ManyToMany field, so we need to link these
        # objects in a different way.
        for pattern in url_patterns:
            new_event.url.add(pattern)
