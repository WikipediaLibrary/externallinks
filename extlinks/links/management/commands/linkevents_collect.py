# Based heavily on
# https://github.com/Samwalton9/hashtags/blob/master/scripts/collect_hashtags.py

from datetime import datetime
import json
import logging
import pytz
from sseclient import SSEClient as EventSource
from urllib.parse import unquote

from django.core.management.base import BaseCommand

from extlinks.links.models import LinkEvent, URLPattern
from extlinks.organisations.models import User

logger = logging.getLogger('django')


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
            all_events = LinkEvent.objects.all()
            if all_events.count() > 0:
                latest_datetime = LinkEvent.objects.all().latest().timestamp
                latest_date_formatted = latest_datetime.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')

                url = base_stream_url + '?since={date}'.format(
                    date=latest_date_formatted)
            else:
                url = base_stream_url
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

        for link in link_list:
            if link['external']:
                if self.link_is_tracked(link['link']):
                    # URLs in the stream are encoded (e.g. %3D instead of =)
                    unquoted_url = unquote(link['link'])

                    event_id = event_dict['meta']['id']
                    event_objects = LinkEvent.objects.filter(link=unquoted_url,
                                                             event_id=event_id)

                    # We want to avoid link additions from e.g. InternetArchive
                    # where the URL takes the structure
                    # https://web.archive.org/https://test.com/
                    protocol_count = unquoted_url.count("//")
                    url_length = len(unquoted_url)

                    # We skip the URL if the length is greater than 2083
                    if not event_objects.exists() and protocol_count < 2 and url_length < 2084:
                        self.add_linkevent_to_db(unquoted_url, change,
                                                 event_dict)

    def link_is_tracked(self, link):
        tracked_links = URLPattern.objects.all().values_list('url', flat=True)

        # This is a quick check so we can filter the majority of events
        # which won't be matching our filters
        if any(links in link for links in tracked_links):
            # Then we do a more detailed check, to make sure this is the
            # root URL.
            for tracked_link in tracked_links:
                # If we track apa.org, we don't want to match iaapa.org
                # so we make sure the URL is actually pointing at apa.org
                url_starts = ["//" + tracked_link, "." + tracked_link]
                if any(start in link for start in url_starts):
                    return True
        else:
            return False


    def add_linkevent_to_db(self, link, change, event_data):
        if "Z" in event_data['meta']['dt']:
            string_format = '%Y-%m-%dT%H:%M:%SZ'
        else:
            string_format = '%Y-%m-%dT%H:%M:%S+00:00'
        datetime_object = datetime.strptime(event_data['meta']['dt'],
                                            string_format)
        try:
            username = event_data['performer']['user_text']
        except KeyError:
            # Per https://phabricator.wikimedia.org/T216726, edits to Flow
            # pages have no performer, so we'll abandon logging this event
            # rather than worry about how to present such an edit.
            logger.info('Skipped event {event_id} due to no performer'.format(
                event_id=event_data['meta']['id']
            ))
            return

        # Find the db object for this user, or create it if we haven't logged
        # an edit from them before now.
        username_object, created = User.objects.get_or_create(
            username=username
        )

        # All URL patterns matching this link
        tracked_urls = URLPattern.objects.all()
        url_patterns = [pattern for pattern in tracked_urls
                        if pattern.url in link]

        # We make a hard assumption here that a given link, despite
        # potentially being associated with multiple url patterns, should
        # ultimately only be associated with a single organisation.
        # I can't think of any situation when this wouldn't be the
        # case, but I can't wait to find out why I'm wrong.
        on_user_list = False
        this_link_org = url_patterns[0].collection.organisation
        username_list = this_link_org.username_list
        if username_list:
            if username_object in username_list.all():
                on_user_list = True

        # Log actions such as page moves and image uploads have no
        # revision ID.
        try:
            revision_id = event_data['rev_id']
        except KeyError:
            revision_id = None

        try:
            user_id = event_data['performer']['user_id']
        except KeyError:
            # IPs have no user_id
            user_id = None

        new_event = LinkEvent(
            link=link,
            timestamp=pytz.utc.localize(datetime_object),
            domain=event_data['meta']['domain'],
            username=username_object,
            rev_id=revision_id,
            user_id=user_id,
            page_title=event_data['page_title'],
            page_namespace=event_data['page_namespace'],
            event_id=event_data['meta']['id'],
            change=change,
            on_user_list=on_user_list,
            user_is_bot=event_data['performer']['user_is_bot']
        )
        new_event.save()

        # LinkEvent.url is a ManyToMany field, so we need to link these
        # objects in a different way.
        for pattern in url_patterns:
            new_event.url.add(pattern)
