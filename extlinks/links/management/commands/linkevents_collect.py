# Based heavily on
# https://github.com/Samwalton9/hashtags/blob/master/scripts/collect_hashtags.py
import hashlib
from datetime import datetime
import json
import logging
import sys
from sseclient import SSEClient as EventSource
from urllib.parse import unquote
from zoneinfo import ZoneInfo

from extlinks.common.management.commands import BaseCommand

from extlinks.links.helpers import link_is_tracked
from extlinks.links.models import LinkEvent, URLPattern
from extlinks.organisations.models import User

logger = logging.getLogger("django")


class Command(BaseCommand):
    help = "Monitors page-links-change for link events"

    def add_arguments(self, parser):
        parser.add_argument(
            "--historical",
            action="store_true",
            help="Parse event stream from last logged event",
        )

        parser.add_argument(
            "--test",
            nargs=1,
            help="Test the command without having to access the stream. Passes a json event",
        )

    def _handle(self, *args, **options):
        base_stream_url = "https://stream.wikimedia.org/v2/stream/page-links-change"

        if options["test"]:
            event_data = options["test"]
            self._evaluate_link(event_data)
            # Since we are not testing the EventStream functionality, we finish
            # execution here
            sys.exit(0)

        # Every time this script is started, find the latest entry in the
        # database, and start the eventstream from there. This ensures that in
        # the event of any downtime, we always maintain 100% data coverage (up
        # to the ~7 days that the EventStream historical data is kept anyway).
        if options["historical"]:
            all_events = LinkEvent.objects.all()
            if all_events.exists():
                latest_datetime = all_events.latest().timestamp
                latest_date_formatted = latest_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

                url = base_stream_url + "?since={date}".format(
                    date=latest_date_formatted
                )
            else:
                url = base_stream_url
        else:
            url = base_stream_url

        self._process_events(url)

    def _process_events(self, url):
        # Eventsource should fail if it can't read data after a while.
        for event in EventSource(
            url,
            # The retry argument sets the delay between retries in milliseconds.
            # We're setting this to 5 minutes.
            # There's no way to set the max_retries value with this library,
            # but since it depends upon requests, which in turn uses urllib3
            # by default, we get a default max_retries value of 3.
            retry=60000,
            # The timeout argument gets passed to requests.get.
            # An integer value sets connect (socket connect) and
            # read (time to first byte / since last byte) timeout values.
            # A tuple value sets each respective value independently.
            # https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
            timeout=(3.05, 7),
            headers={
                "User-Agent": 'Wikilink'
            }
        ):
            if event.event == "message":
                try:
                    event_data = json.loads(event.data)
                except ValueError:
                    continue

                self._evaluate_link(event_data)

    def _evaluate_link(self, event_data):
        if "added_links" in event_data:
            self._process_links(event_data["added_links"], LinkEvent.ADDED, event_data)

        if "removed_links" in event_data:
            self._process_links(
                event_data["removed_links"], LinkEvent.REMOVED, event_data
            )

    def _process_links(self, link_list, change, event_dict):
        """
        Given a list of links, process them.
        Change = 0: Removed
        Change = 1: Added
        """

        for link in link_list:
            if link["external"]:
                if link_is_tracked(link["link"]):
                    # URLs in the stream are encoded (e.g. %3D instead of =)
                    unquoted_url = unquote(link["link"])

                    event_id = event_dict["meta"]["id"]
                    link_event_id = unquoted_url + event_id
                    hash = hashlib.sha256()
                    hash.update(link_event_id.encode("utf-8"))
                    event_objects = LinkEvent.objects.filter(
                        hash_link_event_id=hash.hexdigest()
                    )

                    # We skip the URL if the length is greater than 2083
                    if not event_objects.exists() and len(unquoted_url) < 2084:
                        self._add_linkevent_to_db(unquoted_url, change, event_dict)

    def _add_linkevent_to_db(self, link, change, event_data):
        if "." in event_data["meta"]["dt"]:
            string_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        elif "Z" in event_data["meta"]["dt"]:
            string_format = "%Y-%m-%dT%H:%M:%SZ"
        else:
            string_format = "%Y-%m-%dT%H:%M:%S+00:00"
        datetime_object = datetime.strptime(event_data["meta"]["dt"], string_format)
        try:
            username = event_data["performer"]["user_text"]
        except KeyError:
            # Per https://phabricator.wikimedia.org/T216726, edits to Flow
            # pages have no performer, so we'll abandon logging this event
            # rather than worry about how to present such an edit.
            logger.info(
                "Skipped event {event_id} due to no performer".format(
                    event_id=event_data["meta"]["id"]
                )
            )
            return

        # Find the db object for this user, or create it if we haven't logged
        # an edit from them before now.
        username_object, created = User.objects.get_or_create(username=username)

        url_patterns = URLPattern.objects.matches(link)

        # We make a hard assumption here that a given link, despite
        # potentially being associated with multiple url patterns, should
        # ultimately only be associated with a single organisation.
        # I can't think of any situation when this wouldn't be the
        # case, but I can't wait to find out why I'm wrong.
        on_user_list = False
        this_link_collection = url_patterns[0].collection

        if hasattr(this_link_collection, "organisation"):
            this_link_org = url_patterns[0].collection.organisation
            if hasattr(this_link_org, "username_list"):
                username_list = this_link_org.username_list
                if username_list:
                    if username_object in username_list.all():
                        on_user_list = True
            else:
                logger.error(
                    "Collection {this_link_collection}, Organization {this_link_org} has no username list.".format(
                        this_link_collection=this_link_collection,
                        this_link_org=this_link_org,
                    )
                )
        else:
            logger.error(
                "Collection {this_link_collection} has no organisation.".format(
                    this_link_collection=this_link_collection
                )
            )

        # Log actions such as page moves and image uploads have no
        # revision ID.
        try:
            revision_id = event_data["rev_id"]
        except KeyError:
            revision_id = None

        try:
            user_id = event_data["performer"]["user_id"]
        except KeyError:
            # IPs have no user_id
            user_id = None

        link_event = LinkEvent.objects.create(
            link=link,
            timestamp=datetime_object.replace(tzinfo=ZoneInfo("UTC")),
            domain=event_data["meta"]["domain"],
            username=username_object,
            rev_id=revision_id,
            user_id=user_id,
            page_title=event_data["page_title"],
            page_namespace=event_data["page_namespace"],
            event_id=event_data["meta"]["id"],
            change=change,
            on_user_list=on_user_list,
            user_is_bot=event_data["performer"]["user_is_bot"],
        )
        for url_pattern in url_patterns:
            url_pattern.link_events.add(link_event)
            url_pattern.collections.add(this_link_collection)
            url_pattern.save()
