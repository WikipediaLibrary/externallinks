from django.core.management.base import BaseCommand, call_command

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.links.models import URLPattern, LinkEvent
from extlinks.organisations.models import Organisation, Collection


class Command(BaseCommand):
    help = "Monitors page-links-change for link events"

    def handle(self, *args, **options):
        # Delete organisations, collections and URLPatterns that are related to
        # EZProxy
        ezproxy_org = Organisation.objects.filter(name="Wikipedia Library OCLC EZProxy")
        ezproxy_collection = Collection.objects.filter(name="EZProxy")
        url_patterns = URLPattern.objects.filter(collection=ezproxy_collection)

        LinkAggregate.objects.filter(
            organisation=ezproxy_org, collection=ezproxy_collection
        ).delete()
        PageProjectAggregate.objects.filter(
            organisation=ezproxy_org, collection=ezproxy_collection
        ).delete()
        UserAggregate.objects.filter(
            organisation=ezproxy_org, collection=ezproxy_collection
        ).delete()

        url_patterns.delete()
        ezproxy_collection.delete()
        ezproxy_org.delete()

        # Get LinkEvents that have no URLPatterns associated
        linkevents = LinkEvent.objects.filter(url__isnull=True)

        collections = Collection.objects.all()

        linkevents_changed = 0
        for collection in collections:
            collection_urls = collection.url.all()
            for url_pattern in collection_urls:
                for linkevent in linkevents:
                    proxy_url = url_pattern.url.replace(".", "-")
                    if url_pattern.url in linkevent.link or proxy_url in linkevent.link:
                        linkevent.url.add(url_pattern)
                        linkevents_changed += 1
            if linkevents_changed > 0:
                # There have been changes to this collection, so we must delete
                # the aggregates tables for that collection and run the commands
                # for it
                LinkAggregate.objects.filter(collection=collection).delete()
                PageProjectAggregate.objects.filter(collection=collection).delete()
                UserAggregate.objects.filter(collection=collection).delete()

                call_command("fill_link_aggregates", collections=[collection.pk])
                call_command("fill_pageproject_aggregates", collections=[collection.pk])
                call_command("fill_user_aggregates", collections=[collection.pk])
