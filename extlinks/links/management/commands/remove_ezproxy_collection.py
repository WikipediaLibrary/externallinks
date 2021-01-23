from django.core.management.base import BaseCommand
from django.core.management import call_command

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
        ezproxy_org = self._get_ezproxy_organisation()
        ezproxy_collection = self._get_ezproxy_collection()
        url_patterns = self._get_ezproxy_url_patterns(ezproxy_collection)

        self._delete_aggregates_ezproxy(ezproxy_org, ezproxy_collection, url_patterns)

        # Get LinkEvents that have no URLPatterns associated
        linkevents = LinkEvent.objects.filter(url__isnull=True)

        collections = Collection.objects.all()

        self._process_linkevents_collections(linkevents, collections)

    def _get_ezproxy_organisation(self):
        if Organisation.objects.filter(name="Wikipedia Library OCLC EZProxy").exists():
            return Organisation.objects.get(name="Wikipedia Library OCLC EZProxy")

        return None

    def _get_ezproxy_collection(self):
        if Collection.objects.filter(name="EZProxy").exists():
            return Collection.objects.get(name="EZProxy")

        return None

    def _get_ezproxy_url_patterns(self, collection):
        if collection and URLPattern.objects.filter(collection=collection).exists():
            return URLPattern.objects.get(collection=collection)

        return None

    def _delete_aggregates_ezproxy(self, ezproxy_org, ezproxy_collection, url_patterns):
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

    def _process_linkevents_collections(self, linkevents, collections):
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
