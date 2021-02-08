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
    help = "Deletes the EZProxy collection and organisation and reassigns those LinkEvents to new URLPatterns"

    def handle(self, *args, **options):
        ezproxy_org = self._get_ezproxy_organisation()
        ezproxy_collection = self._get_ezproxy_collection()
        url_patterns = self._get_ezproxy_url_patterns(ezproxy_collection)

        if ezproxy_org and ezproxy_collection and url_patterns:
            self._delete_aggregates_ezproxy(
                ezproxy_org, ezproxy_collection, url_patterns
            )

        # Get LinkEvents that have no URLPatterns associated
        linkevents = LinkEvent.objects.filter(url__isnull=True)

        collections = Collection.objects.all()

        self._process_linkevents_collections(linkevents, collections)

    def _get_ezproxy_organisation(self):
        """
        Gets the EZProxy organisation, or returns None if it's already been deleted

        Parameters
        ----------

        Returns
        -------
        Organisation object or None
        """
        if Organisation.objects.filter(name="Wikipedia Library OCLC EZProxy").exists():
            return Organisation.objects.get(name="Wikipedia Library OCLC EZProxy")

        return None

    def _get_ezproxy_collection(self):
        """
        Gets the EZProxy collection, or returns None if it's already been deleted

        Parameters
        ----------

        Returns
        -------
        Collection object or None
        """
        if Collection.objects.filter(name="EZProxy").exists():
            return Collection.objects.get(name="EZProxy")

        return None

    def _get_ezproxy_url_patterns(self, collection):
        """
        Gets the EZProxy collection, or returns None if it's already been deleted

        Parameters
        ----------
        collection: The collection the URLPatterns belong to

        Returns
        -------
        URLPattern object or None
        """
        if collection and URLPattern.objects.filter(collection=collection).exists():
            return URLPattern.objects.get(collection=collection)

        return None

    def _delete_aggregates_ezproxy(self, ezproxy_org, ezproxy_collection, url_patterns):
        """
        Deletes any aggregate with the EZProxy collection and organisation,
        then deletes the collection, organisation and url patterns

        Parameters
        ----------
        ezproxy_org: Organisation
        The organisation to filter and delete the aggregates tables and that
        will later be deleted

        ezproxy_collection: Collection
        The collection to filter and delete the aggregates tables and that
        will later be deleted

        url_patterns: URLPattern
        The EZProxy URLPatterns that will be deleted

        Returns
        -------

        """
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
        """
        Loops through all collections to get their url patterns. If a linkevent
        link coincides with a URLPattern, it is added to that LinkEvent. That way,
        it will be counted when the aggregates commands are run again

        Parameters
        ----------
        linkevents: Queryset[LinkEvent]
        LinkEvent that have no URLPatterns assigned (therefore no collection assigned)

        collections: Queryset[Collection]
        All of the collections

        Returns
        -------

        """
        for collection in collections:
            linkevents_changed = 0
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
