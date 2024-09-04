from django.core.management.base import BaseCommand
from django.core.management import call_command

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.links.models import LinkEvent


class Command(BaseCommand):
    help = "Fixes all those proxy linkevents that aren't in the user list"

    def handle(self, *args, **options):
        proxy_not_on_user_list_linkevents = LinkEvent.objects.filter(
            link__contains="wikipedialibrary.idm.oclc", on_user_list=False
        )

        if proxy_not_on_user_list_linkevents.exists():
            earliest_link_date = proxy_not_on_user_list_linkevents.earliest(
                "timestamp"
            ).timestamp
            collection_list = set()
            for linkevent in proxy_not_on_user_list_linkevents:
                # Get URLPatterns associated with the linkevent
                urls = linkevent.get_url_patterns
                # Get the organisation from the first url
                if urls:
                    collection = urls[0].collection
                    collection_list.add(collection.id)
                    organisation = collection.organisation
                    username_list = organisation.username_list
                    if username_list:
                        if linkevent.username in username_list.all():
                            linkevent.on_user_list = True
                            linkevent.save()

            if collection_list:
                LinkAggregate.objects.filter(
                    collection__in=collection_list, full_date__gte=earliest_link_date
                ).delete()
                PageProjectAggregate.objects.filter(
                    collection__in=collection_list, full_date__gte=earliest_link_date
                ).delete()
                UserAggregate.objects.filter(
                    collection__in=collection_list, full_date__gte=earliest_link_date
                ).delete()

                call_command("fill_link_aggregates", collections=collection_list)
                call_command("fill_pageproject_aggregates", collections=collection_list)
                call_command("fill_user_aggregates", collections=collection_list)
