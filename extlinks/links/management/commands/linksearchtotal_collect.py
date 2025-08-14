import csv
from urllib.parse import urlparse

import MySQLdb
import os

from extlinks.common.management.commands import BaseCommand
from django.db import close_old_connections

from extlinks.links.models import LinkSearchTotal, URLPattern
from extlinks.settings.base import BASE_DIR


class Command(BaseCommand):
    help = "Updates link totals from externallinks table"

    def _handle(self, *args, **options):

        with open(os.path.join(BASE_DIR, "wiki-list.csv"), "r") as wiki_list:
            csv_reader = csv.reader(wiki_list)
            wiki_list_data = []
            for row in csv_reader:
                wiki_list_data.append(row[0])

        all_urlpatterns = URLPattern.objects.all()

        total_links_dictionary = {}
        for i, language in enumerate(wiki_list_data):
            db = MySQLdb.connect(
                host="{lang}wiki.analytics.db.svc.wikimedia.cloud".format(
                    lang=language
                ),
                user=os.environ["REPLICA_DB_USER"],
                passwd=os.environ["REPLICA_DB_PASSWORD"],
                db="{lang}wiki_p".format(lang=language),
            )

            cur = db.cursor()

            for urlpattern in all_urlpatterns:
                # For the first language, initialise tracking
                if i == 0:
                    total_links_dictionary[urlpattern.pk] = 0
                # adding default protocol even though we don't use it
                # in our SQL query so that we can leverage urlparse function
                url = "https://" + urlpattern.url
                url_parsed = urlparse(url)
                url_path = url_parsed.path
                url_host = url_parsed.hostname
                url_host_reversed = '.'.join(list(reversed(url_host.split("."))))

                query = f"""
                   SELECT COUNT(*) FROM externallinks
                    WHERE el_to_domain_index LIKE '%{url_host_reversed}%'
                    AND el_to_path LIKE '%{url_path}%'
                    """

                cur.execute(
                    query
                )

                this_num_urls = cur.fetchone()[0]

                total_links_dictionary[urlpattern.pk] += this_num_urls

        for urlpattern_pk, total_count in total_links_dictionary.items():
            linksearch_object = LinkSearchTotal(
                url=URLPattern.objects.get(pk=urlpattern_pk), total=total_count
            )
            linksearch_object.save()

        close_old_connections()
