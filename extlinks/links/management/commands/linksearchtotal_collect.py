import csv
from urllib.parse import urlparse

import MySQLdb
import os

from extlinks.common.management.commands import BaseCommand
from django.db import close_old_connections

from extlinks.links.helpers import reverse_host
from extlinks.links.models import LinkSearchTotal, URLPattern
from extlinks.settings.base import BASE_DIR


class Command(BaseCommand):
    help = "Updates link totals from externallinks table"

    def _handle(self, *args, **options):
        protocols = ["http", "https"]

        print("reading wiki-list")
        with open(os.path.join(BASE_DIR, "wiki-list.csv"), "r") as wiki_list:
            csv_reader = csv.reader(wiki_list)
            wiki_list_data = []
            for row in csv_reader:
                wiki_list_data.append(row[0])

        all_urlpatterns = URLPattern.objects.all()

        total_links_dictionary = {}
        for i, language in enumerate(wiki_list_data):
            print("connecting to db {}".format(language))
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
                print("searching url pattern {}".format(urlpattern))
                # For the first language, initialise tracking
                if i == 0:
                    total_links_dictionary[urlpattern.pk] = 0
                # adding default https protocol if we don't already have
                # a protocol in the url string so that we can leverage urlparse function
                if "://" not in urlpattern.url:
                    url = "https://" + urlpattern.url
                else:
                    url = urlpattern.url

                url_parsed = urlparse(url)
                url_host = url_parsed.hostname
                url_path = url_parsed.path
                for protocol in protocols:
                    query = f"""
                        SELECT COUNT(*) FROM externallinks
                        WHERE el_to_domain_index LIKE '{protocol}://{reverse_host(url_host)}%'
                        """
                    if len(url_path) > 0:
                        cond = f"""AND el_to_path LIKE '{url_path}%'
                        """
                        query += cond
                    print("executing query {}".format(query))
                    cur.execute(query)

                    this_num_urls = cur.fetchone()[0]

                    print("found {}".format(this_num_urls))
                    total_links_dictionary[urlpattern.pk] += this_num_urls

        for urlpattern_pk, total_count in total_links_dictionary.items():
            linksearch_object = LinkSearchTotal(
                url=URLPattern.objects.get(pk=urlpattern_pk), total=total_count
            )
            print("saving linksearch_object {}".format(linksearch_object))
            linksearch_object.save()

        close_old_connections()
