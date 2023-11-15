import csv
import MySQLdb
import os

from django.core.management import BaseCommand

from extlinks.links.helpers import split_url_for_query
from extlinks.links.models import LinkSearchTotal, URLPattern
from extlinks.settings.base import BASE_DIR


class Command(BaseCommand):
    help = "Updates link totals from externallinks table"

    def handle(self, *args, **options):
        protocols = ["http", "https"]

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

                url = urlpattern.url
                optimised_url, url_pattern_end = split_url_for_query(url)

                for protocol in protocols:
                    url_pattern_start = protocol + "://" + optimised_url

                    cur.execute(
                        """SELECT COUNT(*) FROM externallinks
                                WHERE el_to_domain_index LIKE '{url_start}'
                                AND el_to_domain_index LIKE '{url_end}'
                                """.format(
                            url_start=url_pattern_start, url_end=url_pattern_end
                        )
                    )

                    this_num_urls = cur.fetchone()[0]

                    total_links_dictionary[urlpattern.pk] += this_num_urls

        for urlpattern_pk, total_count in total_links_dictionary.items():
            linksearch_object = LinkSearchTotal(
                url=URLPattern.objects.get(pk=urlpattern_pk), total=total_count
            )
            linksearch_object.save()
