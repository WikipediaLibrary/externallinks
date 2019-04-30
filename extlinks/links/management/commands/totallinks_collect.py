# Adapted from
# https://github.com/Samwalton9/TWL-tools/blob/master/UpdateGlobalMetrics.py

import toolforge

from django.core.management.base import BaseCommand
from ...models import LinkSearchTotal, URLPattern


class Command(BaseCommand):
    help = "Uses externallinks table to get url usage totals"

    def handle(self, *args, **options):
        toolforge.set_user_agent('External links tool')

        protocols = ['http', 'https']

        # Wikipedias which had 1 million+ articles in 2018 for parity
        # with historical data.
        sites = ['de', 'en', 'es', 'fr', 'it', 'nl', 'ja', 'pl', 'ru', 'ceb',
                 'sv', 'vi', 'war']

        urls = URLPattern.objects.all()

        for url in urls:
            num_urls = 0

            search_term = url.url
            search_term = search_term.strip()  # Catch any trailing spaces
            if search_term[0] == "*":
                search_term = search_term[2:]

            url_start = search_term.split("/")[0].split(".")[::-1]
            url_optimised = '.'.join(url_start) + ".%"

            if "/" in search_term:
                url_end = "/".join(search_term.split("/")[1:])
                url_pattern_end = "%./" + url_end + "%"
            else:
                url_pattern_end = '%'

            for site in sites:
                # TODO: Connect properly.
                conn = toolforge.connect('{}wiki'.format(site))

                for current_protocol in protocols:
                    with conn.cursor() as cur:
                        url_pattern_start = current_protocol + "://" + url_optimised

                        cur.execute('''SELECT COUNT(*) FROM externallinks
                                       WHERE el_index LIKE '%s'
                                       AND el_index LIKE '%s'
                                    ''' % (url_pattern_start,
                                           url_pattern_end))

                        this_num_urls = cur.fetchone()[0]

                    num_urls += this_num_urls

            new_total = LinkSearchTotal(
                url=url,
                total=num_urls
            )
            new_total.save()
