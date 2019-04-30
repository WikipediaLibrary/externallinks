from datetime import date, timedelta
import random

from django.core.management import BaseCommand

from ...models import URLPattern, LinkSearchTotal


class Command(BaseCommand):
    help = "Backfills a set of LinkSearchTotals for each url pattern"

    def add_arguments(self, parser):
        parser.add_argument('weeks', nargs='+', type=int)

    def handle(self, *args, **options):
        # The number of weeks to go back
        num_dates = options['weeks'][0]

        urlpatterns = URLPattern.objects.all()
        for urlpattern in urlpatterns:
            date_total = random.randint(500, 30000)
            this_date = date.today()

            for _ in range(num_dates):

                # Each week, going backwards, we lose between 0 and 10%
                # of the total number of links.
                less_total = random.randint(0, int(date_total*0.1))
                date_total -= less_total

                new_total = LinkSearchTotal(
                    url=urlpattern,
                    date=this_date,
                    total=date_total
                )
                new_total.save()

                this_date = this_date - timedelta(days=7)
