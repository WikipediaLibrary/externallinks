from datetime import datetime, timedelta, timezone
import random
from faker import Faker

from django.core.management import BaseCommand

from extlinks.organisations.models import User
from ...models import URLPattern, LinkEvent


class Command(BaseCommand):
    help = "Backfills a set of linkevents for each url pattern"

    def add_arguments(self, parser):
        parser.add_argument("num_events", nargs="+", type=int)

    def handle(self, *args, **options):
        # Number of link events to log in total
        num_events = options["num_events"][0]

        fake = Faker()
        languages = ["en", "de", "fr", "he", "hi", "ta"]
        users = User.objects.all()
        # Hacky way of adding a weighted random choice of change type.
        # Addition is likely to be more prevalent.
        change_choices = [
            LinkEvent.ADDED,
            LinkEvent.ADDED,
            LinkEvent.ADDED,
            LinkEvent.REMOVED,
        ]

        urlpatterns = URLPattern.objects.all()

        for _ in range(num_events):
            urlpattern = random.choice(urlpatterns)
            organisation = urlpattern.collection.organisation
            random_user = random.choice(users)

            # If this org limits by user, choose either a random user who
            # isn't on the org's user list, or from the org's user list.
            on_user_list = False
            if organisation.limit_by_user:
                username_list = organisation.username_list.all()
                if random_user in username_list:
                    on_user_list = True

            LinkEvent.objects.create(
                link=urlpattern.url + "/" + fake.word(),
                timestamp=fake.date_time_between(
                    start_date=datetime.now() - timedelta(days=365),
                    end_date="now",
                    tzinfo=timezone.utc,
                ),
                domain=random.choice(languages) + ".wikipedia.org",
                username=random_user,
                rev_id=random.randint(10000000, 100000000),
                user_id=random.randint(10000000, 100000000),
                page_title=fake.word(),
                page_namespace=0,
                event_id=fake.uuid4(),
                change=random.choice(change_choices),
                on_user_list=on_user_list,
            )

