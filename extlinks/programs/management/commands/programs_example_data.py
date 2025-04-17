import random
from faker import Faker

from extlinks.common.management.commands import BaseCommand

from extlinks.links.models import URLPattern
from extlinks.organisations.models import Organisation, Collection, User
from extlinks.programs.models import Program


class Command(BaseCommand):
    help = "Creates a range of test programs, organisations, and collections"

    def add_arguments(self, parser):
        parser.add_argument("num", nargs="+", type=int)

    def _handle(self, *args, **options):
        num_programs = options["num"][0]

        fake = Faker()

        for i in range(num_programs):
            new_program = Program(
                name="Program {num}".format(num=i),
                description=fake.text(max_nb_chars=200),
            )
            new_program.save()

            for j in range(random.randint(1, 20)):
                # Will this org limit by user?
                limit_by_user = random.choice([True, False])

                new_org = Organisation(name=fake.company())
                new_org.save()
                if limit_by_user:
                    # Between 10 and 50 users on the list.
                    username_list = [
                        fake.user_name() for _ in range(random.randint(10, 50))
                    ]
                    for username in username_list:
                        user, _ = User.objects.get_or_create(username=username)
                        new_org.username_list.add(user)
                new_org.program.add(new_program)

                for k in range(random.randint(1, 3)):
                    new_collection = Collection(
                        name=fake.sentence(nb_words=3)[:-1], organisation=new_org
                    )
                    new_collection.save()

                    for l in range(random.randint(1, 2)):
                        new_urlpattern = URLPattern(
                            # Strip https:// and /
                            url=fake.url(schemes=["https"])[8:-1],
                        )
                        new_urlpattern.save()
                        new_urlpattern.collections.add(new_collection)
                        new_urlpattern.save()
