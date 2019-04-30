import random
from faker import Faker

from django.core.management import BaseCommand

from ...models import Program, Organisation, Collection
from ....links.models import URLPattern


class Command(BaseCommand):
    help = "Creates a range of test programs, organisations, and collections"

    def add_arguments(self, parser):
        parser.add_argument('num', nargs='+', type=int)

    def handle(self, *args, **options):
        num_programs = options['num'][0]

        fake = Faker()

        for i in range(num_programs):
            new_program = Program(
                name="Program {num}".format(num=i)
            )
            new_program.save()

            for j in range(random.randint(1, 20)):
                new_org = Organisation(
                    name=fake.company(),
                    program=new_program,
                )
                new_org.save()

                for k in range(random.randint(1, 3)):
                    new_collection = Collection(
                        name=fake.sentence(nb_words=3)[:-1],
                        organisation=new_org
                    )
                    new_collection.save()

                    for l in range(random.randint(1, 2)):
                        new_urlpattern = URLPattern(
                            # Strip https:// and /
                            url=fake.url(schemes=['https'])[8:-1],
                            collection=new_collection
                        )
                        new_urlpattern.save()
