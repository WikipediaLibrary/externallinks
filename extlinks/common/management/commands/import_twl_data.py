import csv

from django.core.management import BaseCommand

from extlinks.links.models import URLPattern
from extlinks.organisations.models import Organisation, Collection
from extlinks.programs.models import Program


class Command(BaseCommand):
    help = """
           Imports Programs, Orgs, Collections, and URLPatterns from The Wikipedia
           Library's old metrics collection system"""

    def add_arguments(self, parser):
        parser.add_argument('file_path', nargs='+', type=str)

    def handle(self, *args, **options):
        file_path = options['file_path'][0]

        # Check TWL program exists, if it doesn't, create it.
        try:
            twl_program = Program.objects.get(name='The Wikipedia Library')
        except Program.DoesNotExist:
            twl_program = Program(
                name='The Wikipedia Library'
            )
            twl_program.save()

        with open(file_path, 'r') as input_file:
            csv_reader = csv.reader(input_file)
            csv_reader.next()
            for row in csv_reader:
                organisation = row[0]
                collection = row[1]
                urlpattern = row[2]
                twl_link = row[3]
                print(row)

                # Create Organisation
                try:
                    organisation_object = Organisation.objects.get(
                        name=organisation
                    )
                except Organisation.DoesNotExist:
                    organisation_object = Organisation(
                        name=organisation
                    )
                    if twl_link == 'x':
                        organisation_object.program = twl_program
                    organisation_object.save()

                # Create Collection
                try:
                    collection_object = Collection.objects.get(
                        organisation=organisation_object,
                        name=collection,
                    )
                except Collection.DoesNotExist:
                    collection_object = Collection(
                        name=collection,
                        organisation=organisation_object
                    )
                    collection_object.save()

                # Create URLPattern
                # We shouldn't have any duplicates here but let's be safe.
                try:
                    urlpattern_object = URLPattern.objects.get(
                        url=urlpattern
                    )
                except URLPattern.DoesNotExist:
                    urlpattern_object = URLPattern(
                        url=urlpattern,
                        collection=collection_object
                    )
                    urlpattern_object.save()
