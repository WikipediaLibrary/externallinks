from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = "Adds monthly aggregated data into the UserAggregate table"

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument(
            "--collections",
            nargs="+",
            type=int,
            help="A list of collection IDs that will be processed instead of every collection",
        )

    def handle(self, *args, **options):
        print("Monthly user aggregate")
