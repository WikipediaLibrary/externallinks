from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = "Dump data for a range of dates"

    def add_arguments(self, parser):
        parser.add_argument(
            "--start", type=str, help="Start date (YYYY-MM-DD)", required=True
        )
        parser.add_argument(
            "--end", type=str, help="End date (YYYY-MM-DD)", required=True
        )
        parser.add_argument("--output", type=str, help="Output prefix", required=True)

    def handle(self, *args, **options):
        start_date = datetime.strptime(options["start"], "%Y-%m-%d")
        end_date = datetime.strptime(options["end"], "%Y-%m-%d")
        output = options["output"]

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            self.stdout.write(f"Calling linkevents_archive for date: {date_str}...")

            # Call the existing dump command within Django
            call_command("linkevents_archive", "dump", date=current_date, output=output)

            current_date += timedelta(days=1)

        self.stdout.write(self.style.SUCCESS("All dumps completed!"))
