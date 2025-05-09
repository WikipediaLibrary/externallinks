import os

from extlinks.common.management.commands import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    help = "Uploads all archives currently located in the backup directory to object storage"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dir",
            help="The directory from which to upload archives.",
            type=str
        )

    def _handle(self, *args, **options):
        path = options['dir']
        for filename in os.listdir(path):
            if filename.endswith('.json.gz') and filename.startswith('links_linkevent_'):
                file_path = os.path.join(path, filename)
                if os.path.isfile(file_path):
                    call_command("linkevents_archive", "upload", file_path)
