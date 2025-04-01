from django.core.management.base import BaseCommand as DjangoBaseCommand
from filelock import FileLock
import inspect
from os import remove
from os.path import basename


class BaseCommand(DjangoBaseCommand):
    """
    Django BaseCommand wrapper that adds file locks to management commands
    """

    def handle(self, *args, **options):
        lockname = basename(inspect.getfile(self.__class__))
        # Use a lockfile to prevent overruns.
        lockfile = "/tmp/{}.lock".format(lockname)
        lock = FileLock(lockfile)
        lock.acquire()
        try:
            self._handle(*args, **options)
        finally:
            lock.release()
            remove(lockfile)
