from django.core.management.base import BaseCommand as DjangoBaseCommand
from filelock import FileLock
import inspect
import logging
from os import remove
from os.path import basename
import sys
from tenacity import Retrying, stop_after_attempt, wait_exponential

logger = logging.getLogger("django")


class BaseCommand(DjangoBaseCommand):
    """
    Django BaseCommand wrapper that adds
        - file locks
        - up to 5 retries with exponential backoff
    """

    def __init__(self, *args, **options):
        super().__init__(*args, **options)
        self.filename = basename(inspect.getfile(self.__class__))
        self.e = None

    def retry_log(self, retry_state):
        logger.warning(f"{self.filename} attempt {retry_state.attempt_number} failed")

    def handle(self, *args, **options):
        # Use a lockfile to prevent overruns.
        logger.info(f"Executing {self.filename}")
        lockfile = f"/tmp/{self.filename}.lock"
        lock = FileLock(lockfile)
        lock.acquire()
        try:
            for attempt in Retrying(
                after=self.retry_log,
                reraise=True,
                stop=stop_after_attempt(5),
                wait=wait_exponential(multiplier=1, min=60, max=300),
            ):
                with attempt:
                    self._handle(*args, **options)
        except Exception as e:
            logger.warning(f"Retries exhausted for {self.filename}")
            logger.error(e)
            self.e = e
        lock.release()
        remove(lockfile)
        if self.e is not None:
            raise self.e
