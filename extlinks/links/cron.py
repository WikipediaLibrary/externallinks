import os

from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class TotalLinksCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 300
    MIN_NUM_FAILURES = 3
    # 10080 is weekly.
    schedule = Schedule(
        run_every_mins=10080, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "links.total_links_cron"

    def do(self):
        call_command("linksearchtotal_collect")


class ArchiveLinksCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 300
    MIN_NUM_FAILURES = 3
    # Will run daily at 02:00 UTC
    schedule = Schedule(
        run_at_times=["02:00"], retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "links.archive_links_cron"

    def do(self):
        use_object_storage_only = (
            os.getenv("LINKEVENTS_ARCHIVE_OBJECT_STORAGE_ONLY", "false").lower()
            == "true"
        )
        command_args = ["linkevents_archive"]
        if use_object_storage_only:
            command_args.append("--object-storage-only")

        call_command(*command_args)
