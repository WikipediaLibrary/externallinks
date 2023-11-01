from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class TotalLinksCron(CronJobBase):
    # 10080 is weekly.
    schedule = Schedule(run_every_mins=10080)
    RETRY_AFTER_FAILURE_MINS = 300
    MIN_NUM_FAILURES = 3
    code = "links.total_links_cron"

    def do(self):
        call_command("linksearchtotal_collect")
