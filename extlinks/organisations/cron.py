from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class UserListsCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 10
    MIN_NUM_FAILURES = 3
    schedule = Schedule(
        run_every_mins=60, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "organisations.user_lists_cron"

    def do(self):
        call_command("users_update_lists")
