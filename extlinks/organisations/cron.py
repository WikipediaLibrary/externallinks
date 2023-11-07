from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class UserListsCron(CronJobBase):
    schedule = Schedule(run_every_mins=60)
    RETRY_AFTER_FAILURE_MINS = 10
    MIN_NUM_FAILURES = 3
    code = "organisations.user_lists_cron"

    def do(self):
        call_command("users_update_lists")
