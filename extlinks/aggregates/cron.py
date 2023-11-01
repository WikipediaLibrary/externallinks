from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class LinkAggregatesCron(CronJobBase):
    # Will run daily at midnight UTC
    schedule = Schedule(run_at_times=["00:00"])
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    code = "aggregates.link_aggregates_cron"

    def do(self):
        call_command("fill_link_aggregates")


class UserAggregatesCron(CronJobBase):
    # Will run daily at 00:05 UTC
    schedule = Schedule(run_at_times=["00:05"])
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    code = "aggregates.user_aggregates_cron"

    def do(self):
        call_command("fill_user_aggregates")


class PageProjectAggregatesCron(CronJobBase):
    # Will run daily at 00:45 UTC
    schedule = Schedule(run_at_times=["00:45"])
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    code = "aggregates.pageproject_aggregates_cron"

    def do(self):
        call_command("fill_pageproject_aggregates")
