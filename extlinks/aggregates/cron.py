from django_cron import CronJobBase, Schedule

from subprocess import check_output


class LinkAggregatesCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run daily at midnight UTC
    schedule = Schedule(
        run_at_times=["00:00"], retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "aggregates.link_aggregates_cron"

    def do(self):
        return check_output(["python", "manage.py", "fill_link_aggregates"], text=True)


class MonthlyLinkAggregatesCron(CronJobBase):
    """
    Aggregates daily data from `aggregates_linkaggregate` table into
    monthly data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.MonthlyLinkAggregatesCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run monthly on the 3rd day at 01:00 UTC
    schedule = Schedule(
        run_at_times=["01:00"],
        run_on_days=[10],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.monthly_link_aggregates_cron"

    def do(self):
        return check_output(
            ["python", "manage.py", "fill_monthly_link_aggregates"], text=True
        )


class UserAggregatesCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run daily at 00:05 UTC
    schedule = Schedule(
        run_at_times=["00:05"], retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "aggregates.user_aggregates_cron"

    def do(self):
        return check_output(["python", "manage.py", "fill_user_aggregates"], text=True)


class MonthlyUserAggregatesCron(CronJobBase):
    """
    Aggregates daily data from `aggregates_useraggregate` table into
    monthly data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.MonthlyUserAggregatesCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run monthly on the 3rd day at 01:30 UTC
    schedule = Schedule(
        run_at_times=["01:30"],
        run_on_days=[10],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.monthly_user_aggregates_cron"

    def do(self):
        return check_output(
            ["python", "manage.py", "fill_monthly_user_aggregates"], text=True
        )


class PageProjectAggregatesCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run daily at 00:45 UTC
    schedule = Schedule(
        run_at_times=["00:45"], retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "aggregates.pageproject_aggregates_cron"

    def do(self):
        return check_output(
            ["python", "manage.py", "fill_pageproject_aggregates"], text=True
        )


class MonthlyPageProjectAggregatesCron(CronJobBase):
    """
    Aggregates daily data from `aggregates_pageprojectaggregate` table into
    monthly data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.MonthlyPageProjectAggregatesCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run monthly on the 3rd day at 03:30 UTC
    schedule = Schedule(
        run_at_times=["03:30"],
        run_on_days=[10],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.monthly_pageproject_aggregates_cron"

    def do(self):
        return check_output(
            ["python", "manage.py", "fill_monthly_pageproject_aggregates"], text=True
        )
