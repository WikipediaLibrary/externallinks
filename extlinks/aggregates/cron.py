import datetime

from dateutil.relativedelta import relativedelta
from subprocess import check_output

from django_cron import CronJobBase, Schedule


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
    # Will run every 24 hours at 03:00
    schedule = Schedule(
        run_at_times=["03:00"],
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
    # Will run every 24 hours at 03:10
    schedule = Schedule(
        run_at_times=["03:10"],
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
    # Will run every 24 hours at 03:50
    schedule = Schedule(
        run_at_times=["03:50"],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.monthly_pageproject_aggregates_cron"

    def do(self):
        return check_output(
            ["python", "manage.py", "fill_monthly_pageproject_aggregates"], text=True
        )


class ProgramTopOrganisationsTotalsCron(CronJobBase):
    """
    Calculates top organisations totals for all programs from LinkAggregate data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.ProgramTopOrganisationsTotalsCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run every 24 hours at 05:00
    schedule = Schedule(
        run_at_times=["05:00"],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.program_top_organisations_cron"

    def do(self):
        now = datetime.datetime.now(datetime.timezone.utc).date()
        last_month = now - relativedelta(months=1)

        return check_output(
            [
                "python",
                "manage.py",
                "fill_top_organisations_totals",
                "--date",
                f"{last_month.year:04}-{last_month.month:02}",
            ],
            text=True,
        )


class ProgramTopProjectsTotalsCron(CronJobBase):
    """
    Calculates top projects totals for all programs from PageProjectAggregate data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.ProgramTopProjectsTotalsCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run every 24 hours at 05:05
    schedule = Schedule(
        run_at_times=["05:05"],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.program_top_projects_cron"

    def do(self):
        now = datetime.datetime.now(datetime.timezone.utc).date()
        last_month = now - relativedelta(months=1)

        return check_output(
            [
                "python",
                "manage.py",
                "fill_top_projects_totals",
                "--date",
                f"{last_month.year:04}-{last_month.month:02}",
            ],
            text=True,
        )


class ProgramTopUsersTotalsCron(CronJobBase):
    """
    Calculates top users totals for all programs from UserAggregate data.

    To run manually:
        python manage.py runcrons --force extlinks.aggregates.cron.ProgramTopUsersTotalsCron
    """

    RETRY_AFTER_FAILURE_MINS = 360
    MIN_NUM_FAILURES = 5
    # Will run every 24 hours at 05:10
    schedule = Schedule(
        run_at_times=["05:10"],
        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS,
    )
    code = "aggregates.program_top_users_cron"

    def do(self):
        now = datetime.datetime.now(datetime.timezone.utc).date()
        last_month = now - relativedelta(months=1)

        return check_output(
            [
                "python",
                "manage.py",
                "fill_top_users_totals",
                "--date",
                f"{last_month.year:04}-{last_month.month:02}",
            ],
            text=True,
        )
