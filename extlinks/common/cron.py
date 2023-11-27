from subprocess import check_output
from django_cron import CronJobBase, Schedule

class BackupCron(CronJobBase):
    RETRY_AFTER_FAILURE_MINS = 120
    MIN_NUM_FAILURES = 2
    # 2880 is every other day.
    schedule = Schedule(
        run_every_mins=2880, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS
    )
    code = "common.backup"

    def do(self):
        # Using check_output here because we want to log STDOUT.
        # To avoid logging for commands with sensitive output, import and use
        # subprocess.call instead of subprocess.check_output.
        return check_output("/app/bin/backup.sh", text=True)
