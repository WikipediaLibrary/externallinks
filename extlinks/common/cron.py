from subprocess import check_output
from django_cron import CronJobBase, Schedule


class BackupCron(CronJobBase):
    # 10080 is weekly.
    schedule = Schedule(run_every_mins=10080)
    code = "common.backup"

    def do(self):
        # Using check_output here because we want to log STDOUT.
        # To avoid logging for commands with sensitive output, import and use
        # subprocess.call instead of subprocess.check_output.
        return check_output("/app/bin/backup.sh")
