from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class MyCronJob(CronJobBase):
    schedule = Schedule(run_every_mins=60)
    code = 'organisations.user_lists_cron'

    def do(self):
        call_command('users_update_lists')
