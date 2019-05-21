from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class MyCronJob(CronJobBase):
    RUN_AT_TIMES = ['00:00']

    schedule = Schedule(run_at_times=RUN_AT_TIMES)
    code = 'links.total_links_cron'

    def do(self):
        call_command('collect_linksearch_data')
