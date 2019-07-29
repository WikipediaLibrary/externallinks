from django_cron import CronJobBase, Schedule

from django.core.management import call_command


class TotalLinksCron(CronJobBase):
    # 10080 is weekly.
    schedule = Schedule(run_every_mins=10080)
    code = 'links.total_links_cron'

    def do(self):
        call_command('collect_linksearch_data')
