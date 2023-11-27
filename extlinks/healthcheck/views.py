from datetime import timedelta
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.utils.timezone import now
from django.views.decorators.cache import cache_page
from django_cron.models import CronJobLog
from extlinks.links.models import LinkEvent

@method_decorator(cache_page(60 * 1), name="dispatch")
class LinkEventHealthCheckView(View):
    """
    Healthcheck that passes only if the latest link event is less than a day old
    """
    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_linkevent_datetime = LinkEvent.objects.all().latest().timestamp
            cutoff_datetime = now() - timedelta(days=1)
            if latest_linkevent_datetime > cutoff_datetime:
                status_code = 200
                status_msg = "ok"
            else:
                status_msg = "out of date"
        except LinkEvent.DoesNotExist:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class AggregatesCronHealthCheckView(View):
    """
    Healthcheck that passes only if the link aggregate jobs have all run successfully in the last 2 days
    """
    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_link_aggregates_cron_endtime = CronJobLog.objects.filter(code="aggregates.link_aggregates_cron", is_success=True).latest("end_time").end_time
            latest_user_aggregates_cron_endtime = CronJobLog.objects.filter(code="aggregates.user_aggregates_cron", is_success=True).latest("end_time").end_time
            latest_pageproject_aggregates_cron_endtime = CronJobLog.objects.filter(code="aggregates.pageproject_aggregates_cron", is_success=True).latest("end_time").end_time
            cutoff_datetime = now() - timedelta(days=2)
            if latest_link_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_user_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_pageproject_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except CronJobLog.DoesNotExist:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class CommonCronHealthCheckView(View):
    """
    Healthcheck that passes only if the common jobs have all run successfully in the last 3 days
    """
    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_common_backup_endtime = CronJobLog.objects.filter(code="common.backup", is_success=True).latest("end_time").end_time
            cutoff_datetime = now() - timedelta(days=3)
            if latest_common_backup_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except CronJobLog.DoesNotExist:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class LinksCronHealthCheckView(View):
    """
    Healthcheck that passes only if the links jobs have all run successfully in the last 9 days
    """
    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_total_links_endtime = CronJobLog.objects.filter(code="links.total_links_cron", is_success=True).latest("end_time").end_time
            cutoff_datetime = now() - timedelta(days=9)
            if latest_total_links_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except CronJobLog.DoesNotExist:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class OrganizationsCronHealthCheckView(View):
    """
    Healthcheck that passes only if the Organizations jobs have all run successfully in the last 2 hours
    """
    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_user_lists_endtime = CronJobLog.objects.filter(code="organisations.user_lists_cron", is_success=True).latest("end_time").end_time
            cutoff_datetime = now() - timedelta(hours=2)
            if latest_user_lists_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except CronJobLog.DoesNotExist:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response
