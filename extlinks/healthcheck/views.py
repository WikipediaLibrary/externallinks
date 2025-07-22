import datetime
import os
import glob

from datetime import timedelta

from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.utils.timezone import now
from django.views.decorators.cache import cache_page

from extlinks.aggregates.models import (
    LinkAggregate,
    UserAggregate,
    PageProjectAggregate,
)
from extlinks.links.models import LinkEvent, LinkSearchTotal
from extlinks.organisations.models import Organisation


def get_most_recent(aggregate, monthly=False) -> datetime.date | None:
    try:
        if monthly:
            return aggregate.objects.filter(day=0).latest("full_date").full_date
        else:
            return aggregate.objects.exclude(day=0).latest("full_date").full_date
    except aggregate.DoesNotExist:
        pass


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
            latest_link_aggregates_cron_endtime = get_most_recent(LinkAggregate)
            latest_user_aggregates_cron_endtime = get_most_recent(UserAggregate)
            latest_pageproject_aggregates_cron_endtime = get_most_recent(
                PageProjectAggregate
            )

            cutoff_datetime = (now() - timedelta(days=2)).date()
            if latest_link_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_user_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_pageproject_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except:
            status_code = 404
            status_msg = "not found"

        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class MonthlyAggregatesCronHealthCheckView(View):
    """
    Healthcheck that passes only if the monthly aggregate jobs have all run successfully in the last month
    """

    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "error"
        try:
            latest_link_aggregates_cron_endtime = get_most_recent(LinkAggregate, True)
            latest_user_aggregates_cron_endtime = get_most_recent(UserAggregate, True)
            latest_pageproject_aggregates_cron_endtime = get_most_recent(
                PageProjectAggregate, True
            )
            # Monthly jobs may take some time to run, let's give 35 days to make sure
            cutoff_datetime = (now() - timedelta(days=35)).date()
            if latest_link_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_user_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            elif latest_pageproject_aggregates_cron_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response


@method_decorator(cache_page(60 * 1), name="dispatch")
class CommonCronHealthCheckView(View):
    """
    Healthcheck that passes only if a backup file has been created in the last 3 days
    """

    def get(self, request, *args, **kwargs):
        status_code = 500
        status_msg = "out of date"

        for i in range(3):
            date = now() - timedelta(days=i)
            filename = "links_linkevent_{}_*.json.gz".format(date.strftime("%Y%m%d"))
            filepath = os.path.join(os.environ["HOST_BACKUP_DIR"], filename)

            if bool(glob.glob(filepath)):
                status_code = 200
                status_msg = "ok"
                break

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
            latest_total_links_endtime = LinkSearchTotal.objects.latest("date").date
            cutoff_datetime = now().date() - timedelta(days=9)
            if latest_total_links_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except:
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
            latest_user_lists_endtime = Organisation.objects.latest(
                "username_list_updated"
            ).username_list_updated
            cutoff_datetime = now() - timedelta(hours=2)
            if latest_user_lists_endtime < cutoff_datetime:
                status_msg = "out of date"
            else:
                status_code = 200
                status_msg = "ok"
        except:
            status_code = 404
            status_msg = "not found"
        response = JsonResponse({"status": status_msg})
        response.status_code = status_code
        return response
