from datetime import timedelta
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.utils.timezone import now
from django.views.decorators.cache import cache_page
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
            cutoff_datetime = now() - timedelta(days=2)
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
