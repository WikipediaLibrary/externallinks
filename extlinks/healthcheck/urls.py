from django.urls import path

from .views import (
    AggregatesCronHealthCheckView,
    CommonCronHealthCheckView,
    LinksCronHealthCheckView,
    OrganizationsCronHealthCheckView,
    LinkEventHealthCheckView,
    MonthlyAggregatesCronHealthCheckView,
)

urlpatterns = [
    path("link_event", LinkEventHealthCheckView.as_view(), name="link_event"),
    path("agg_crons", AggregatesCronHealthCheckView.as_view(), name="agg_crons"),
    path("common_crons", CommonCronHealthCheckView.as_view(), name="common_crons"),
    path("link_crons", LinksCronHealthCheckView.as_view(), name="link_crons"),
    path("org_crons", OrganizationsCronHealthCheckView.as_view(), name="org_crons"),
    path("month_agg_crons", MonthlyAggregatesCronHealthCheckView.as_view(), name="month_agg_crons"),
]
