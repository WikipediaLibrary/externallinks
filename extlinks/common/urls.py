from django.urls import path

from extlinks.common.views import (
    CSVProjectTotals,
    CSVUserTotals,
    CSVAllLinkEvents,
)

# Shared URL paths. These get namespaced by each app's urlpatterns.
urlpatterns = [
    path(
        "<int:pk>/csv/project_totals",
        CSVProjectTotals.as_view(),
        name="csv_project_totals",
    ),
    path("<int:pk>/csv/user_totals", CSVUserTotals.as_view(), name="csv_user_totals"),
    path("<int:pk>/csv/all_links", CSVAllLinkEvents.as_view(), name="csv_all_links"),
]
