from django.conf.urls import url
from django.urls import path

from extlinks.common.views import CSVOrgTotals
from extlinks.common.urls import urlpatterns as shared_urls
from .views import (
    ProgramListView,
    ProgramDetailView,
    get_editor_count,
    get_project_count,
    get_links_count,
)

urlpatterns = [
    path("", ProgramListView.as_view(), name="list"),
    path("<int:pk>", ProgramDetailView.as_view(), name="detail"),
    url(r"^editor_count/$", get_editor_count, name="editor_count"),
    url(r"^project_count/$", get_project_count, name="project_count"),
    url(r"^links_count/$", get_links_count, name="links_count"),
    # CSV downloads
    path("<int:pk>/csv/org_totals", CSVOrgTotals.as_view(), name="csv_org_totals"),
]

urlpatterns += shared_urls
