from django.urls import path

from extlinks.common.views import CSVOrgTotals
from extlinks.common.urls import urlpatterns as shared_urls
from .views import (
    ProgramListView,
    ProgramDetailView,
    get_editor_count,
    get_project_count,
    get_links_count,
    get_top_organisations,
    get_top_projects,
    get_top_users,
)

urlpatterns = [
    path("", ProgramListView.as_view(), name="list"),
    path("<int:pk>", ProgramDetailView.as_view(), name="detail"),
    path("editor_count/", get_editor_count, name="editor_count"),
    path("project_count/", get_project_count, name="project_count"),
    path("links_count/", get_links_count, name="links_count"),
    path("top_organisations/", get_top_organisations, name="top_organisations"),
    path("top_projects/", get_top_projects, name="top_projects"),
    path("top_users/", get_top_users, name="top_users"),
    # CSV downloads
    path("<int:pk>/csv/org_totals", CSVOrgTotals.as_view(), name="csv_org_totals"),
]

urlpatterns += shared_urls
