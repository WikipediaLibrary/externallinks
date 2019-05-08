from django.urls import path

from .views import (ProgramListView,
                    ProgramDetailView,
                    OrganisationDetailView,
                    OrganisationListView,
                    CSVOrgTotals,
                    CSVProjectTotals,
                    CSVUserTotals,
                    CSVAllLinkEvents,
                    CSVPageTotals)

urlpatterns = [
    path('', ProgramListView.as_view(), name='list'),
    path('<int:pk>', ProgramDetailView.as_view(), name='detail'),
    path('organisations/<int:pk>', OrganisationDetailView.as_view(),
         name='organisation-detail'),
    path('organisations', OrganisationListView.as_view(),
         name='organisation-list'),

    # CSV downloads
    path('<int:pk>/csv/org_totals', CSVOrgTotals.as_view(),
         name='csv_org_totals'),
    path('<int:pk>/csv/project_totals', CSVProjectTotals.as_view(),
         name='csv_project_totals'),
    path('organisations/<int:pk>/csv/project_totals',
         CSVProjectTotals.as_view(), name='csv_project_totals_org'),
    path('<int:pk>/csv/user_totals', CSVUserTotals.as_view(),
         name='csv_user_totals'),
    path('organisations/<int:pk>/csv/user_totals',
         CSVUserTotals.as_view(), name='csv_user_totals_org'),
    path('<int:pk>/csv/all_links', CSVAllLinkEvents.as_view(),
         name='csv_all_links'),
    path('organisations/<int:pk>/csv/all_links',
         CSVAllLinkEvents.as_view(), name='csv_all_links_org'),
    path('organisations/<int:pk>/csv/page_totals', CSVPageTotals.as_view(),
         name='csv_page_totals')
]
