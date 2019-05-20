from django.urls import path

from extlinks.common.views import CSVOrgTotals
from extlinks.common.urls import urlpatterns as shared_urls
from .views import (ProgramListView,
                    ProgramDetailView)

urlpatterns = [
    path('', ProgramListView.as_view(), name='list'),
    path('<int:pk>', ProgramDetailView.as_view(), name='detail'),

    # CSV downloads
    path('<int:pk>/csv/org_totals', CSVOrgTotals.as_view(),
         name='csv_org_totals'),
]

urlpatterns += shared_urls
