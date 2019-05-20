from django.urls import path

from extlinks.common.views import CSVPageTotals
from extlinks.common.urls import urlpatterns as shared_urls
from .views import (OrganisationDetailView,
                    OrganisationListView)

urlpatterns = [
    path('', OrganisationListView.as_view(), name='list'),
    path('<int:pk>', OrganisationDetailView.as_view(), name='detail'),

    # CSV downloads
    path('<int:pk>/csv/page_totals', CSVPageTotals.as_view(),
         name='csv_page_totals'),
]

urlpatterns += shared_urls
