from django.urls import path

from .views import (ProgramListView,
                    ProgramDetailView,
                    OrganisationDetailView,
                    OrganisationListView)

urlpatterns = [
    path('', ProgramListView.as_view(), name='list'),
    path('<int:pk>', ProgramDetailView.as_view(), name='detail'),
    path('organisations/<int:pk>', OrganisationDetailView.as_view(),
         name='organisation-detail'),
    path('organisations', OrganisationListView.as_view(),
         name='organisation-list')
]
