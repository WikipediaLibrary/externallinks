from django.urls import path

from .views import LinkEventHealthCheckView

urlpatterns = [
    path("link_event", LinkEventHealthCheckView.as_view(), name="link_event"),
]
