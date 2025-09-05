from os import getenv
from django.contrib import admin
from django.urls import include, path
from django.conf import settings
from django.views.generic import TemplateView

from extlinks.healthcheck.urls import urlpatterns as healthcheck_urls
from extlinks.programs.urls import urlpatterns as programs_urls
from extlinks.organisations.urls import urlpatterns as organisations_urls

from .views import Homepage, Documentation

handler500 = "extlinks.views.custom_server_error"

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", Homepage.as_view(), name="homepage"),
    path("docs", Documentation.as_view(), name="documentation"),
    path(
        "healthcheck/",
        include((healthcheck_urls, "healthcheck"), namespace="healthcheck"),
    ),
    path("programs/", include((programs_urls, "programs"), namespace="programs")),
    path(
        "organisations/",
        include((organisations_urls, "organisations"), namespace="organisations"),
    ),
]

reqs = getenv("REQUIREMENTS_FILE", "django.txt")
if settings.DEBUG and reqs == "local.txt":
    if not settings.TESTING:
        import debug_toolbar

        urlpatterns += [
            path("__debug__/", include(debug_toolbar.urls)),
            path("400/", TemplateView.as_view(template_name="400.html")),
            path("403/", TemplateView.as_view(template_name="403.html")),
            path("404/", TemplateView.as_view(template_name="404.html")),
            path("500/", TemplateView.as_view(template_name="500/500.html")),
        ]
