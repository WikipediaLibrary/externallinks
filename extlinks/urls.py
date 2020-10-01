import os
from django.contrib import admin
from django.urls import include, path
from django.conf import settings

from extlinks.programs.urls import urlpatterns as programs_urls
from extlinks.organisations.urls import urlpatterns as organisations_urls

from .views import Homepage, Documentation

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", Homepage.as_view(), name="homepage"),
    path("docs", Documentation.as_view(), name="documentation"),
    path("programs/", include((programs_urls, "programs"), namespace="programs")),
    path(
        "organisations/",
        include((organisations_urls, "organisations"), namespace="organisations"),
    ),
]

if settings.DEBUG and os.environ["REQUIREMENTS_FILE"] == "local.txt":
    import debug_toolbar

    urlpatterns += [
        path("__debug__/", include(debug_toolbar.urls)),
    ]
