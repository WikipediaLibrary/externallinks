from django.contrib import admin
from django.urls import include, path

from extlinks.programs.urls import urlpatterns as programs_urls

from .views import Homepage

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', Homepage.as_view(), name='homepage'),

    path('programs', include((programs_urls, 'programs'),
                             namespace='programs'))
]
