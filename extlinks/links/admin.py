from django.contrib import admin

from .models import URLPattern, LinkSearchTotal, LinkEvent


class URLPatternAdmin(admin.ModelAdmin):
    list_display = ('url', 'collection')


admin.site.register(URLPattern, URLPatternAdmin)


class LinkSearchTotalAdmin(admin.ModelAdmin):
    list_display = ('url', 'date', 'total')


admin.site.register(LinkSearchTotal, LinkSearchTotalAdmin)


class LinkEventAdmin(admin.ModelAdmin):
    list_display = ('link', 'timestamp', 'domain', 'username', 'change')


admin.site.register(LinkEvent, LinkEventAdmin)
