from django.contrib import admin
from django.contrib.contenttypes.admin import GenericTabularInline, GenericStackedInline

from .models import URLPattern, LinkSearchTotal, LinkEvent


class LinkEventURLPatternAdminInline(GenericTabularInline):
    model = LinkEvent
    show_change_link = True
    exclude = ['username', 'user_id', 'on_user_list', 'url']


class URLPatternAdmin(admin.ModelAdmin):
    list_display = ("url",)
    list_select_related = ["collection"]
    inlines = [
        LinkEventURLPatternAdminInline,
    ]


admin.site.register(URLPattern, URLPatternAdmin)


class LinkSearchTotalAdmin(admin.ModelAdmin):
    list_display = ("url", "date", "total")


admin.site.register(LinkSearchTotal, LinkSearchTotalAdmin)


class LinkEventAdmin(admin.ModelAdmin):
    list_display = ("link", "timestamp", "domain", "username", "change")
    list_select_related = ["username", "content_type"]
    readonly_fields = ["url_pattern_display"]
    exclude = ["content_type", "object_id"]

    @admin.display(description="URLPattern")
    def url_pattern_display(self, instance):
        return URLPattern.objects.filter(pk=instance.content_object.pk).first()


admin.site.register(LinkEvent, LinkEventAdmin)
