from django.contrib import admin
from django.contrib.contenttypes.admin import GenericTabularInline

from .models import URLPattern, LinkSearchTotal, LinkEvent


class LinkEventURLPatternAdminInline(GenericTabularInline):
    model = LinkEvent
    show_change_link = True
    exclude = ["user_id", "url"]
    readonly_fields = [
        "link",
        "timestamp",
        "domain",
        "rev_id",
        "page_title",
        "page_namespace",
        "event_id",
        "user_is_bot",
        "hash_link_event_id",
        "change",
        "username",
        "on_user_list",
    ]

    def get_queryset(self, request):
        qs = super().get_queryset(request)

        return qs.select_related("username")


class URLPatternAdmin(admin.ModelAdmin):
    list_display = ("url",)
    exclude = ["collections"]
    autocomplete_fields = ["collection"]
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
    readonly_fields = ["url_pattern_display", "username"]
    exclude = ["content_type", "object_id", "url"]

    @admin.display(description="URLPattern")
    def url_pattern_display(self, instance):
        return URLPattern.objects.filter(pk=instance.content_object.pk).first()


admin.site.register(LinkEvent, LinkEventAdmin)
