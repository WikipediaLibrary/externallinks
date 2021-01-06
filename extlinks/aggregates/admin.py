from django.contrib import admin

from .models import LinkAggregate, UserAggregate, PageProjectAggregate


class LinkAggregateAdmin(admin.ModelAdmin):
    list_display = (
        "organisation",
        "collection",
        "full_date",
        "total_links_added",
        "total_links_removed",
        "on_user_list",
    )
    list_filter = ("organisation", "collection", "month", "year", "on_user_list")


admin.site.register(LinkAggregate, LinkAggregateAdmin)


class UserAggregateAdmin(admin.ModelAdmin):
    list_display = (
        "organisation",
        "collection",
        "username",
        "full_date",
        "total_links_added",
        "total_links_removed",
        "on_user_list",
    )
    list_filter = ("organisation", "collection", "month", "year", "on_user_list")


admin.site.register(UserAggregate, UserAggregateAdmin)


class PageProjectAggregateAdmin(admin.ModelAdmin):
    list_display = (
        "organisation",
        "collection",
        "project_name",
        "page_name",
        "full_date",
        "total_links_added",
        "total_links_removed",
        "on_user_list",
    )
    list_filter = ("organisation", "collection", "month", "year", "on_user_list")


admin.site.register(PageProjectAggregate, PageProjectAggregateAdmin)
