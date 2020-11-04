from django.contrib import admin

from .models import LinkAggregate


class LinkAggregateAdmin(admin.ModelAdmin):
    list_display = (
        "organisation",
        "collection",
        "full_date",
        "total_links_added",
        "total_links_removed",
    )
    list_filter = ("organisation", "collection", "month", "year")


admin.site.register(LinkAggregate, LinkAggregateAdmin)
