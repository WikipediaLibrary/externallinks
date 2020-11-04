from django.contrib import admin

from .models import LinkAggregate


class LinkAggregateAdmin(admin.ModelAdmin):
    list_display = (
        "collection",
        "full_date",
        "total_links_added",
        "total_links_removed",
    )


admin.site.register(LinkAggregate, LinkAggregateAdmin)
