from django.contrib import admin

from .models import LinkAggregate


class LinkAggregateAdmin(admin.ModelAdmin):
    pass


admin.site.register(LinkAggregate, LinkAggregateAdmin)
