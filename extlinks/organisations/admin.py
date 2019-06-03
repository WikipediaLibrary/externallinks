from django.contrib import admin

from .models import Organisation, Collection


class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name',)


admin.site.register(Organisation, OrganisationAdmin)


class CollectionAdmin(admin.ModelAdmin):
    list_display = ('name', 'organisation')


admin.site.register(Collection, CollectionAdmin)
