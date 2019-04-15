from django.contrib import admin

from .models import Program, Organisation, Collection


class ProgramAdmin(admin.ModelAdmin):
    list_display = ('name',)


admin.site.register(Program, ProgramAdmin)


class OrganisationAdmin(admin.ModelAdmin):
    list_display = ('name', 'program')


admin.site.register(Organisation, OrganisationAdmin)


class CollectionAdmin(admin.ModelAdmin):
    list_display = ('name', 'organisation')


admin.site.register(Collection, CollectionAdmin)