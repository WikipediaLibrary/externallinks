from django.contrib import admin

from .models import Program


class ProgramAdmin(admin.ModelAdmin):
    list_display = ("name",)


admin.site.register(Program, ProgramAdmin)
