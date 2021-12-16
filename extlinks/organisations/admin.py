from django.contrib import admin

from .models import Organisation, Collection, User


class UserAdmin(admin.ModelAdmin):
    list_display = ("username",)


admin.site.register(User, UserAdmin)


class OrganisationAdmin(admin.ModelAdmin):
    list_display = ("name",)
    list_filter = ("name",)
    exclude = ("username_list",)


admin.site.register(Organisation, OrganisationAdmin)


class CollectionAdmin(admin.ModelAdmin):
    list_display = ("name", "organisation")
    list_filter = ("name", "organisation")


admin.site.register(Collection, CollectionAdmin)
