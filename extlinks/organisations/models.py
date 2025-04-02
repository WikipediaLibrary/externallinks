from django.contrib.contenttypes.models import ContentType
from django.db import models

from extlinks.links.models import LinkEvent, URLPattern


class User(models.Model):
    class Meta:
        app_label = "organisations"

    username = models.CharField(max_length=235, unique=True)

    def __str__(self):
        return self.username


class Organisation(models.Model):
    class Meta:
        app_label = "organisations"
        ordering = ["name"]

    name = models.CharField(max_length=40)

    # programs.Program syntax required to avoid circular import.
    program = models.ManyToManyField("programs.Program", blank=True)

    username_list = models.ManyToManyField(User, blank=True)
    # If a URL is placed here, we'll use it to regularly update username_list
    username_list_url = models.URLField(blank=True, null=True)
    username_list_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    @property
    def limit_by_user(self):
        return self.username_list.exists()


class Collection(models.Model):
    class Meta:
        app_label = "organisations"
        ordering = ["name"]

    name = models.CharField(max_length=40)

    organisation = models.ForeignKey(Organisation, null=True, on_delete=models.SET_NULL)

    def __str__(self):
        return self.name

    def get_linkevents(self):
        url_patterns = URLPattern.objects.filter(collections__name__contains=self.name)
        url_pattern_type = ContentType.objects.get_for_model(URLPattern)
        return LinkEvent.objects.filter(
            content_type__pk=url_pattern_type.id, object_id__in=url_patterns
        )

    def get_url_patterns(self):
        return URLPattern.objects.filter(collections__name__contains=self.name)
