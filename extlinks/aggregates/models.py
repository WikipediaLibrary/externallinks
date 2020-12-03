from django.db import models
from django.core.exceptions import ValidationError

from extlinks.organisations.models import Collection, Organisation


class LinkAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
        ]

    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    collection = models.ForeignKey(
        Collection, on_delete=models.CASCADE, blank=False, null=False
    )
    day = models.PositiveIntegerField()
    month = models.PositiveIntegerField()
    year = models.PositiveIntegerField()
    full_date = models.DateField()
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.day = self.full_date.day
        self.month = self.full_date.month
        self.year = self.full_date.year
        if self.pk is None:
            self.full_clean(validate_unique=True)
        super().save(*args, **kwargs)

    def validate_unique(self, *args, **kwargs):
        super(LinkAggregate, self).validate_unique(*args, **kwargs)

        if self.__class__.objects.filter(
            organisation=self.organisation,
            collection=self.collection,
            full_date=self.full_date,
        ).exists():
            raise ValidationError(
                message="LinkAggregate with this combination (organisation, collection, full_date) already exists.",
                code="unique_together",
            )


class UserAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
        ]

    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    collection = models.ForeignKey(
        Collection, on_delete=models.CASCADE, blank=False, null=False
    )
    username = models.CharField(max_length=235)
    day = models.PositiveIntegerField()
    month = models.PositiveIntegerField()
    year = models.PositiveIntegerField()
    full_date = models.DateField()
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.day = self.full_date.day
        self.month = self.full_date.month
        self.year = self.full_date.year
        if self.pk is None:
            self.full_clean(validate_unique=True)
        super().save(*args, **kwargs)

    def validate_unique(self, *args, **kwargs):
        super().validate_unique(*args, **kwargs)

        if self.__class__.objects.filter(
            organisation=self.organisation,
            collection=self.collection,
            username=self.username,
            full_date=self.full_date,
        ).exists():
            raise ValidationError(
                message="UserAggregate with this combination (organisation, collection, username, full_date) already exists.",
                code="unique_together",
            )


class PageProjectAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
        ]

    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    collection = models.ForeignKey(
        Collection, on_delete=models.CASCADE, blank=False, null=False
    )
    project_name = models.CharField(max_length=32)
    page_name = models.CharField(max_length=255)
    day = models.PositiveIntegerField()
    month = models.PositiveIntegerField()
    year = models.PositiveIntegerField()
    full_date = models.DateField()
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.day = self.full_date.day
        self.month = self.full_date.month
        self.year = self.full_date.year
        if self.pk is None:
            self.full_clean(validate_unique=True)
        super().save(*args, **kwargs)

    def validate_unique(self, *args, **kwargs):
        super().validate_unique(*args, **kwargs)

        if self.__class__.objects.filter(
            organisation=self.organisation,
            collection=self.collection,
            project_name=self.project_name,
            page_name=self.page_name,
            full_date=self.full_date,
        ).exists():
            raise ValidationError(
                message="PageProjectAggregate with this combination (organisation, collection, project_name, page_name, full_date) already exists.",
                code="unique_together",
            )
