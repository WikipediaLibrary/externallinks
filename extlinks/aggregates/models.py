from django.db import models
from django.core.exceptions import ValidationError

from extlinks.organisations.models import Collection, Organisation, User
from extlinks.programs.models import Program


class LinkAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
            models.Index(
                fields=[
                    "organisation_id",
                    "collection_id",
                    "on_user_list",
                    "year",
                    "month",
                ]
            ),
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
    on_user_list = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        if self.day is None or self.day != 0:
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
            on_user_list=self.on_user_list,
            day=self.day,  # day can be 0 if the aggregation is a monthly one
        ).exists():
            raise ValidationError(
                message="LinkAggregate with this combination (organisation, collection, full_date, on_user_list) already exists.",
                code="unique_together",
            )


class UserAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
            models.Index(
                fields=[
                    "organisation_id",
                    "collection_id",
                    "username",
                    "on_user_list",
                    "year",
                    "month",
                ]
            ),
            models.Index(fields=["organisation", "username"]),
            models.Index(fields=["collection", "username"]),
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
    on_user_list = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        if self.day is None or self.day != 0:
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
            on_user_list=self.on_user_list,
            day=self.day,  # day can be 0 if the aggregation is a monthly one
        ).exists():
            raise ValidationError(
                message="UserAggregate with this combination (organisation, collection, username, full_date, on_user_list) already exists.",
                code="unique_together",
            )


class PageProjectAggregate(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["full_date"]),
            models.Index(fields=["collection"]),
            models.Index(fields=["organisation"]),
            models.Index(
                fields=["full_date", "collection_id", "project_name", "page_name"]
            ),
            models.Index(
                fields=[
                    "organisation_id",
                    "collection_id",
                    "project_name",
                    "page_name",
                    "on_user_list",
                    "year",
                    "month",
                ]
            ),
            models.Index(fields=["organisation", "project_name"]),
            models.Index(fields=["collection", "project_name", "page_name"]),
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
    on_user_list = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        if self.day is None or self.day != 0:
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
            on_user_list=self.on_user_list,
            day=self.day,  # day can be 0 if the aggregation is a monthly one
        ).exists():
            raise ValidationError(
                message="PageProjectAggregate with this combination (organisation, collection, project_name, page_name, full_date, on_user_list) already exists.",
                code="unique_together",
            )


class ProgramTopOrganisationsTotal(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["program_id", "full_date", "organisation_id"]),
            models.Index(fields=["program_id", "organisation_id"]),
        ]

    program = models.ForeignKey(Program, on_delete=models.CASCADE)
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    full_date = models.DateField()
    on_user_list = models.BooleanField(default=False)
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class ProgramTopProjectsTotal(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["program_id", "full_date", "project_name"]),
            models.Index(fields=["program_id", "project_name"]),
        ]

    program = models.ForeignKey(Program, on_delete=models.CASCADE)
    project_name = models.CharField(max_length=32)
    full_date = models.DateField()
    on_user_list = models.BooleanField(default=False)
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class ProgramTopUsersTotal(models.Model):
    class Meta:
        app_label = "aggregates"
        indexes = [
            models.Index(fields=["program_id", "full_date", "username"]),
            models.Index(fields=["program_id", "username"]),
        ]

    program = models.ForeignKey(Program, on_delete=models.CASCADE)
    username = models.CharField(max_length=235)
    full_date = models.DateField()
    on_user_list = models.BooleanField(default=False)
    total_links_added = models.PositiveIntegerField()
    total_links_removed = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
