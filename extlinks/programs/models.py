from django.db import models

from extlinks.organisations.models import Organisation


class Program(models.Model):
    class Meta:
        app_label = "programs"
        ordering = ['name']

    name = models.CharField(max_length=40)

    description = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.name

    @property
    def get_org_count(self):
        return Organisation.objects.filter(
            program=self
        ).count()

    @property
    def any_orgs_user_list(self):
        """
        Returns True if any of this program's organisations limit by user
        """
        program_orgs = Organisation.objects.filter(
            program=self)

        for org in program_orgs:
            if org.limit_by_user:
                return True

        return False
