from django.db import models


class Program(models.Model):
    class Meta:
        app_label = "programs"

    name = models.CharField(max_length=40)


class Organisation(models.Model):
    class Meta:
        app_label = "programs"

    name = models.CharField(max_length=40)

    program = models.ForeignKey(Program, blank=True, null=True,
                                on_delete=models.SET_NULL)


class Collection(models.Model):
    class Meta:
        app_label = "programs"

    name = models.CharField(max_length=40)

    organisation = models.ForeignKey(Organisation, null=True,
                                     on_delete=models.SET_NULL)
