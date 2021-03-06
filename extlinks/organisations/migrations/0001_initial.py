# Generated by Django 2.2 on 2019-05-20 14:01

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("programs", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Organisation",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=40)),
                ("limit_by_user", models.BooleanField(default=False)),
                ("username_list", models.TextField(blank=True, null=True)),
                ("username_list_url", models.URLField(blank=True, null=True)),
                (
                    "program",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="programs.Program",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="Collection",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=40)),
                (
                    "organisation",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to="organisations.Organisation",
                    ),
                ),
            ],
        ),
    ]
