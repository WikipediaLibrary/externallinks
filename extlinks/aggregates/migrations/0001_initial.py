# Generated by Django 3.1.3 on 2020-11-10 00:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("organisations", "0006_auto_20190730_1355"),
    ]

    operations = [
        migrations.CreateModel(
            name="LinkAggregate",
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
                ("day", models.PositiveIntegerField()),
                ("month", models.PositiveIntegerField()),
                ("year", models.PositiveIntegerField()),
                ("full_date", models.DateField()),
                ("total_links_added", models.PositiveIntegerField()),
                ("total_links_removed", models.PositiveIntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "collection",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="organisations.collection",
                    ),
                ),
                (
                    "organisation",
                    models.ForeignKey(
                        default=None,
                        on_delete=django.db.models.deletion.CASCADE,
                        to="organisations.organisation",
                    ),
                ),
            ],
        ),
    ]
