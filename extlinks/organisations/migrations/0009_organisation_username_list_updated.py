# Generated by Django 4.2.20 on 2025-04-02 03:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("organisations", "0008_alter_collection_id_alter_organisation_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="organisation",
            name="username_list_updated",
            field=models.DateTimeField(auto_now=True),
        ),
    ]
