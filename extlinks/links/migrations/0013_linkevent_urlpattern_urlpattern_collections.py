# Generated by Django 4.2.14 on 2024-08-20 16:33

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("organisations", "0008_alter_collection_id_alter_organisation_id_and_more"),
        ("links", "0012_alter_linkevent_id_alter_linksearchtotal_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="linkevent",
            name="urlpattern",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="linkevents",
                to="links.urlpattern",
            ),
        ),
        migrations.AddField(
            model_name="urlpattern",
            name="collections",
            field=models.ManyToManyField(
                related_name="urlpatterns", to="organisations.collection"
            ),
        ),
    ]