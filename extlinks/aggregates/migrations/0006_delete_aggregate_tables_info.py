from django.db import migrations


def delete_information_aggregate_tables(apps, schema_editor):
    LinkAggregate = apps.get_model("aggregates", "LinkAggregate")
    PageProjectAggregate = apps.get_model("aggregates", "PageProjectAggregate")
    UserAggregate = apps.get_model("aggregates", "UserAggregate")

    LinkAggregate.objects.all().delete()
    PageProjectAggregate.objects.all().delete()
    UserAggregate.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [("aggregates", "0005_add_organisation_index")]

    operations = [migrations.RunPython(delete_information_aggregate_tables)]
