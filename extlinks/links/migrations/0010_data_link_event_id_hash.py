# Generated by Django 3.1.14 on 2023-02-15 17:15
import hashlib
from django.db import migrations


def add_hash_link_event_id(apps, schema_editor):
    LinkEvent = apps.get_model("links", "LinkEvent")
    unhashed = LinkEvent.objects.filter(hash_link_event_id__exact='')
    for i in range(100):
        if unhashed.count() == 0:
            break
        else:
            for event in unhashed.all()[:100000]:
                link_event_id = event.link + event.event_id
                hash = hashlib.sha256()
                hash.update(link_event_id.encode("utf-8"))
                event.hash_link_event_id = hash.hexdigest()
                event.save(update_fields=(['hash_link_event_id']))


class Migration(migrations.Migration):

    dependencies = [
        ("links", "0009_auto_20230215_1656"),
    ]

    operations = [migrations.RunPython(add_hash_link_event_id)]