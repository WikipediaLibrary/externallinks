from django.db import migrations


def add_link_events_to_proquest_openurl_collection(apps, schema_editor):
    LinkEvent = apps.get_model("links", "LinkEvent")
    Collection = apps.get_model("organisations", "Collection")
    URLPattern = apps.get_model("links", "URLPattern")
    proquest_openurl_collection = Collection.objects.filter(name="Proquest OpenURL")
    if proquest_openurl_collection:
        proquest_openurl_linkevents = LinkEvent.objects.filter(
            link__icontains="gateway.proquest.com/openurl"
        )
        for proquest_openurl_linkevent in proquest_openurl_linkevents:
            proquest_openurl_linkevent.url.pattern = proquest_openurl_collection[0].url.get()
            proquest_openurl_linkevent.save()


class Migration(migrations.Migration):

    dependencies = [("links", "0007_auto_20190730_1355")]

    operations = [migrations.RunPython(add_link_events_to_proquest_openurl_collection)]
