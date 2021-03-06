# Generated by Django 2.2 on 2019-06-03 11:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("links", "0003_auto_20190530_1045"),
    ]

    operations = [
        migrations.AlterField(
            model_name="linkevent",
            name="user_id",
            field=models.PositiveIntegerField(null=True),
        ),
        migrations.AddConstraint(
            model_name="linksearchtotal",
            constraint=models.UniqueConstraint(
                fields=("url", "date"), name="unique_date_total"
            ),
        ),
    ]
