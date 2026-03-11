from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("main", "0002_event_unique_name_alter_event_date"),
    ]

    operations = [
        migrations.AddField(
            model_name="result",
            name="startlist_unique_name",
            field=models.CharField(blank=True, max_length=128, null=True, unique=True),
        ),
    ]
