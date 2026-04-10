# Generated manually for SeasonStats model

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('leaderboard', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='SeasonStats',
            fields=[
                ('year', models.IntegerField(primary_key=True, serialize=False)),
                ('longest_hitting_streak', models.IntegerField(blank=True, null=True)),
                ('streak_holder_name', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'season_stats',
            },
        ),
    ]
