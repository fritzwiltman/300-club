# Generated manually - Add MlbLeader model for tracking MLB league leaders

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('leaderboard', '0003_add_season_fields'),
    ]

    operations = [
        # Create mlb_leaders table
        migrations.RunSQL(
            sql="""
                CREATE TABLE IF NOT EXISTS mlb_leaders (
                    id SERIAL PRIMARY KEY,
                    season INTEGER NOT NULL,
                    category VARCHAR(50) NOT NULL,
                    rank INTEGER NOT NULL,
                    player_name VARCHAR(100) NOT NULL,
                    team VARCHAR(10),
                    value DOUBLE PRECISION NOT NULL,
                    api_player_id INTEGER
                );
            """,
            reverse_sql="DROP TABLE IF EXISTS mlb_leaders;",
        ),

        # Add index on (season, category) for efficient queries
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS mlb_leaders_season_category_idx ON mlb_leaders (season, category);",
            reverse_sql="DROP INDEX IF EXISTS mlb_leaders_season_category_idx;",
        ),
    ]
