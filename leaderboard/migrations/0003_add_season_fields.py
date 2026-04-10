# Generated manually - Add season fields to Hitter, Pitcher, and Pick models
# This migration restructures Hitter and Pitcher tables to support multiple seasons

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('leaderboard', '0002_add_season_stats'),
    ]

    operations = [
        # =====================================================
        # HITTERS TABLE - Restructure for multi-season support
        # =====================================================

        # Step 1: Add new 'id' column (will become primary key)
        migrations.RunSQL(
            sql="ALTER TABLE hitters ADD COLUMN id SERIAL;",
            reverse_sql="ALTER TABLE hitters DROP COLUMN id;",
        ),

        # Step 2: Add 'season' column with default 2024
        migrations.RunSQL(
            sql="ALTER TABLE hitters ADD COLUMN season INTEGER DEFAULT 2024 NOT NULL;",
            reverse_sql="ALTER TABLE hitters DROP COLUMN season;",
        ),

        # Step 3: Remove old primary key constraint from player_id
        migrations.RunSQL(
            sql="ALTER TABLE hitters DROP CONSTRAINT IF EXISTS hitters_pkey;",
            reverse_sql="ALTER TABLE hitters ADD PRIMARY KEY (player_id);",
        ),

        # Step 4: Add new primary key on 'id'
        migrations.RunSQL(
            sql="ALTER TABLE hitters ADD PRIMARY KEY (id);",
            reverse_sql="ALTER TABLE hitters DROP CONSTRAINT hitters_pkey;",
        ),

        # Step 5: Add unique constraint on (player_id, season)
        migrations.RunSQL(
            sql="ALTER TABLE hitters ADD CONSTRAINT hitters_player_season_unique UNIQUE (player_id, season);",
            reverse_sql="ALTER TABLE hitters DROP CONSTRAINT hitters_player_season_unique;",
        ),

        # Step 6: Add indexes for hitters
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS hitters_season_idx ON hitters (season);",
            reverse_sql="DROP INDEX IF EXISTS hitters_season_idx;",
        ),
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS hitters_player_season_idx ON hitters (player_id, season);",
            reverse_sql="DROP INDEX IF EXISTS hitters_player_season_idx;",
        ),

        # =====================================================
        # PITCHERS TABLE - Restructure for multi-season support
        # =====================================================

        # Step 1: Add new 'id' column (will become primary key)
        migrations.RunSQL(
            sql="ALTER TABLE pitchers ADD COLUMN id SERIAL;",
            reverse_sql="ALTER TABLE pitchers DROP COLUMN id;",
        ),

        # Step 2: Add 'season' column with default 2024
        migrations.RunSQL(
            sql="ALTER TABLE pitchers ADD COLUMN season INTEGER DEFAULT 2024 NOT NULL;",
            reverse_sql="ALTER TABLE pitchers DROP COLUMN season;",
        ),

        # Step 3: Remove old primary key constraint from player_id
        migrations.RunSQL(
            sql="ALTER TABLE pitchers DROP CONSTRAINT IF EXISTS pitchers_pkey;",
            reverse_sql="ALTER TABLE pitchers ADD PRIMARY KEY (player_id);",
        ),

        # Step 4: Add new primary key on 'id'
        migrations.RunSQL(
            sql="ALTER TABLE pitchers ADD PRIMARY KEY (id);",
            reverse_sql="ALTER TABLE pitchers DROP CONSTRAINT pitchers_pkey;",
        ),

        # Step 5: Add unique constraint on (player_id, season)
        migrations.RunSQL(
            sql="ALTER TABLE pitchers ADD CONSTRAINT pitchers_player_season_unique UNIQUE (player_id, season);",
            reverse_sql="ALTER TABLE pitchers DROP CONSTRAINT pitchers_player_season_unique;",
        ),

        # Step 6: Add indexes for pitchers
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS pitchers_season_idx ON pitchers (season);",
            reverse_sql="DROP INDEX IF EXISTS pitchers_season_idx;",
        ),
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS pitchers_player_season_idx ON pitchers (player_id, season);",
            reverse_sql="DROP INDEX IF EXISTS pitchers_player_season_idx;",
        ),

        # =====================================================
        # PICKS TABLE - Add season field
        # =====================================================

        # Add 'season' column with default 2024
        migrations.RunSQL(
            sql="ALTER TABLE picks ADD COLUMN season INTEGER DEFAULT 2024 NOT NULL;",
            reverse_sql="ALTER TABLE picks DROP COLUMN season;",
        ),

        # Add indexes for picks
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS picks_season_idx ON picks (season);",
            reverse_sql="DROP INDEX IF EXISTS picks_season_idx;",
        ),
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS picks_user_season_idx ON picks (user_id, season);",
            reverse_sql="DROP INDEX IF EXISTS picks_user_season_idx;",
        ),
        migrations.RunSQL(
            sql="CREATE INDEX IF NOT EXISTS picks_user_category_season_idx ON picks (user_id, category_id, season);",
            reverse_sql="DROP INDEX IF EXISTS picks_user_category_season_idx;",
        ),
    ]
