from django.db import models

class Category(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)

    class Meta:
        db_table = 'categories'


    def __str__(self):
        return self.name


class CustomUser(models.Model):
    name = models.CharField(max_length=255)  # User's name
    mbr_id = models.IntegerField(unique=True, primary_key=True)  # Membership ID

    class Meta:
        db_table = 'users'  # Maps to your existing users table


    def __str__(self):
        return self.name


class Player(models.Model):
    id = models.AutoField(primary_key=True)
    player_name = models.CharField(max_length=100)
    player_type = models.CharField(max_length=10, choices=[('hitter', 'Hitter'), ('pitcher', 'Pitcher')])
    api_player_id = models.IntegerField(unique=True, null=True)

    class Meta:
        db_table = 'players'


    def __str__(self):
        return self.player_name
    

class Hitter(models.Model):
    id = models.AutoField(primary_key=True)
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        db_column='player_id',
        to_field='id',
        related_name='hitter_seasons'
    )
    season = models.IntegerField(db_column='season', default=2024)

    average = models.FloatField(db_column='average', null=True)
    ops = models.FloatField(db_column='ops', null=True)
    plate_appearances = models.IntegerField(db_column='plate_appearances', null=True)
    home_runs = models.IntegerField(db_column='home_runs', null=True)
    rbis = models.IntegerField(db_column='rbis', null=True)
    stolen_bases = models.IntegerField(db_column='stolen_bases', null=True)

    class Meta:
        db_table = 'hitters'
        unique_together = [['player', 'season']]
        indexes = [
            models.Index(fields=['season']),
            models.Index(fields=['player', 'season']),
        ]

    def __str__(self):
        return f'Hitting stats for {self.player.player_name} ({self.season})'
    

class Pitcher(models.Model):
    id = models.AutoField(primary_key=True)
    player = models.ForeignKey(
        Player,
        on_delete=models.CASCADE,
        db_column='player_id',
        to_field='id',
        related_name='pitcher_seasons'
    )
    season = models.IntegerField(db_column='season', default=2024)

    wins = models.IntegerField(db_column='wins', null=True)
    losses = models.IntegerField(db_column='losses', null=True)
    era = models.FloatField(db_column='era', null=True)
    strikeouts = models.IntegerField(db_column='strikeouts', null=True)

    class Meta:
        db_table = 'pitchers'
        unique_together = [['player', 'season']]
        indexes = [
            models.Index(fields=['season']),
            models.Index(fields=['player', 'season']),
        ]

    def __str__(self):
        return f'Pitching stats for {self.player.player_name} ({self.season})'
    
    
class Pick(models.Model):
    id = models.AutoField(primary_key=True)

    # user_id -> Foreign key to CustomUser
    user = models.ForeignKey(
        CustomUser,
        on_delete=models.CASCADE,
        db_column='user_id',
        null=True
    )

    # category_id -> Foreign key to Category
    category = models.ForeignKey(
        Category,
        on_delete=models.CASCADE,
        db_column='category_id',
        null=True
    )

    # Season year for this pick
    season = models.IntegerField(db_column='season', default=2024)

    # player_name -> It's just a string in the picks table, not a foreign key.
    player_name = models.CharField(max_length=101, db_column='player_name')

    # is_alternate -> Boolean field
    is_alternate = models.BooleanField(db_column='is_alternate', null=True, blank=True)

    # pick_order -> The order in which this player was picked, can be null
    pick_order = models.IntegerField(db_column='pick_order', null=True, blank=True)

    # pick_value -> For prediction categories (RBI, SB, DiMaggio)
    pick_value = models.IntegerField(db_column='pick_value', null=True, blank=True)

    class Meta:
        db_table = 'picks'
        indexes = [
            models.Index(fields=['season']),
            models.Index(fields=['user', 'season']),
            models.Index(fields=['user', 'category', 'season']),
        ]

    def __str__(self):
        return f"{self.user.name} - {self.category.name} - {self.player_name} ({self.season})"


class SeasonStats(models.Model):
    """
    Stores league-wide season statistics for tracking things like
    the longest hitting streak (DiMaggio Prize).
    """
    year = models.IntegerField(primary_key=True)
    longest_hitting_streak = models.IntegerField(null=True, blank=True)
    streak_holder_name = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        db_table = 'season_stats'

    def __str__(self):
        return f"Season Stats {self.year}"