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
    mbr_id = models.IntegerField(unique=True, primary_key=True)  # Membership ID (optional)

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
    # Because each row in 'hitters' links to one unique player,
    # we use OneToOneField with primary_key=True.
    player = models.OneToOneField(
        Player,
        on_delete=models.CASCADE,
        primary_key=True,       # Tells Django 'player' is also the PK in 'hitters'.
        db_column='player_id',  # Column name in the 'hitters' table
        to_field='id'           # Matches 'id' in the 'players' table
    )

    average = models.FloatField(db_column='average', null=True)
    ops = models.FloatField(db_column='ops', null=True)
    plate_appearances = models.IntegerField(db_column='plate_appearances', null=True)
    home_runs = models.IntegerField(db_column='home_runs', null=True)
    rbis = models.IntegerField(db_column='rbis', null=True)
    stolen_bases = models.IntegerField(db_column='stolen_bases', null=True)

    class Meta:
        db_table = 'hitters'


    def __str__(self):
        return f'Hitting stats for {self.player.player_name}'
    

class Pitcher(models.Model):
    # One-to-one relationship ensures each Player has a single pitcher record
    player = models.OneToOneField(
        Player,
        on_delete=models.CASCADE,
        primary_key=True,       # 'player' is also the PK in 'pitchers'
        db_column='player_id',  # Column name in the 'pitchers' table
        to_field='id'           # Matches 'id' in the 'players' table
    )

    wins = models.IntegerField(db_column='wins', null=True)
    losses = models.IntegerField(db_column='losses', null=True)
    era = models.FloatField(db_column='era', null=True)
    strikeouts = models.IntegerField(db_column='strikeouts', null=True)

    class Meta:
        db_table = 'pitchers'


    def __str__(self):
        return f'Pitching stats for {self.player.player_name}'
    
    
class Pick(models.Model):
    id = models.AutoField(primary_key=True)

    # user_id -> Foreign key to CustomUser
    # Remember to specify db_column='user_id' so Django knows which column to map.
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

    # player_name -> It's just a string in the picks table, not a foreign key.
    player_name = models.CharField(max_length=101, db_column='player_name')

    # is_alternate -> Boolean field
    is_alternate = models.BooleanField(db_column='is_alternate', null=True, blank=True)

    # pick_order -> The order in which this player was picked, can be null
    pick_order = models.IntegerField(db_column='pick_order', null=True, blank=True)

    # pick_value -> If you’re not sure of the data type, start with IntegerField or FloatField
    pick_value = models.IntegerField(db_column='pick_value', null=True, blank=True)

    class Meta:
        db_table = 'picks'


    def __str__(self):
        return f"{self.user.name} - {self.category.name} - {self.player_name}"