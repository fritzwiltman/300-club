from django.urls import path
from .views import (
    player_list,
    hitter_leaderboard,
    homerun_leaderboard,
    ops_leaderboard,
    pitcher_leaderboard,
    rbi_champion_leaderboard,
    stolen_base_leaderboard,
    dimaggio_leaderboard,
    user_list,
    category_list,
)

urlpatterns = [
    # Existing endpoints
    path('players/', player_list, name='player_list'),
    path('batters/', hitter_leaderboard, name='hitter_leaderboard'),
    path('homeruns/', homerun_leaderboard, name='homerun_leaderboard'),

    # New leaderboard endpoints
    path('ops/', ops_leaderboard, name='ops_leaderboard'),
    path('pitchers/', pitcher_leaderboard, name='pitcher_leaderboard'),
    path('rbi-champion/', rbi_champion_leaderboard, name='rbi_champion_leaderboard'),
    path('stolen-bases/', stolen_base_leaderboard, name='stolen_base_leaderboard'),
    path('dimaggio/', dimaggio_leaderboard, name='dimaggio_leaderboard'),

    # Supporting endpoints
    path('users/', user_list, name='user_list'),
    path('categories/', category_list, name='category_list'),
]