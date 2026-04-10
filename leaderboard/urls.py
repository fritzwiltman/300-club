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
    # MLB Leaders endpoints
    batters_mlb_leaders,
    ops_mlb_leaders,
    homeruns_mlb_leaders,
    pitchers_mlb_leaders,
    rbi_mlb_leaders,
    stolen_bases_mlb_leaders,
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

    # MLB League Leaders endpoints
    path('batters/mlb-leaders/', batters_mlb_leaders, name='batters_mlb_leaders'),
    path('ops/mlb-leaders/', ops_mlb_leaders, name='ops_mlb_leaders'),
    path('homeruns/mlb-leaders/', homeruns_mlb_leaders, name='homeruns_mlb_leaders'),
    path('pitchers/mlb-leaders/', pitchers_mlb_leaders, name='pitchers_mlb_leaders'),
    path('rbi-champion/mlb-leaders/', rbi_mlb_leaders, name='rbi_mlb_leaders'),
    path('stolen-bases/mlb-leaders/', stolen_bases_mlb_leaders, name='stolen_bases_mlb_leaders'),

    # Supporting endpoints
    path('users/', user_list, name='user_list'),
    path('categories/', category_list, name='category_list'),
]