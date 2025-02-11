from django.urls import path
from .views import player_list, hitter_leaderboard, homerun_leaderboard

urlpatterns = [
    path('players/', player_list, name='player_list'),
    path('batters/', hitter_leaderboard, name='hitter_leaderboard'),
    path('homeruns/', homerun_leaderboard, name='homerun_leaderboard'),
]