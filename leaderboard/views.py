from datetime import date, timedelta
from django.http import JsonResponse
from django.shortcuts import render

# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Player, Pick, Hitter, CustomUser
from leaderboard.serializers import HitterLeaderboardSerializer, PlayerSerializer

@api_view(['GET'])
def player_list(request):
    players = Player.objects.all()
    serializer = PlayerSerializer(players, many=True)
    return Response(serializer.data, status=200)


def calculate_pro_rated_plate_appearances():
    """Calculate the minimum plate appearances required based on how many weeks have passed since March 18 (for 2025 season)"""
    SEASON_START = date(2024, 3, 18)
    TOTAL_WEEKS = 27
    FULL_SEASON_PA = 502

    today = date.today()

    # If the season hasn't started, return 0 to avoid division errors
    if today < SEASON_START:
        return 0
    
    # Calculate the number of weeks elapsed since SEASON_START
    days_elapsed = (today - SEASON_START).days
    weeks_elapsed = (days_elapsed // 7)
    weeks_elapsed = min(weeks_elapsed, TOTAL_WEEKS)

    return FULL_SEASON_PA * (weeks_elapsed / TOTAL_WEEKS) # Pro-rated plate appearances
    

@api_view(['GET'])
def hitter_leaderboard(request):
    """
    Calculate and return hitter leaderboard rankings.
    """
    leaderboard = []
    min_plate_appearances = calculate_pro_rated_plate_appearances()

    for user in CustomUser.objects.all():
        # Get all users picks for the "batters" category (regular and alternates)
        user_regular_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=1, is_alternate=False)
        user_alternate_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=2, is_alternate=True)

        # Get hitters who qualify
        qualified_regulars = [
            pick for pick in user_regular_picks
            if Hitter.objects.filter(player_id=Player.objects.get(player_name=pick.player_name).id,
                                     plate_appearances__gte=min_plate_appearances)
                                     .exists()
        ]

        qualified_alternates = [
            pick for pick in user_alternate_picks
            if Hitter.objects.filter(player_id=Player.objects.get(player_name=pick.player_name).id,
                                     plate_appearances__gte=min_plate_appearances)
                                     .exists()
        ]

        # Replace unqualified regulars with qualified alternates
        final_picks = qualified_regulars + qualified_alternates[:10-len(qualified_regulars)]

        # Disqualify if fewer than 10 qualified hitters
        if len(final_picks) < 10:
            continue

        # Calculate aggregate average
        aggregate_average = sum(
            Hitter.objects.get(player_id=Player.objects.get(player_name=pick.player_name).id).average
            for pick in final_picks
        ) / 10

        # Calculate alternates' average for tiebreaking
        alternate_average = (
            sum(Hitter.objects.get(player_id=Player.objects.get(player_name=pick.player_name).id).average 
                for pick in qualified_alternates) /
            len(qualified_alternates) if qualified_alternates else 0
        )

        # Add entry to leaderboard
        leaderboard.append({
            "user_name": user.name,
            "aggregate_average": round(aggregate_average, 4),
            "alternate_average": round(alternate_average, 4),
            "qualified_batters": [
                {
                    "player_name": pick.player_name,
                    "average": Hitter.objects.get(player_id=Player.objects.get(player_name=pick.player_name).id).average,
                }
                for pick in final_picks
            ]
        })

    # Sort leaderboard by aggregate average and alternates' average
    leaderboard.sort(key=lambda pick: (-pick["aggregate_average"], -pick["alternate_average"]))
    # Add rank to each entry
    for rank, entry in enumerate(leaderboard, start=1):
        entry["rank"] = rank

    try:
        serializer = HitterLeaderboardSerializer(leaderboard, many=True)
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        print(str(e))  # Logs the full error traceback
        return JsonResponse({'error': str(e)}, status=500)
    

