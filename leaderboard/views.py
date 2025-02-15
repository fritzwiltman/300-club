from datetime import date, timedelta
from django.http import JsonResponse
from django.shortcuts import render

# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Player, Pick, Hitter, CustomUser
from leaderboard.serializers import HitterLeaderboardSerializer, HomerunLeaderboardSerializer, PlayerSerializer

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
        # Get user's picks
        user_regular_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=1, is_alternate=False)
        user_alternate_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=2, is_alternate=True)
        
        # Get qualified and disqualified picks
        qualified_picks = []
        disqualified_picks = []

        for pick in user_regular_picks:
            player = Player.objects.get(player_name=pick.player_name)
            hitter = Hitter.objects.filter(player_id=player.id).first()
            
            if hitter and hitter.plate_appearances >= min_plate_appearances:
                qualified_picks.append({
                    "player_name": pick.player_name,
                    "average": hitter.average,
                    "ops": hitter.ops
                })
            else:
                disqualified_picks.append({
                    "player_name": pick.player_name,
                    "plate_appearances": hitter.plate_appearances if hitter else 0
                })

        # Handle alternates
        qualified_alternates = []
        for pick in user_alternate_picks:
            player = Player.objects.get(player_name=pick.player_name)
            hitter = Hitter.objects.filter(player_id=player.id).first()
            
            if hitter and hitter.plate_appearances >= min_plate_appearances:
                qualified_alternates.append({
                    "player_name": pick.player_name,
                    "average": hitter.average,
                    "ops": hitter.ops
                })
            else:
                disqualified_picks.append({
                    "player_name": pick.player_name,
                    "plate_appearances": hitter.plate_appearances if hitter else 0
                })

        # Replace unqualified regulars with qualified alternates
        final_qualified_picks = qualified_picks + qualified_alternates[:10-len(qualified_picks)]

        # Determine if user is disqualified
        is_disqualified = len(final_qualified_picks) < 10
        aggregate_average = None
        alternate_average = None
        aggregate_ops = None

        if not is_disqualified:
            aggregate_average = sum(player["average"] for player in final_qualified_picks) / 10
            alternate_average = (
                sum(player["average"] for player in qualified_alternates) / len(qualified_alternates)
                if qualified_alternates else 0
            )
            aggregate_ops = sum(player["ops"] for player in final_qualified_picks) / 10

        # Create and add user entry
        user_entry = {
            "user_name": user.name,
            "aggregate_average": round(aggregate_average, 4) if aggregate_average else None,
            "alternate_average": round(alternate_average, 4) if alternate_average else None,
            "aggregate_ops": round(aggregate_ops, 4) if aggregate_ops else None,
            "qualified_picks": final_qualified_picks,  # Includes qualified alternates
            "disqualified_picks": disqualified_picks,
            "rank": 0 if is_disqualified else None  # Will be assigned below
        }
        leaderboard.append(user_entry)
        
    # Sort and rank users properly
    ranked_users = [entry for entry in leaderboard if entry["rank"] != 0]
    
    ranked_users.sort(
        key=lambda pick: (
            -pick["aggregate_average"] if pick["aggregate_average"] is not None else float('-inf'),
            -pick["alternate_average"] if pick["alternate_average"] is not None else float('-inf'),
            -pick["aggregate_ops"] if pick["aggregate_ops"] is not None else float('-inf')
        )
    )

    # Assign rank
    for rank, entry in enumerate(ranked_users, start=1):
        entry["rank"] = rank

    final_response = ranked_users + [entry for entry in leaderboard if entry["rank"] == 0]

    try:
        serializer = HitterLeaderboardSerializer(final_response, many=True)
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def homerun_leaderboard(request):
    """
    Calculate and return home run leaderboard rankings.
    """
    leaderboard = []
    min_plate_appearances = calculate_pro_rated_plate_appearances()

    for user in CustomUser.objects.all():
        # Debugging: Print alternate picks per user
        print(f"User {user.name} Alternate Picks Before Processing:")
        
        user_alternate_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=2, is_alternate=True)
        
        for pick in user_alternate_picks:
            player = Player.objects.get(player_name=pick.player_name)
            hitter = Hitter.objects.filter(player_id=player.id).first()
            
            print(f"  Player: {pick.player_name}, Average: {hitter.average if hitter else 'N/A'}, Plate Appearances: {hitter.plate_appearances if hitter else 'N/A'}")

        # Get user's picks
        user_homerun_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=4)
        all_homerun_picks = []

        for pick in user_homerun_picks:
            player = Player.objects.get(player_name=pick.player_name)
            hitter = Hitter.objects.filter(player_id=player.id).first()

            if hitter:
                all_homerun_picks.append({
                    "player_name": pick.player_name,
                    "home_runs": hitter.home_runs
                })
        
        # Of 4 picks per user, sort by home runs and take the top 3
        all_homerun_picks.sort(key=lambda pick: -pick["home_runs"])
        top_three_picks = all_homerun_picks[:3]
        tiebreaker_pick = all_homerun_picks[3] if len(all_homerun_picks) > 3 else None

        top_three_total_homeruns = sum(pick["home_runs"] for pick in top_three_picks)
        first_tiebreaker_homeruns = tiebreaker_pick["home_runs"] if tiebreaker_pick else None

        # 2nd tiebreaker, which is average of qualified alternate batters
        user_alternate_picks = Pick.objects.filter(user_id=user.mbr_id, category_id=2, is_alternate=True)
        qualified_alternates = [
            Player.objects.get(player_name=pick.player_name)
            for pick in user_alternate_picks
        ]
        qualified_alternates_average = (
            sum(Hitter.objects.get(player_id=player.id).average for player in qualified_alternates) /
            len(qualified_alternates) if qualified_alternates else 0
        )

        alternate_batters_picks = [
            {
                "player_name": pick.player_name,
                "average": round(Hitter.objects.get(player_id=pick.id).average, 4),
                "is_disqualified": Hitter.objects.get(player_id=pick.id).plate_appearances < min_plate_appearances
            }
            for pick in qualified_alternates
        ]

        # Append to leaderboard
        leaderboard.append({
            "user_name": user.name,
            "top_three_total_homeruns": top_three_total_homeruns,
            "first_tiebreaker_homeruns": first_tiebreaker_homeruns,
            "second_tiebreaker_average": round(qualified_alternates_average, 4),
            "rank": None,  # Will be assigned later
            "all_homerun_picks": all_homerun_picks,
            "alternate_batters_picks": alternate_batters_picks
        })
    
    # Sort and rank users by total home runs, then tiebreakers
    leaderboard.sort(
        key=lambda user: (
            -user["top_three_total_homeruns"],
            -user["first_tiebreaker_homeruns"] if user["first_tiebreaker_homeruns"] is not None else float('-inf'),
            -user["second_tiebreaker_average"]
        )
    )

    # Assign ranks
    for rank, entry in enumerate(leaderboard, start=1):
        entry["rank"] = rank

    # Serialize and return response
    try:
        serializer = HomerunLeaderboardSerializer(leaderboard, many=True)
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        print(str(e))  # Logs the full error traceback
        return JsonResponse({'error': str(e)}, status=500)
    
