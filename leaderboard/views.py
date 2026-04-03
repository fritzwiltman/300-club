from collections import defaultdict
from datetime import date, timedelta
from django.http import JsonResponse
from django.shortcuts import render

# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Player, Pick, Hitter, Pitcher, CustomUser, Category, SeasonStats
from leaderboard.serializers import (
    HitterLeaderboardSerializer,
    HomerunLeaderboardSerializer,
    PlayerSerializer,
    OpsLeaderboardSerializer,
    PitcherLeaderboardSerializer,
    RbiChampionLeaderboardSerializer,
    StolenBaseLeaderboardSerializer,
    DiMaggioLeaderboardSerializer,
    UserSerializer,
    CategorySerializer,
)


# Default season for API queries
DEFAULT_SEASON = 2025


def get_season_from_request(request):
    """Get season from query parameter, defaulting to DEFAULT_SEASON."""
    season = request.GET.get('season', DEFAULT_SEASON)
    try:
        return int(season)
    except (ValueError, TypeError):
        return DEFAULT_SEASON


def build_hitter_stats_lookup(season):
    """Build a lookup dict of player_name -> hitter stats for a given season."""
    hitters = Hitter.objects.filter(season=season).select_related('player')
    return {h.player.player_name: h for h in hitters}


def build_pitcher_stats_lookup(season):
    """Build a lookup dict of player_name -> pitcher stats for a given season."""
    pitchers = Pitcher.objects.filter(season=season).select_related('player')
    return {p.player.player_name: p for p in pitchers}

@api_view(['GET'])
def player_list(request):
    players = Player.objects.all()
    serializer = PlayerSerializer(players, many=True)
    return Response(serializer.data, status=200)


def calculate_pro_rated_plate_appearances():
    """Calculate the minimum plate appearances required based on how many weeks have passed since March 18 (for 2025 season)"""
    SEASON_START = date(2026, 3, 25)
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
    season = get_season_from_request(request)

    # Prefetch all data to avoid N+1 queries
    users = list(CustomUser.objects.all())
    all_picks = Pick.objects.filter(category_id__in=[1, 2], season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = defaultdict(list)
    for pick in all_picks:
        picks_by_user[pick.user_id].append(pick)

    for user in users:
        user_picks = picks_by_user.get(user.mbr_id, [])
        user_regular_picks = [p for p in user_picks if p.category_id == 1 and not p.is_alternate]
        user_alternate_picks = [p for p in user_picks if p.category_id == 2 and p.is_alternate]

        # Get qualified and disqualified picks
        qualified_picks = []
        disqualified_picks = []

        for pick in user_regular_picks:
            hitter = hitter_stats.get(pick.player_name)

            if hitter and hitter.plate_appearances and hitter.plate_appearances >= min_plate_appearances:
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
            hitter = hitter_stats.get(pick.player_name)

            if hitter and hitter.plate_appearances and hitter.plate_appearances >= min_plate_appearances:
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
    season = get_season_from_request(request)

    # Prefetch all data to avoid N+1 queries
    users = list(CustomUser.objects.all())
    all_picks = Pick.objects.filter(category_id__in=[2, 4], season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = defaultdict(list)
    for pick in all_picks:
        picks_by_user[pick.user_id].append(pick)

    for user in users:
        user_picks = picks_by_user.get(user.mbr_id, [])
        user_homerun_picks = [p for p in user_picks if p.category_id == 4]
        user_alternate_picks = [p for p in user_picks if p.category_id == 2 and p.is_alternate]

        # Get home run stats for user's picks
        all_homerun_picks = []
        for pick in user_homerun_picks:
            hitter = hitter_stats.get(pick.player_name)
            if hitter:
                all_homerun_picks.append({
                    "player_name": pick.player_name,
                    "home_runs": hitter.home_runs or 0
                })

        # Of 4 picks per user, sort by home runs and take the top 3
        all_homerun_picks.sort(key=lambda p: -(p["home_runs"] or 0))
        top_three_picks = all_homerun_picks[:3]
        tiebreaker_pick = all_homerun_picks[3] if len(all_homerun_picks) > 3 else None

        top_three_total_homeruns = sum(pick["home_runs"] for pick in top_three_picks)
        first_tiebreaker_homeruns = tiebreaker_pick["home_runs"] if tiebreaker_pick else None

        # 2nd tiebreaker: average of qualified alternate batters
        qualified_alternates_average = 0
        alternate_batters_picks = []
        qualified_count = 0

        for pick in user_alternate_picks:
            hitter = hitter_stats.get(pick.player_name)
            if hitter:
                is_disqualified = not hitter.plate_appearances or hitter.plate_appearances < min_plate_appearances
                alternate_batters_picks.append({
                    "player_name": pick.player_name,
                    "average": round(hitter.average, 4) if hitter.average else 0,
                    "is_disqualified": is_disqualified
                })
                if not is_disqualified and hitter.average:
                    qualified_alternates_average += hitter.average
                    qualified_count += 1

        if qualified_count > 0:
            qualified_alternates_average = qualified_alternates_average / qualified_count

        # Append to leaderboard
        leaderboard.append({
            "user_name": user.name,
            "top_three_total_homeruns": top_three_total_homeruns,
            "first_tiebreaker_homeruns": first_tiebreaker_homeruns,
            "second_tiebreaker_average": round(qualified_alternates_average, 4),
            "rank": None,
            "all_homerun_picks": all_homerun_picks,
            "alternate_batters_picks": alternate_batters_picks
        })

    # Sort and rank users by total home runs, then tiebreakers
    leaderboard.sort(
        key=lambda entry: (
            -entry["top_three_total_homeruns"],
            -(entry["first_tiebreaker_homeruns"] or 0),
            -entry["second_tiebreaker_average"]
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
        return JsonResponse({'error': str(e)}, status=500)


def get_qualified_alternates_average(user_mbr_id, min_plate_appearances, hitter_stats, season, alternate_picks=None):
    """
    Helper function to calculate qualified alternates average for a user.
    Used as tiebreaker across multiple competitions.

    Args:
        user_mbr_id: The user's membership ID
        min_plate_appearances: Minimum PA required to qualify
        hitter_stats: Dict of player_name -> Hitter object for the season
        season: The season year to filter picks
        alternate_picks: Optional pre-fetched list of alternate picks for this user
    """
    if alternate_picks is None:
        alternate_picks = Pick.objects.filter(user_id=user_mbr_id, category_id=2, is_alternate=True, season=season)

    qualified_alternates = []

    for pick in alternate_picks:
        hitter = hitter_stats.get(pick.player_name)
        if hitter and hitter.plate_appearances and hitter.plate_appearances >= min_plate_appearances:
            qualified_alternates.append(hitter.average or 0)

    if qualified_alternates:
        return sum(qualified_alternates) / len(qualified_alternates)
    return 0


@api_view(['GET'])
def ops_leaderboard(request):
    """
    Calculate and return OPS leaderboard rankings.
    Uses same batters as batting average contest, ranked by aggregate OPS.
    """
    leaderboard = []
    min_plate_appearances = calculate_pro_rated_plate_appearances()
    season = get_season_from_request(request)

    # Prefetch all data to avoid N+1 queries
    users = list(CustomUser.objects.all())
    all_picks = Pick.objects.filter(category_id__in=[1, 2], season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = defaultdict(list)
    for pick in all_picks:
        picks_by_user[pick.user_id].append(pick)

    for user in users:
        user_picks = picks_by_user.get(user.mbr_id, [])
        regular_picks = [p for p in user_picks if p.category_id == 1 and not p.is_alternate]
        alternate_picks = [p for p in user_picks if p.category_id == 2 and p.is_alternate]

        qualified_picks = []
        disqualified_picks = []

        for pick in regular_picks:
            hitter = hitter_stats.get(pick.player_name)
            if hitter:
                if hitter.plate_appearances and hitter.plate_appearances >= min_plate_appearances:
                    qualified_picks.append({
                        "player_name": pick.player_name,
                        "ops": hitter.ops or 0,
                        "average": hitter.average or 0
                    })
                else:
                    disqualified_picks.append({
                        "player_name": pick.player_name,
                        "plate_appearances": hitter.plate_appearances or 0
                    })
            else:
                disqualified_picks.append({
                    "player_name": pick.player_name,
                    "plate_appearances": 0
                })

        # Handle alternates
        qualified_alternates = []
        for pick in alternate_picks:
            hitter = hitter_stats.get(pick.player_name)
            if hitter:
                if hitter.plate_appearances and hitter.plate_appearances >= min_plate_appearances:
                    qualified_alternates.append({
                        "player_name": pick.player_name,
                        "ops": hitter.ops or 0,
                        "average": hitter.average or 0
                    })
                else:
                    disqualified_picks.append({
                        "player_name": pick.player_name,
                        "plate_appearances": hitter.plate_appearances or 0
                    })

        # Replace unqualified regulars with qualified alternates
        final_qualified_picks = qualified_picks + qualified_alternates[:10 - len(qualified_picks)]

        # Determine if user is disqualified
        is_disqualified = len(final_qualified_picks) < 10
        aggregate_ops = None
        alternate_average = None

        if not is_disqualified:
            aggregate_ops = sum(pick["ops"] for pick in final_qualified_picks) / 10
            alternate_average = (
                sum(pick["average"] for pick in qualified_alternates) / len(qualified_alternates)
                if qualified_alternates else 0
            )

        user_entry = {
            "user_name": user.name,
            "aggregate_ops": round(aggregate_ops, 5) if aggregate_ops else None,
            "alternate_average": round(alternate_average, 4) if alternate_average else None,
            "qualified_picks": final_qualified_picks,
            "disqualified_picks": disqualified_picks,
            "rank": 0 if is_disqualified else None
        }
        leaderboard.append(user_entry)

    # Sort and rank users
    ranked_users = [entry for entry in leaderboard if entry["rank"] != 0]
    ranked_users.sort(
        key=lambda entry: (
            -entry["aggregate_ops"] if entry["aggregate_ops"] is not None else float('-inf'),
            -entry["alternate_average"] if entry["alternate_average"] is not None else float('-inf')
        )
    )

    for rank, entry in enumerate(ranked_users, start=1):
        entry["rank"] = rank

    final_response = ranked_users + [entry for entry in leaderboard if entry["rank"] == 0]

    try:
        serializer = OpsLeaderboardSerializer(final_response, many=True)
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def pitcher_leaderboard(request):
    """
    Calculate and return pitcher leaderboard rankings.
    Top 3 of 4 pitchers count toward total wins.
    Tiebreakers: 4th pick wins -> won-lost avg -> aggregate ERA -> alternates avg
    """
    leaderboard = []
    min_plate_appearances = calculate_pro_rated_plate_appearances()
    season = get_season_from_request(request)

    # Prefetch all data
    users = list(CustomUser.objects.all())
    pitcher_picks = Pick.objects.filter(category_id=3, season=season).select_related('user')
    pitcher_stats = build_pitcher_stats_lookup(season)
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = defaultdict(list)
    for pick in pitcher_picks:
        picks_by_user[pick.user_id].append(pick)

    for user in users:
        user_picks = picks_by_user.get(user.mbr_id, [])
        all_pitcher_picks = []
        total_wins = 0
        total_losses = 0
        total_era = 0
        era_count = 0

        for pick in user_picks:
            pitcher = pitcher_stats.get(pick.player_name)
            if pitcher:
                wins = pitcher.wins or 0
                losses = pitcher.losses or 0
                era = pitcher.era

                all_pitcher_picks.append({
                    "player_name": pick.player_name,
                    "wins": wins,
                    "losses": losses,
                    "era": era,
                    "strikeouts": pitcher.strikeouts or 0
                })
                total_wins += wins
                total_losses += losses
                if era is not None:
                    total_era += era
                    era_count += 1

        # Sort by wins and take top 3
        all_pitcher_picks.sort(key=lambda p: -(p["wins"] or 0))
        top_three = all_pitcher_picks[:3]
        tiebreaker_pick = all_pitcher_picks[3] if len(all_pitcher_picks) > 3 else None

        top_three_total_wins = sum(p["wins"] or 0 for p in top_three)
        first_tiebreaker_wins = tiebreaker_pick["wins"] if tiebreaker_pick else None

        # Calculate won-lost average (win percentage)
        total_games = total_wins + total_losses
        win_pct = total_wins / total_games if total_games > 0 else None

        # Calculate aggregate ERA
        avg_era = total_era / era_count if era_count > 0 else None

        # Get qualified alternates average
        alt_avg = get_qualified_alternates_average(user.mbr_id, min_plate_appearances, hitter_stats, season)

        leaderboard.append({
            "user_name": user.name,
            "top_three_total_wins": top_three_total_wins,
            "first_tiebreaker_wins": first_tiebreaker_wins,
            "second_tiebreaker_win_pct": round(win_pct, 4) if win_pct is not None else None,
            "third_tiebreaker_era": round(avg_era, 2) if avg_era is not None else None,
            "fourth_tiebreaker_alt_avg": round(alt_avg, 4) if alt_avg else None,
            "rank": None,
            "pitcher_picks": all_pitcher_picks
        })

    # Sort by total wins, then tiebreakers
    leaderboard.sort(
        key=lambda entry: (
            -entry["top_three_total_wins"],
            -(entry["first_tiebreaker_wins"] or 0),
            -(entry["second_tiebreaker_win_pct"] or 0),
            entry["third_tiebreaker_era"] if entry["third_tiebreaker_era"] is not None else float('inf'),
            -(entry["fourth_tiebreaker_alt_avg"] or 0)
        )
    )

    for rank, entry in enumerate(leaderboard, start=1):
        entry["rank"] = rank

    try:
        serializer = PitcherLeaderboardSerializer(leaderboard, many=True)
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def rbi_champion_leaderboard(request):
    """
    Calculate and return RBI champion leaderboard.
    Must pick the correct player who leads MLB in RBIs.
    Among correct pickers, closest to actual RBI count wins.
    """
    min_plate_appearances = calculate_pro_rated_plate_appearances()
    season = get_season_from_request(request)

    # Find actual RBI leader from hitters table for the given season
    rbi_leader = Hitter.objects.filter(season=season).select_related('player').order_by('-rbis').first()
    actual_rbi_leader = None
    if rbi_leader:
        actual_rbi_leader = {
            "player_name": rbi_leader.player.player_name,
            "rbis": rbi_leader.rbis or 0
        }

    # Prefetch data
    users = list(CustomUser.objects.all())
    rbi_picks = Pick.objects.filter(category_id=5, season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = {}
    for pick in rbi_picks:
        picks_by_user[pick.user_id] = pick

    leaderboard = []
    for user in users:
        pick = picks_by_user.get(user.mbr_id)
        if not pick:
            continue

        predicted_correct = False
        rbi_difference = None

        if actual_rbi_leader and pick.player_name == actual_rbi_leader["player_name"]:
            predicted_correct = True
            predicted_rbis = pick.pick_value or 0
            rbi_difference = abs(actual_rbi_leader["rbis"] - predicted_rbis)

        alt_avg = get_qualified_alternates_average(user.mbr_id, min_plate_appearances, hitter_stats, season)

        leaderboard.append({
            "user_name": user.name,
            "predicted_player": pick.player_name,
            "predicted_rbis": pick.pick_value,
            "predicted_correct_player": predicted_correct,
            "rbi_difference": rbi_difference,
            "alternates_average": round(alt_avg, 4) if alt_avg else None,
            "rank": None
        })

    # Sort: correct pickers first (by difference, then alternates avg), then incorrect pickers
    correct_pickers = [e for e in leaderboard if e["predicted_correct_player"]]
    incorrect_pickers = [e for e in leaderboard if not e["predicted_correct_player"]]

    correct_pickers.sort(
        key=lambda e: (
            e["rbi_difference"] or float('inf'),
            -(e["alternates_average"] or 0)
        )
    )

    for rank, entry in enumerate(correct_pickers, start=1):
        entry["rank"] = rank

    # Incorrect pickers get no rank (prize not awarded if wrong player)
    for entry in incorrect_pickers:
        entry["rank"] = None

    final_leaderboard = correct_pickers + incorrect_pickers

    try:
        serializer = RbiChampionLeaderboardSerializer({
            "actual_rbi_leader": actual_rbi_leader,
            "leaderboard": final_leaderboard
        })
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def stolen_base_leaderboard(request):
    """
    Calculate and return stolen base champion leaderboard.
    Must pick the correct player who leads MLB in stolen bases.
    Among correct pickers, closest to actual SB count wins.
    """
    min_plate_appearances = calculate_pro_rated_plate_appearances()
    season = get_season_from_request(request)

    # Find actual SB leader from hitters table for the given season
    sb_leader = Hitter.objects.filter(season=season).select_related('player').order_by('-stolen_bases').first()
    actual_sb_leader = None
    if sb_leader:
        actual_sb_leader = {
            "player_name": sb_leader.player.player_name,
            "stolen_bases": sb_leader.stolen_bases or 0
        }

    # Prefetch data
    users = list(CustomUser.objects.all())
    sb_picks = Pick.objects.filter(category_id=6, season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = {}
    for pick in sb_picks:
        picks_by_user[pick.user_id] = pick

    leaderboard = []
    for user in users:
        pick = picks_by_user.get(user.mbr_id)
        if not pick:
            continue

        predicted_correct = False
        sb_difference = None

        if actual_sb_leader and pick.player_name == actual_sb_leader["player_name"]:
            predicted_correct = True
            predicted_sb = pick.pick_value or 0
            sb_difference = abs(actual_sb_leader["stolen_bases"] - predicted_sb)

        alt_avg = get_qualified_alternates_average(user.mbr_id, min_plate_appearances, hitter_stats, season)

        leaderboard.append({
            "user_name": user.name,
            "predicted_player": pick.player_name,
            "predicted_stolen_bases": pick.pick_value,
            "predicted_correct_player": predicted_correct,
            "sb_difference": sb_difference,
            "alternates_average": round(alt_avg, 4) if alt_avg else None,
            "rank": None
        })

    # Sort: correct pickers first, then by difference, then alternates avg
    correct_pickers = [e for e in leaderboard if e["predicted_correct_player"]]
    incorrect_pickers = [e for e in leaderboard if not e["predicted_correct_player"]]

    correct_pickers.sort(
        key=lambda e: (
            e["sb_difference"] or float('inf'),
            -(e["alternates_average"] or 0)
        )
    )

    for rank, entry in enumerate(correct_pickers, start=1):
        entry["rank"] = rank

    for entry in incorrect_pickers:
        entry["rank"] = None

    final_leaderboard = correct_pickers + incorrect_pickers

    try:
        serializer = StolenBaseLeaderboardSerializer({
            "actual_sb_leader": actual_sb_leader,
            "leaderboard": final_leaderboard
        })
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def dimaggio_leaderboard(request):
    """
    Calculate and return DiMaggio Prize leaderboard.
    Exact number of longest hitting streak required to win.
    No player selection - just the streak number.
    """
    min_plate_appearances = calculate_pro_rated_plate_appearances()
    season = get_season_from_request(request)

    # Get actual longest streak from SeasonStats model for requested season
    season_stats = SeasonStats.objects.filter(year=season).first()
    actual_longest_streak = season_stats.longest_hitting_streak if season_stats else None
    streak_holder_name = season_stats.streak_holder_name if season_stats else None

    # Prefetch data
    users = list(CustomUser.objects.all())
    dimaggio_picks = Pick.objects.filter(category_id=7, season=season).select_related('user')
    hitter_stats = build_hitter_stats_lookup(season)

    # Group picks by user
    picks_by_user = {}
    for pick in dimaggio_picks:
        picks_by_user[pick.user_id] = pick

    leaderboard = []
    for user in users:
        pick = picks_by_user.get(user.mbr_id)
        if not pick:
            continue

        predicted_streak = pick.pick_value
        is_exact_match = False

        if actual_longest_streak is not None and predicted_streak == actual_longest_streak:
            is_exact_match = True

        alt_avg = get_qualified_alternates_average(user.mbr_id, min_plate_appearances, hitter_stats, season)

        leaderboard.append({
            "user_name": user.name,
            "predicted_streak": predicted_streak,
            "is_exact_match": is_exact_match,
            "alternates_average": round(alt_avg, 4) if alt_avg else None,
            "rank": None
        })

    # Only exact matches get ranked
    exact_matches = [e for e in leaderboard if e["is_exact_match"]]
    non_matches = [e for e in leaderboard if not e["is_exact_match"]]

    # Sort exact matches by alternates average (tiebreaker)
    exact_matches.sort(key=lambda e: -(e["alternates_average"] or 0))

    for rank, entry in enumerate(exact_matches, start=1):
        entry["rank"] = rank

    final_leaderboard = exact_matches + non_matches

    try:
        serializer = DiMaggioLeaderboardSerializer({
            "actual_longest_streak": actual_longest_streak,
            "streak_holder_name": streak_holder_name,
            "leaderboard": final_leaderboard
        })
        return Response(serializer.data, status=200, content_type='application/json')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@api_view(['GET'])
def user_list(request):
    """Return list of all contestants."""
    users = CustomUser.objects.all().values('mbr_id', 'name')
    serializer = UserSerializer(users, many=True)
    return Response(serializer.data, status=200)


@api_view(['GET'])
def category_list(request):
    """Return list of competition categories with metadata."""
    # Category metadata (not stored in DB, defined here)
    category_info = {
        1: {"display_name": "Batting Average", "picks_per_user": 10,
            "description": "Pick 10 batters. Aggregate batting average determines ranking."},
        2: {"display_name": "Alternate Batters", "picks_per_user": 5,
            "description": "5 backup batters used if regulars are disqualified."},
        3: {"display_name": "Pitching Victories", "picks_per_user": 4,
            "description": "Pick 4 pitchers. Top 3 wins count toward total."},
        4: {"display_name": "Home Runs", "picks_per_user": 4,
            "description": "Pick 4 home run hitters. Top 3 count toward total."},
        5: {"display_name": "RBI Champion", "picks_per_user": 1,
            "description": "Pick the player who will lead MLB in RBIs and predict the total."},
        6: {"display_name": "Stolen Base Champion", "picks_per_user": 1,
            "description": "Pick the player who will lead MLB in stolen bases and predict the total."},
        7: {"display_name": "DiMaggio Prize", "picks_per_user": 1,
            "description": "Predict the longest hitting streak of the season. Exact number required."},
    }

    categories = Category.objects.all()
    result = []
    for cat in categories:
        info = category_info.get(cat.id, {})
        result.append({
            "id": cat.id,
            "name": cat.name,
            "display_name": info.get("display_name", cat.name),
            "picks_per_user": info.get("picks_per_user", 1),
            "description": info.get("description", "")
        })

    serializer = CategorySerializer(result, many=True)
    return Response(serializer.data, status=200)
