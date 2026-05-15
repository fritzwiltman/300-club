# leaderboard/tests/test_serializers.py
import pytest
from rest_framework.exceptions import ValidationError
from leaderboard.models import Player
from leaderboard.serializers import (
    PlayerSerializer,
    HitterLeaderboardSerializer,
    OpsLeaderboardSerializer,
    HomerunLeaderboardSerializer,
    PitcherLeaderboardSerializer,
    RbiChampionLeaderboardSerializer,
    StolenBaseLeaderboardSerializer,
    DiMaggioLeaderboardSerializer,
    AlternateBatterSerializer,
    PitcherPickSerializer,
    RbiChampionEntrySerializer,
    StolenBaseEntrySerializer,
    DiMaggioEntrySerializer,
    UserSerializer,
    CategorySerializer,
)

@pytest.mark.django_db
def test_player_serializer_valid():
    """
    Test that PlayerSerializer successfully validates
    and saves valid data.
    """
    data = {
        "player_name": "Test Player",
        "player_type": "hitter",
        "api_player_id": 1
    }
    serializer = PlayerSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"

    instance = serializer.save()  # Create Player in DB
    assert instance.player_name == data["player_name"]
    assert instance.player_type == data["player_type"]
    assert instance.api_player_id == data["api_player_id"]

@pytest.mark.django_db
def test_player_serializer_invalid_missing_fields():
    """
    Test that PlayerSerializer rejects data when required fields are missing.
    """
    data = {
        # "player_name" is missing
        "player_type": "hitter",
        "api_player_id": 1
    }
    serializer = PlayerSerializer(data=data)

    assert not serializer.is_valid()
    assert "player_name" in serializer.errors  # Expecting an error for missing player_name

@pytest.mark.django_db
def test_player_serializer_invalid_wrong_type():
    """
    Test that PlayerSerializer rejects invalid field choices.
    """
    data = {
        "player_name": "Test Player",
        "player_type": "invalid_type",  # Not 'hitter' or 'pitcher'
        "api_player_id": 99
    }
    serializer = PlayerSerializer(data=data)

    assert not serializer.is_valid()
    assert "player_type" in serializer.errors

@pytest.mark.django_db
def test_player_serializer_update():
    """
    Test updating an existing Player using the serializer.
    """
    player = Player.objects.create(player_name="Original", player_type="hitter", api_player_id=10)
    update_data = {"player_name": "Updated Name"}

    serializer = PlayerSerializer(player, data=update_data, partial=True)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"

    updated_player = serializer.save()
    assert updated_player.player_name == "Updated Name"
    # Other fields remain unchanged
    assert updated_player.player_type == "hitter"
    assert updated_player.api_player_id == 10


# ============== HITTER LEADERBOARD SERIALIZER TESTS ==============

def test_hitter_leaderboard_serializer_valid():
    """Test HitterLeaderboardSerializer with valid data."""
    data = {
        "user_name": "Test User",
        "aggregate_average": "0.3000",
        "alternate_average": "0.2800",
        "aggregate_ops": "0.9000",
        "rank": 1,
        "qualified_picks": [{"player_name": "Player 1", "average": "0.300"}],
        "disqualified_picks": []
    }
    serializer = HitterLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_hitter_leaderboard_serializer_null_aggregates():
    """Test HitterLeaderboardSerializer allows null aggregates for disqualified users."""
    data = {
        "user_name": "Disqualified User",
        "aggregate_average": None,
        "alternate_average": None,
        "aggregate_ops": None,
        "rank": 0,
        "qualified_picks": [],
        "disqualified_picks": [{"player_name": "Player 1", "plate_appearances": "100"}]
    }
    serializer = HitterLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_hitter_leaderboard_serializer_empty_picks():
    """Test HitterLeaderboardSerializer with empty pick lists."""
    data = {
        "user_name": "Empty User",
        "aggregate_average": "0.2500",
        "alternate_average": "0.2400",
        "aggregate_ops": "0.7500",
        "rank": 5,
        "qualified_picks": [],
        "disqualified_picks": []
    }
    serializer = HitterLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== OPS LEADERBOARD SERIALIZER TESTS ==============

def test_ops_leaderboard_serializer_valid():
    """Test OpsLeaderboardSerializer with valid data."""
    data = {
        "user_name": "OPS Leader",
        "aggregate_ops": "0.90000",
        "alternate_average": "0.2800",
        "rank": 1,
        "qualified_picks": [{"player_name": "P1", "ops": "0.900"}],
        "disqualified_picks": []
    }
    serializer = OpsLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_ops_leaderboard_serializer_null_ops():
    """Test OpsLeaderboardSerializer allows null OPS for disqualified."""
    data = {
        "user_name": "No OPS",
        "aggregate_ops": None,
        "alternate_average": None,
        "rank": 0,
        "qualified_picks": [],
        "disqualified_picks": [{"player_name": "P1", "plate_appearances": "50"}]
    }
    serializer = OpsLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== HOMERUN LEADERBOARD SERIALIZER TESTS ==============

def test_homerun_leaderboard_serializer_valid():
    """Test HomerunLeaderboardSerializer with valid data."""
    data = {
        "user_name": "HR King",
        "top_three_total_homeruns": 120,
        "first_tiebreaker_homeruns": 35,
        "second_tiebreaker_average": 0.2800,
        "rank": 1,
        "all_homerun_picks": [{"player_name": "P1", "home_runs": "40"}],
        "alternate_batters_picks": [{"player_name": "A1", "average": 0.280, "is_disqualified": False}]
    }
    serializer = HomerunLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_homerun_leaderboard_serializer_null_tiebreaker():
    """Test HomerunLeaderboardSerializer allows null first_tiebreaker."""
    data = {
        "user_name": "No Tiebreaker",
        "top_three_total_homeruns": 90,
        "first_tiebreaker_homeruns": None,
        "second_tiebreaker_average": 0.2500,
        "rank": 3,
        "all_homerun_picks": [],
        "alternate_batters_picks": []
    }
    serializer = HomerunLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_homerun_leaderboard_serializer_null_rank():
    """Test HomerunLeaderboardSerializer allows null rank."""
    data = {
        "user_name": "Unranked",
        "top_three_total_homeruns": 0,
        "first_tiebreaker_homeruns": None,
        "second_tiebreaker_average": 0.0,
        "rank": None,
        "all_homerun_picks": [],
        "alternate_batters_picks": []
    }
    serializer = HomerunLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== PITCHER LEADERBOARD SERIALIZER TESTS ==============

def test_pitcher_leaderboard_serializer_valid():
    """Test PitcherLeaderboardSerializer with valid data."""
    data = {
        "user_name": "Ace Owner",
        "top_three_total_wins": 45,
        "first_tiebreaker_wins": 12,
        "second_tiebreaker_win_pct": 0.6500,
        "third_tiebreaker_era": 3.25,
        "fourth_tiebreaker_alt_avg": 0.2900,
        "rank": 1,
        "pitcher_picks": [{"player_name": "P1", "wins": 15, "losses": 5, "era": 3.00, "strikeouts": 200}]
    }
    serializer = PitcherLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_pitcher_leaderboard_serializer_null_tiebreakers():
    """Test PitcherLeaderboardSerializer allows null tiebreakers."""
    data = {
        "user_name": "Few Stats",
        "top_three_total_wins": 30,
        "first_tiebreaker_wins": None,
        "second_tiebreaker_win_pct": None,
        "third_tiebreaker_era": None,
        "fourth_tiebreaker_alt_avg": None,
        "rank": 5,
        "pitcher_picks": []
    }
    serializer = PitcherLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== PITCHER PICK SERIALIZER TESTS ==============

def test_pitcher_pick_serializer_valid():
    """Test PitcherPickSerializer with valid data."""
    data = {"player_name": "Ace", "wins": 15, "losses": 5, "era": 2.50, "strikeouts": 220}
    serializer = PitcherPickSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_pitcher_pick_serializer_null_stats():
    """Test PitcherPickSerializer allows null stats."""
    data = {"player_name": "Rookie", "wins": None, "losses": None, "era": None, "strikeouts": None}
    serializer = PitcherPickSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== RBI CHAMPION SERIALIZER TESTS ==============

def test_rbi_champion_entry_serializer_valid():
    """Test RbiChampionEntrySerializer with valid data."""
    data = {
        "user_name": "RBI Guesser",
        "predicted_player": "Aaron Judge",
        "predicted_rbis": 140,
        "picked_player_ytd_rbi": 130,
        "deviation": -10,
        "alternates_average": 0.2800,
        "rank": 1
    }
    serializer = RbiChampionEntrySerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_rbi_champion_leaderboard_serializer_valid():
    """Test RbiChampionLeaderboardSerializer with valid data."""
    data = {
        "actual_rbi_leader": {"player_name": "Leader", "rbis": 130},
        "prorated_projection": 145.5,
        "leaderboard": [{
            "user_name": "Test",
            "predicted_player": "Leader",
            "predicted_rbis": 140,
            "picked_player_ytd_rbi": 130,
            "deviation": -5,
            "alternates_average": 0.2800,
            "rank": 1
        }]
    }
    serializer = RbiChampionLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_rbi_champion_leaderboard_serializer_null_leader():
    """Test RbiChampionLeaderboardSerializer with null leader (pre-season)."""
    data = {
        "actual_rbi_leader": None,
        "prorated_projection": None,
        "leaderboard": []
    }
    serializer = RbiChampionLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== STOLEN BASE SERIALIZER TESTS ==============

def test_stolen_base_entry_serializer_valid():
    """Test StolenBaseEntrySerializer with valid data."""
    data = {
        "user_name": "Speed Guesser",
        "predicted_player": "Fast Runner",
        "predicted_stolen_bases": 75,
        "picked_player_ytd_sb": 70,
        "deviation": -5,
        "alternates_average": 0.2700,
        "rank": 1
    }
    serializer = StolenBaseEntrySerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_stolen_base_leaderboard_serializer_valid():
    """Test StolenBaseLeaderboardSerializer with valid data."""
    data = {
        "actual_sb_leader": {"player_name": "Speed", "stolen_bases": 70},
        "prorated_projection": 85.0,
        "leaderboard": [{
            "user_name": "Test",
            "predicted_player": "Speed",
            "predicted_stolen_bases": 75,
            "picked_player_ytd_sb": 70,
            "deviation": -10,
            "alternates_average": 0.2700,
            "rank": 1
        }]
    }
    serializer = StolenBaseLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_stolen_base_leaderboard_serializer_null_leader():
    """Test StolenBaseLeaderboardSerializer with null leader."""
    data = {
        "actual_sb_leader": None,
        "prorated_projection": None,
        "leaderboard": []
    }
    serializer = StolenBaseLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== DIMAGGIO SERIALIZER TESTS ==============

def test_dimaggio_entry_serializer_valid():
    """Test DiMaggioEntrySerializer with valid data."""
    data = {
        "user_name": "Streak Guesser",
        "predicted_streak": 25,
        "is_exact_match": True,
        "alternates_average": 0.2900,
        "rank": 1
    }
    serializer = DiMaggioEntrySerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_dimaggio_entry_serializer_no_match():
    """Test DiMaggioEntrySerializer for non-matching prediction."""
    data = {
        "user_name": "Wrong Guess",
        "predicted_streak": 30,
        "is_exact_match": False,
        "alternates_average": 0.2800,
        "rank": None
    }
    serializer = DiMaggioEntrySerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_dimaggio_leaderboard_serializer_valid():
    """Test DiMaggioLeaderboardSerializer with valid data."""
    data = {
        "actual_longest_streak": 25,
        "streak_holder_name": "Hot Hitter",
        "leaderboard": [{
            "user_name": "Test",
            "predicted_streak": 25,
            "is_exact_match": True,
            "alternates_average": 0.2900,
            "rank": 1
        }]
    }
    serializer = DiMaggioLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_dimaggio_leaderboard_serializer_null_streak():
    """Test DiMaggioLeaderboardSerializer with null streak (mid-season)."""
    data = {
        "actual_longest_streak": None,
        "streak_holder_name": None,
        "leaderboard": [{
            "user_name": "Test",
            "predicted_streak": 30,
            "is_exact_match": False,
            "alternates_average": 0.2800,
            "rank": None
        }]
    }
    serializer = DiMaggioLeaderboardSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== ALTERNATE BATTER SERIALIZER TESTS ==============

def test_alternate_batter_serializer_valid():
    """Test AlternateBatterSerializer with valid data."""
    data = {"player_name": "Alt Player", "average": 0.285, "is_disqualified": False}
    serializer = AlternateBatterSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_alternate_batter_serializer_disqualified():
    """Test AlternateBatterSerializer for disqualified alternate."""
    data = {"player_name": "DQ Alt", "average": 0.220, "is_disqualified": True}
    serializer = AlternateBatterSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


# ============== USER SERIALIZER TESTS ==============

def test_user_serializer_valid():
    """Test UserSerializer with valid data."""
    data = {"mbr_id": 123, "name": "Test User"}
    serializer = UserSerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_user_serializer_from_instance():
    """Test UserSerializer serializes data correctly."""
    data = {"mbr_id": 456, "name": "Another User"}
    serializer = UserSerializer(data=data)
    assert serializer.is_valid()
    assert serializer.validated_data["mbr_id"] == 456
    assert serializer.validated_data["name"] == "Another User"


# ============== CATEGORY SERIALIZER TESTS ==============

def test_category_serializer_valid():
    """Test CategorySerializer with valid data."""
    data = {
        "id": 1,
        "name": "batters",
        "display_name": "Batting Average",
        "picks_per_user": 10,
        "description": "Pick 10 batters."
    }
    serializer = CategorySerializer(data=data)
    assert serializer.is_valid(), f"Serializer errors: {serializer.errors}"


def test_category_serializer_all_fields():
    """Test CategorySerializer validates all required fields."""
    data = {
        "id": 3,
        "name": "pitchers",
        "display_name": "Pitching Victories",
        "picks_per_user": 4,
        "description": "Select 4 starting pitchers."
    }
    serializer = CategorySerializer(data=data)
    assert serializer.is_valid()
    assert serializer.validated_data["picks_per_user"] == 4