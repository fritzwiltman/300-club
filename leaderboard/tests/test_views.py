# leaderboard/tests/test_views.py
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock
from rest_framework.test import APIClient
from rest_framework import status
from leaderboard.models import CustomUser, Pick, Player, Hitter, Pitcher, Category, SeasonStats, MlbLeader
from leaderboard.views import (
    calculate_pro_rated_plate_appearances,
    get_season_from_request,
    normalize_name,
    build_hitter_stats_lookup,
    build_pitcher_stats_lookup,
    get_users_with_picks,
    get_season_progress,
    calculate_prorated_projection,
    get_qualified_alternates_average,
    DEFAULT_SEASON,
)
import json

@pytest.mark.django_db
def test_player_list_view():
    """
    Test the GET /leaderboard/players/ endpoint returns a list of players.
    """
    # Setup: create sample Player objects
    Player.objects.create(player_name="Test Player 1", player_type="hitter", api_player_id=1)
    Player.objects.create(player_name="Test Player 2", player_type="pitcher", api_player_id=2)

    client = APIClient()
    response = client.get('/leaderboard/players/')
    
    assert response.status_code == status.HTTP_200_OK, response.content
    data = response.json()
    assert len(data) == 2
    assert data[0]["player_name"] == "Test Player 1"
    assert data[1]["player_name"] == "Test Player 2"

@pytest.fixture
def setup_hitters_test_data():
    """Fixture to set up test data before each test for hitters leaderboard"""
    category_batters = Category.objects.create(id=1, name="batters")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)
    user3 = CustomUser.objects.create(name="User 3", mbr_id=3)

    # Create 40 players
    players = []
    for i in range(40):
        players.append(Player.objects.create(
            id=i+1,
            player_name=f"Player {i+1}",
            player_type="hitter",
            api_player_id=10000+i+1,
        ))

    # Create hitters with various stats (use DEFAULT_SEASON=2026)
    for i, player in enumerate(players):
        # Set lower plate appearances for some alternates to force disqualification
        if i < 30:
            plate_appearances = 600  # Qualified
        elif 30 <= i < 35:
            plate_appearances = 450  # These should get disqualified as alternates
        elif 35 <= i < 38:
            plate_appearances = 650  # Qualified
        else:
            plate_appearances = 400  # Strongly disqualified
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.300 + (i * .002),  # Slightly increasing averages
            ops=1.000 - (i * 0.002),  # Slightly decreasing OPS
            plate_appearances=plate_appearances,
        )

    # User 1 Picks - 10 Regular, 5 Alternates (use DEFAULT_SEASON=2026)
    for i in range(10):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_batters.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1, season=2026)

    for i in range(10, 15):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    # User 2 Picks (10 Different Regulars)
    for i in range(10):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_batters.id, player_name=players[i+10].player_name, is_alternate=False, pick_order=i+1, season=2026)

    for i in range(10, 15):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    # User 3 Picks (10 Different Regulars, 7 of which are disqualified)
    for i in range(10):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_batters.id, player_name=players[i+25].player_name, is_alternate=False, pick_order=i+1, season=2026)

    for i in range(35, 40):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    return user1, user2, user3, players


@pytest.mark.django_db
def test_hitter_leaderboard_with_disqualified_alternate(setup_hitters_test_data, monkeypatch):
    """Ensure that at least one alternate pick is disqualified due to low plate appearances."""
    # Mock date to end of season so min_pa is high (~502)
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 9, 30)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    # Find a user with disqualified alternates
    user_with_disqualified_alternates = next(
        (user for user in data if any(
            int(pick["plate_appearances"]) < calculate_pro_rated_plate_appearances()
            for pick in user["disqualified_picks"]
        )), None
    )

    assert user_with_disqualified_alternates is not None, "At least one user should have disqualified alternates"


@pytest.mark.django_db
def test_hitter_leaderboard(setup_hitters_test_data, monkeypatch):
    """
    Tests that hitter leaderboard correctly calculates rankings and includes disqualified users.
    """
    # Mock date to end of season so min_pa is high (~502)
    # This ensures players with PA < 502 are disqualified
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 9, 30)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    # Ensure the response status is 200
    assert response.status_code == 200

    # Ensure 3 users are returned (2 ranked, 1 disqualified)
    assert len(data) == 3

    # Ensure the first user is ranked 1 (highest average)
    assert data[0]["user_name"] == "User 2"
    assert data[0]["rank"] == 1

    # Ensure the second user is ranked 2
    assert data[1]["user_name"] == "User 1"
    assert data[1]["rank"] == 2

    # Ensure User 3 is disqualified (rank = 0) and has disqualified picks
    disqualified_user = next(user for user in data if user["rank"] == 0)
    assert disqualified_user["user_name"] == "User 3"
    assert disqualified_user["disqualified_picks"] != []  # Ensure picks are included


@pytest.mark.django_db
def test_all_disqualified_users_are_present(setup_hitters_test_data, monkeypatch):
    # Mock date to end of season so min_pa is high (~502)
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 9, 30)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    # Find all users with rank 0 (disqualified users)
    disqualified_users = [entry for entry in data if entry["rank"] == 0]

    # Ensure that at least one user is disqualified
    assert len(disqualified_users) >= 1, "There should be at least one disqualified user."

    # Check that User 3 is among them
    user3_entry = next((entry for entry in disqualified_users if entry["user_name"] == "User 3"), None)
    assert user3_entry is not None, "User 3 should be in the disqualified list."

    # Ensure User 3 has 8 qualified batters (5 regulars + 3 alternates)
    assert len(user3_entry["qualified_picks"]) == 8


@pytest.mark.django_db
def test_disqualified_users_have_picks_displayed(setup_hitters_test_data, monkeypatch):
    # Mock date to end of season so min_pa is high (~502)
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 9, 30)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    # Find User 3
    user3_entry = next((entry for entry in data if entry["user_name"] == "User 3"), None)
    assert user3_entry is not None, "User 3 should be in the leaderboard."

    # Assert User 3 has a rank of 0 (disqualified)
    assert user3_entry["rank"] == 0

    # Ensure that User 3 still has their picks listed
    assert "qualified_picks" in user3_entry
    assert len(user3_entry["qualified_picks"]) == 8  # Should still appear


@pytest.mark.django_db
def test_hitter_leaderboard_serialization_error(mocker):
    """Ensure that hitter leaderboard handles serialization errors gracefully."""
    mocker.patch("leaderboard.views.HitterLeaderboardSerializer", side_effect=Exception("Mocked serialization error"))
    
    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    assert response.status_code == 500
    assert "error" in data
    assert "Mocked serialization error" in data["error"]


@pytest.mark.django_db
def test_pro_rated_plate_appearances(monkeypatch):
    """Test dynamic plate appearances calculation based on date."""
    # SEASON_START is date(2026, 3, 25), TOTAL_WEEKS=27, FULL_SEASON_PA=502
    test_cases = [
        (date(2026, 4, 1), 502 / 27),  # Week 1
        (date(2026, 4, 29), 502 / 27 * 5),  # Week 5
        (date(2026, 9, 30), 502),  # End of Season
    ]

    for test_date, expected in test_cases:
        # Mock the date class itself, not just today()
        class MockDate(date):
            @classmethod
            def today(cls):
                return test_date

        monkeypatch.setattr("leaderboard.views.date", MockDate)
        result = calculate_pro_rated_plate_appearances()

        assert result == pytest.approx(expected, rel=1e-2), f"Failed for {test_date}"


@pytest.mark.django_db
def test_pro_rated_plate_appearances_before_season(monkeypatch):
    """Ensure that if today is before the season start, the function returns 0."""
    future_date = date(2024, 3, 1)  # Before season start (March 18)
    
    class MockDate(date):
        @classmethod
        def today(cls):
            return future_date

    monkeypatch.setattr("leaderboard.views.date", MockDate)  # Mock today's date
    result = calculate_pro_rated_plate_appearances()
    
    assert result == 0, "Expected 0 plate appearances before season start"

@pytest.fixture
def setup_homerun_test_data():
    """Fixture to set up test data before each test for home run leaderboard"""
    category_homeruns = Category.objects.create(id=4, name="homeruns")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)
    user3 = CustomUser.objects.create(name="User 3", mbr_id=3)

    # Create 20 players for testing
    players = []
    for i in range(20):
        players.append(Player.objects.create(
            id=i+1,
            player_name=f"Player {i+1}",
            player_type="hitter",
            api_player_id=20000+i+1,
        ))

    # Create hitters with varying home runs and plate appearances (all qualified)
    for i, player in enumerate(players):
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.280 + (i * 0.001),
            ops=0.900 - (i * 0.005),
            plate_appearances=600,
            home_runs=20 + (i * 2)
        )

    # User 1 Picks (4 Regulars)
    for i in range(4):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1, season=2026)

    # User 2 Picks (4 Different Regulars)
    for i in range(4):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1, season=2026)

    # User 3 Picks (4 picks)
    for i in range(8, 12):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1, season=2026)

    # Alternate Picks for User 1 (For second tiebreaker)
    for i in range(15, 18):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    # Alternate Picks for User 2 (For second tiebreaker)
    for i in range(16, 19):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    # Alternate Picks for User 3 (For second tiebreaker)
    for i in range(17, 20):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1, season=2026)

    return user1, user2, user3, players


@pytest.mark.django_db
def test_homerun_leaderboard(setup_homerun_test_data):
    """
    Test the GET /leaderboard/homeruns/ endpoint returns a properly ranked home run leaderboard.
    """
    client = APIClient()
    response = client.get('/leaderboard/homeruns/')
    data = response.json()
    print(json.dumps(data, indent=4))
    # Ensure response is 200
    assert response.status_code == status.HTTP_200_OK, response.content

    # Ensure 3 users are returned
    assert len(data) == 3

    # Ensure ranking is correct (User 2 should have the most HRs, then User 1, then User 3)
    assert data[0]["user_name"] == "User 3"
    assert data[0]["rank"] == 1

    assert data[1]["user_name"] == "User 2"
    assert data[1]["rank"] == 2

    assert data[2]["user_name"] == "User 1"
    assert data[2]["rank"] == 3


@pytest.mark.django_db
def test_homerun_leaderboard_tiebreakers(setup_homerun_test_data):
    """
    Ensure that the tiebreakers (4th pick and alternates) correctly determine rankings.
    """
    client = APIClient()
    response = client.get('/leaderboard/homeruns/')
    data = response.json()

    # Find user 3
    user3_entry = next(user for user in data if user["user_name"] == "User 3")

    # Ensure User 3 has the correct number of home runs
    assert len(user3_entry["all_homerun_picks"]) == 4  # Should still have 4 picks
    assert int(user3_entry["all_homerun_picks"][-1]["home_runs"]) > 0  # Last pick is the first tiebreaker

    # Ensure the second tiebreaker is properly calculated
    assert "second_tiebreaker_average" in user3_entry


@pytest.mark.django_db
def test_homerun_leaderboard_serialization_error(mocker):
    """
    Ensure the home run leaderboard handles serialization errors gracefully.
    """
    mocker.patch("leaderboard.views.HomerunLeaderboardSerializer", side_effect=Exception("Mocked serialization error"))

    client = APIClient()
    response = client.get('/leaderboard/homeruns/')
    data = response.json()

    assert response.status_code == 500
    assert "error" in data
    assert "Mocked serialization error" in data["error"]


# ============== OPS LEADERBOARD TESTS ==============

@pytest.mark.django_db
def test_ops_leaderboard(setup_hitters_test_data, monkeypatch):
    """
    Test the GET /leaderboard/ops/ endpoint returns a properly ranked OPS leaderboard.
    """
    # Mock date to end of season so min_pa is high (~502)
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 9, 30)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/ops/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data) == 3

    # User 1 has the highest OPS (lowest index players have higher OPS in fixture)
    assert data[0]["user_name"] == "User 1"
    assert data[0]["rank"] == 1
    assert data[0]["aggregate_ops"] is not None

    # User 3 should be disqualified (rank = 0)
    disqualified_user = next((user for user in data if user["rank"] == 0), None)
    assert disqualified_user is not None
    assert disqualified_user["user_name"] == "User 3"


@pytest.mark.django_db
def test_ops_leaderboard_serialization_error(mocker):
    """Ensure OPS leaderboard handles serialization errors gracefully."""
    mocker.patch("leaderboard.views.OpsLeaderboardSerializer", side_effect=Exception("Mocked error"))

    client = APIClient()
    response = client.get('/leaderboard/ops/')
    data = response.json()

    assert response.status_code == 500
    assert "error" in data


# ============== PITCHER LEADERBOARD TESTS ==============

@pytest.fixture
def setup_pitcher_test_data():
    """Fixture to set up test data for pitcher leaderboard"""
    category_pitchers = Category.objects.create(id=3, name="pitchers")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)
    user3 = CustomUser.objects.create(name="User 3", mbr_id=3)

    # Create pitchers
    pitchers = []
    for i in range(15):
        player = Player.objects.create(
            id=i+1,
            player_name=f"Pitcher {i+1}",
            player_type="pitcher",
            api_player_id=30000+i+1,
        )
        pitchers.append(player)
        Pitcher.objects.create(
            player_id=player.id,
            season=2026,
            wins=15 - i,
            losses=5 + i,
            era=2.50 + (i * 0.25),
            strikeouts=200 - (i * 10),
        )

    # Create some hitters for alternates tiebreaker
    hitters = []
    for i in range(5):
        player = Player.objects.create(
            id=100+i,
            player_name=f"Hitter {i+1}",
            player_type="hitter",
            api_player_id=40000+i+1,
        )
        hitters.append(player)
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.300 - (i * 0.01),
            ops=0.900,
            plate_appearances=600,
        )

    # User 1 picks (first 4 pitchers - highest wins)
    for i in range(4):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1, season=2026)

    # User 2 picks (next 4 pitchers - medium wins)
    for i in range(4, 8):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1, season=2026)

    # User 3 picks (last 4 pitchers - lowest wins)
    for i in range(8, 12):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1, season=2026)

    # Alternate picks for tiebreaker
    for i, user in enumerate([user1, user2, user3]):
        for j in range(3):
            Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name=hitters[j].player_name, is_alternate=True, pick_order=j+1, season=2026)

    return user1, user2, user3, pitchers


@pytest.mark.django_db
def test_pitcher_leaderboard(setup_pitcher_test_data):
    """
    Test the GET /leaderboard/pitchers/ endpoint returns properly ranked pitchers.
    """
    client = APIClient()
    response = client.get('/leaderboard/pitchers/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data) == 3

    # User 1 should be ranked first (highest total wins)
    assert data[0]["user_name"] == "User 1"
    assert data[0]["rank"] == 1
    assert data[0]["top_three_total_wins"] > data[1]["top_three_total_wins"]

    # Check tiebreaker fields are present
    assert "first_tiebreaker_wins" in data[0]
    assert "second_tiebreaker_win_pct" in data[0]
    assert "third_tiebreaker_era" in data[0]
    assert "fourth_tiebreaker_alt_avg" in data[0]


@pytest.mark.django_db
def test_pitcher_leaderboard_picks_sorted_by_wins(setup_pitcher_test_data):
    """Ensure pitcher picks are sorted by wins (highest first)."""
    client = APIClient()
    response = client.get('/leaderboard/pitchers/')
    data = response.json()

    for user_entry in data:
        picks = user_entry["pitcher_picks"]
        for i in range(len(picks) - 1):
            assert picks[i]["wins"] >= picks[i + 1]["wins"], "Picks should be sorted by wins descending"


# ============== RBI CHAMPION TESTS ==============

@pytest.fixture
def setup_rbi_test_data():
    """Fixture for RBI champion leaderboard tests"""
    category_rbi = Category.objects.create(id=5, name="rbi_champion")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)
    user3 = CustomUser.objects.create(name="User 3", mbr_id=3)

    # Create players with RBIs
    players = []
    for i in range(10):
        player = Player.objects.create(
            id=i+1,
            player_name=f"RBI Player {i+1}",
            player_type="hitter",
            api_player_id=50000+i+1,
        )
        players.append(player)
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.280,
            ops=0.850,
            plate_appearances=600,
            rbis=130 - (i * 5),
            stolen_bases=10,
        )

    # Create MLB leaders for RBI (the endpoint uses MlbLeader table)
    for i in range(3):
        MlbLeader.objects.create(
            season=2026,
            category='runsBattedIn',
            rank=i + 1,
            player_name=f"RBI Player {i+1}",
            team="NYY",
            value=130 - (i * 5),
            api_player_id=50000+i+1,
        )

    # User 1 picks correct player (RBI leader) with close prediction
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_rbi.id, player_name="RBI Player 1", pick_value=128, season=2026)

    # User 2 picks correct player with farther prediction
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_rbi.id, player_name="RBI Player 1", pick_value=140, season=2026)

    # User 3 picks wrong player
    Pick.objects.create(user_id=user3.mbr_id, category_id=category_rbi.id, player_name="RBI Player 2", pick_value=125, season=2026)

    # Alternates for tiebreaker
    for user in [user1, user2, user3]:
        Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name="RBI Player 3", is_alternate=True, season=2026)

    return user1, user2, user3, players


@pytest.mark.django_db
def test_rbi_champion_leaderboard(setup_rbi_test_data, monkeypatch):
    """Test RBI champion leaderboard correctly ranks users."""
    # Mock date to mid-season to get a prorated projection
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 7, 1)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/rbi-champion/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert "actual_rbi_leader" in data
    assert data["actual_rbi_leader"]["player_name"] == "RBI Player 1"
    assert data["actual_rbi_leader"]["rbis"] == 130

    leaderboard = data["leaderboard"]
    assert len(leaderboard) == 3

    # Ranking is by picked player's YTD RBIs (desc), then |deviation| (asc)
    # User 1 and User 2 both picked "RBI Player 1" who has 130 YTD RBIs
    # User 3 picked "RBI Player 2" who has 125 YTD RBIs
    # So User 1 & 2 rank above User 3 due to higher YTD RBIs

    # Among User 1 & 2 (same YTD RBIs), sort by |deviation| from prorated projection
    # At mid-season, prorated projection ≈ 293 RBIs (130 * 162/72)
    # User 1 predicted 128 -> deviation = 128 - 293 = -165 -> |165|
    # User 2 predicted 140 -> deviation = 140 - 293 = -153 -> |153|
    # User 2 has smaller |deviation|, so User 2 ranks first
    assert leaderboard[0]["user_name"] == "User 2"
    assert leaderboard[0]["predicted_player"] == "RBI Player 1"
    assert leaderboard[0]["predicted_rbis"] == 140
    assert leaderboard[0]["rank"] == 1

    assert leaderboard[1]["user_name"] == "User 1"
    assert leaderboard[1]["predicted_player"] == "RBI Player 1"
    assert leaderboard[1]["predicted_rbis"] == 128
    assert leaderboard[1]["rank"] == 2

    # User 3 picked a different player with lower YTD RBIs
    user3_entry = next(e for e in leaderboard if e["user_name"] == "User 3")
    assert user3_entry["predicted_player"] == "RBI Player 2"
    assert user3_entry["rank"] == 3


# ============== STOLEN BASE CHAMPION TESTS ==============

@pytest.fixture
def setup_sb_test_data():
    """Fixture for stolen base champion leaderboard tests"""
    category_sb = Category.objects.create(id=6, name="stolen_base_champion")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)

    # Create players with stolen bases
    for i in range(5):
        player = Player.objects.create(
            id=i+1,
            player_name=f"SB Player {i+1}",
            player_type="hitter",
            api_player_id=60000+i+1,
        )
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.280,
            ops=0.800,
            plate_appearances=600,
            rbis=50,
            stolen_bases=70 - (i * 10),
        )

    # Create MLB leaders for stolen bases (the endpoint uses MlbLeader table)
    for i in range(3):
        MlbLeader.objects.create(
            season=2026,
            category='stolenBases',
            rank=i + 1,
            player_name=f"SB Player {i+1}",
            team="NYY",
            value=70 - (i * 10),
            api_player_id=60000+i+1,
        )

    # User 1 picks correct player
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_sb.id, player_name="SB Player 1", pick_value=68, season=2026)

    # User 2 picks wrong player
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_sb.id, player_name="SB Player 2", pick_value=65, season=2026)

    return user1, user2


@pytest.mark.django_db
def test_stolen_base_leaderboard(setup_sb_test_data, monkeypatch):
    """Test stolen base champion leaderboard."""
    # Mock date to mid-season to get a prorated projection
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 7, 1)
    monkeypatch.setattr("leaderboard.views.date", MockDate)

    client = APIClient()
    response = client.get('/leaderboard/stolen-bases/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert data["actual_sb_leader"]["player_name"] == "SB Player 1"
    assert data["actual_sb_leader"]["stolen_bases"] == 70

    leaderboard = data["leaderboard"]

    # Ranking is by picked player's YTD SBs (desc), then |deviation| (asc)
    # User 1 picked "SB Player 1" who has 70 YTD SBs
    # User 2 picked "SB Player 2" who has 60 YTD SBs
    # So User 1 ranks above User 2 due to higher YTD SBs

    user1 = next(e for e in leaderboard if e["user_name"] == "User 1")
    assert user1["predicted_player"] == "SB Player 1"
    assert user1["predicted_stolen_bases"] == 68
    assert user1["rank"] == 1

    user2 = next(e for e in leaderboard if e["user_name"] == "User 2")
    assert user2["predicted_player"] == "SB Player 2"
    assert user2["rank"] == 2


# ============== DIMAGGIO TESTS ==============

@pytest.fixture
def setup_dimaggio_test_data():
    """Fixture for DiMaggio prize leaderboard tests"""
    category_dimaggio = Category.objects.create(id=7, name="dimaggio")
    category_alternates = Category.objects.create(id=2, name="batters_alternates")

    user1 = CustomUser.objects.create(name="User 1", mbr_id=1)
    user2 = CustomUser.objects.create(name="User 2", mbr_id=2)

    # Create some hitters for alternates
    for i in range(3):
        player = Player.objects.create(
            id=i+1,
            player_name=f"Alt Player {i+1}",
            player_type="hitter",
            api_player_id=70000+i+1,
        )
        Hitter.objects.create(
            player_id=player.id,
            season=2026,
            average=0.300 - (i * 0.01),
            ops=0.850,
            plate_appearances=600,
        )

    # DiMaggio picks (just predicted streak, no player)
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_dimaggio.id, player_name="", pick_value=25, season=2026)
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_dimaggio.id, player_name="", pick_value=30, season=2026)

    # Alternates for tiebreaker
    for user in [user1, user2]:
        Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name="Alt Player 1", is_alternate=True, season=2026)

    return user1, user2


@pytest.mark.django_db
def test_dimaggio_leaderboard(setup_dimaggio_test_data):
    """Test DiMaggio prize leaderboard returns all predictions."""
    client = APIClient()
    response = client.get('/leaderboard/dimaggio/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    # actual_longest_streak is None until set
    assert data["actual_longest_streak"] is None

    leaderboard = data["leaderboard"]
    assert len(leaderboard) == 2

    # Since actual streak is None, no one has an exact match
    for entry in leaderboard:
        assert entry["is_exact_match"] is False
        assert entry["rank"] is None


# ============== USER LIST TESTS ==============

@pytest.mark.django_db
def test_user_list():
    """Test the GET /leaderboard/users/ endpoint."""
    CustomUser.objects.create(name="Test User 1", mbr_id=101)
    CustomUser.objects.create(name="Test User 2", mbr_id=102)

    client = APIClient()
    response = client.get('/leaderboard/users/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data) == 2
    assert data[0]["name"] == "Test User 1"
    assert data[0]["mbr_id"] == 101


# ============== CATEGORY LIST TESTS ==============

@pytest.mark.django_db
def test_category_list():
    """Test the GET /leaderboard/categories/ endpoint."""
    Category.objects.create(id=1, name="batters")
    Category.objects.create(id=3, name="pitchers")
    Category.objects.create(id=4, name="home_run_hitters")

    client = APIClient()
    response = client.get('/leaderboard/categories/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data) == 3

    # Check that display_name and description are included
    batters = next(c for c in data if c["name"] == "batters")
    assert batters["display_name"] == "Batting Average"
    assert batters["picks_per_user"] == 10

    pitchers = next(c for c in data if c["name"] == "pitchers")
    assert pitchers["display_name"] == "Pitching Victories"
    assert pitchers["picks_per_user"] == 4


# ============== HELPER FUNCTION UNIT TESTS ==============

class TestGetSeasonFromRequest:
    """Tests for get_season_from_request helper."""

    def test_returns_default_when_no_param(self):
        """Test returns default season when no parameter provided."""
        request = MagicMock()
        request.GET.get.return_value = None
        assert get_season_from_request(request) == DEFAULT_SEASON

    def test_returns_integer_when_valid_string(self):
        """Test returns integer when valid season string provided."""
        request = MagicMock()
        request.GET.get.return_value = "2024"
        assert get_season_from_request(request) == 2024

    def test_returns_default_when_invalid_string(self):
        """Test returns default for invalid (non-numeric) string."""
        request = MagicMock()
        request.GET.get.return_value = "not-a-year"
        assert get_season_from_request(request) == DEFAULT_SEASON

    def test_returns_default_when_empty_string(self):
        """Test returns default for empty string."""
        request = MagicMock()
        request.GET.get.return_value = ""
        assert get_season_from_request(request) == DEFAULT_SEASON

    def test_handles_float_string(self):
        """Test handles float string (should fail int conversion)."""
        request = MagicMock()
        request.GET.get.return_value = "2024.5"
        assert get_season_from_request(request) == DEFAULT_SEASON


class TestNormalizeName:
    """Tests for normalize_name helper."""

    def test_removes_accents(self):
        """Test removes accented characters."""
        assert normalize_name("José") == "jose"
        assert normalize_name("Ramírez") == "ramirez"
        assert normalize_name("Albíes") == "albies"
        assert normalize_name("González") == "gonzalez"

    def test_removes_multiple_accents(self):
        """Test removes multiple accents in a name."""
        assert normalize_name("José Ramírez") == "jose ramirez"
        assert normalize_name("Adrián Beltré") == "adrian beltre"

    def test_lowercases_result(self):
        """Test lowercases the result."""
        assert normalize_name("Mike Trout") == "mike trout"
        assert normalize_name("SHOHEI OHTANI") == "shohei ohtani"

    def test_preserves_spaces(self):
        """Test preserves spaces in names."""
        assert normalize_name("Bo Bichette") == "bo bichette"

    def test_handles_empty_string(self):
        """Test handles empty string."""
        assert normalize_name("") == ""

    def test_handles_none(self):
        """Test handles None input."""
        assert normalize_name(None) == ""

    def test_handles_special_characters(self):
        """Test handles special characters like apostrophes."""
        assert normalize_name("O'Neill") == "o'neill"
        assert normalize_name("De La Cruz") == "de la cruz"


class TestGetSeasonProgress:
    """Tests for get_season_progress helper."""

    def test_returns_zero_before_season(self, monkeypatch):
        """Test returns 0 games before season start."""
        class MockDate(date):
            @classmethod
            def today(cls):
                return date(2026, 3, 1)
        monkeypatch.setattr("leaderboard.views.date", MockDate)

        games, total, ratio = get_season_progress()
        assert games == 0
        assert total == 162
        assert ratio == 0.0

    def test_calculates_correctly_opening_day(self, monkeypatch):
        """Test returns 0 on opening day (no games played yet)."""
        class MockDate(date):
            @classmethod
            def today(cls):
                return date(2026, 3, 27)
        monkeypatch.setattr("leaderboard.views.date", MockDate)

        games, total, ratio = get_season_progress()
        assert games == 0
        assert total == 162

    def test_calculates_correctly_mid_season(self, monkeypatch):
        """Test calculates correctly during mid-season."""
        class MockDate(date):
            @classmethod
            def today(cls):
                return date(2026, 7, 1)  # ~96 days after 3/27
        monkeypatch.setattr("leaderboard.views.date", MockDate)

        games, total, ratio = get_season_progress()
        # 96 days * 0.75 = 72 games
        assert games == 72
        assert total == 162
        assert ratio == pytest.approx(72 / 162, rel=0.01)

    def test_caps_at_162_games(self, monkeypatch):
        """Test caps at 162 games past end of season."""
        class MockDate(date):
            @classmethod
            def today(cls):
                return date(2026, 12, 1)
        monkeypatch.setattr("leaderboard.views.date", MockDate)

        games, total, ratio = get_season_progress()
        assert games == 162
        assert ratio == 1.0


class TestCalculateProratedProjection:
    """Tests for calculate_prorated_projection helper."""

    def test_projects_correctly_half_season(self):
        """Test projection at half season."""
        # 50 stats in 81 games -> 100 projected for 162
        projection = calculate_prorated_projection(50, 81, 162)
        assert projection == pytest.approx(100, rel=0.01)

    def test_projects_correctly_quarter_season(self):
        """Test projection at quarter season."""
        # 25 stats in 40 games -> ~101 projected
        projection = calculate_prorated_projection(25, 40, 162)
        assert projection == pytest.approx(101.25, rel=0.01)

    def test_returns_ytd_value_when_zero_games(self):
        """Test returns YTD value when 0 games played."""
        projection = calculate_prorated_projection(50, 0, 162)
        assert projection == 50

    def test_returns_ytd_value_when_negative_games(self):
        """Test returns YTD value when negative games (edge case)."""
        projection = calculate_prorated_projection(50, -1, 162)
        assert projection == 50

    def test_handles_zero_ytd_value(self):
        """Test handles zero YTD value."""
        projection = calculate_prorated_projection(0, 81, 162)
        assert projection == 0

    def test_handles_full_season(self):
        """Test handles full season (no projection needed)."""
        projection = calculate_prorated_projection(100, 162, 162)
        assert projection == pytest.approx(100, rel=0.01)


@pytest.mark.django_db
class TestBuildHitterStatsLookup:
    """Tests for build_hitter_stats_lookup helper."""

    def test_returns_dict_keyed_by_player_name(self):
        """Test returns dict keyed by player name."""
        player = Player.objects.create(player_name="Test Hitter", player_type="hitter", api_player_id=1)
        Hitter.objects.create(player=player, season=2025, average=0.300, ops=0.900, plate_appearances=500)

        lookup = build_hitter_stats_lookup(2025)
        assert "Test Hitter" in lookup
        assert lookup["Test Hitter"].average == 0.300

    def test_returns_empty_dict_for_nonexistent_season(self):
        """Test returns empty dict for season with no data."""
        lookup = build_hitter_stats_lookup(1900)
        assert lookup == {}

    def test_includes_multiple_players(self):
        """Test includes all players for the season."""
        for i in range(3):
            player = Player.objects.create(player_name=f"Hitter {i}", player_type="hitter", api_player_id=100 + i)
            Hitter.objects.create(player=player, season=2025, average=0.300 - (i * 0.01))

        lookup = build_hitter_stats_lookup(2025)
        assert len(lookup) == 3
        assert "Hitter 0" in lookup
        assert "Hitter 1" in lookup
        assert "Hitter 2" in lookup


@pytest.mark.django_db
class TestBuildPitcherStatsLookup:
    """Tests for build_pitcher_stats_lookup helper."""

    def test_returns_dict_keyed_by_player_name(self):
        """Test returns dict keyed by player name."""
        player = Player.objects.create(player_name="Test Pitcher", player_type="pitcher", api_player_id=1)
        Pitcher.objects.create(player=player, season=2025, wins=15, losses=5, era=3.00)

        lookup = build_pitcher_stats_lookup(2025)
        assert "Test Pitcher" in lookup
        assert lookup["Test Pitcher"].wins == 15

    def test_returns_empty_dict_for_nonexistent_season(self):
        """Test returns empty dict for season with no data."""
        lookup = build_pitcher_stats_lookup(1900)
        assert lookup == {}


@pytest.mark.django_db
class TestGetUsersWithPicks:
    """Tests for get_users_with_picks helper."""

    def test_returns_only_users_with_picks(self):
        """Test returns only users who have picks for the season."""
        user1 = CustomUser.objects.create(name="Has Picks", mbr_id=1)
        CustomUser.objects.create(name="No Picks", mbr_id=2)
        category = Category.objects.create(name="test")

        Pick.objects.create(user=user1, category=category, player_name="Test", season=2025)

        users = get_users_with_picks(2025)
        assert len(users) == 1
        assert users[0].name == "Has Picks"

    def test_returns_empty_list_when_no_picks(self):
        """Test returns empty list when no picks exist."""
        CustomUser.objects.create(name="User", mbr_id=1)
        users = get_users_with_picks(2025)
        assert users == []

    def test_filters_by_season(self):
        """Test filters picks by season correctly."""
        user = CustomUser.objects.create(name="User", mbr_id=1)
        category = Category.objects.create(name="test")

        Pick.objects.create(user=user, category=category, player_name="Test", season=2024)

        # User has picks for 2024, not 2025
        users_2025 = get_users_with_picks(2025)
        users_2024 = get_users_with_picks(2024)

        assert len(users_2025) == 0
        assert len(users_2024) == 1


@pytest.mark.django_db
class TestGetQualifiedAlternatesAverage:
    """Tests for get_qualified_alternates_average helper."""

    def test_calculates_average_of_qualified_alternates(self):
        """Test calculates average correctly for qualified alternates."""
        user = CustomUser.objects.create(name="Test", mbr_id=1)
        category = Category.objects.create(id=2, name="alternates")

        # Create qualified alternates
        for i in range(3):
            player = Player.objects.create(player_name=f"Alt {i}", player_type="hitter", api_player_id=100 + i)
            Hitter.objects.create(player=player, season=2025, average=0.300 - (i * 0.01), plate_appearances=600)
            Pick.objects.create(user=user, category=category, player_name=f"Alt {i}", is_alternate=True, season=2025)

        hitter_stats = build_hitter_stats_lookup(2025)
        avg = get_qualified_alternates_average(user.mbr_id, 500, hitter_stats, 2025)

        # Average of 0.300, 0.290, 0.280 = 0.29
        assert avg == pytest.approx(0.29, rel=0.01)

    def test_returns_zero_when_no_qualified_alternates(self):
        """Test returns 0 when no alternates qualify."""
        user = CustomUser.objects.create(name="Test", mbr_id=1)
        hitter_stats = {}

        avg = get_qualified_alternates_average(user.mbr_id, 500, hitter_stats, 2025)
        assert avg == 0

    def test_excludes_unqualified_alternates(self):
        """Test excludes alternates below PA threshold."""
        user = CustomUser.objects.create(name="Test", mbr_id=1)
        category = Category.objects.create(id=2, name="alternates")

        # Create one qualified and one unqualified alternate
        player1 = Player.objects.create(player_name="Qualified", player_type="hitter", api_player_id=1)
        Hitter.objects.create(player=player1, season=2025, average=0.300, plate_appearances=600)
        Pick.objects.create(user=user, category=category, player_name="Qualified", is_alternate=True, season=2025)

        player2 = Player.objects.create(player_name="Unqualified", player_type="hitter", api_player_id=2)
        Hitter.objects.create(player=player2, season=2025, average=0.350, plate_appearances=100)
        Pick.objects.create(user=user, category=category, player_name="Unqualified", is_alternate=True, season=2025)

        hitter_stats = build_hitter_stats_lookup(2025)
        avg = get_qualified_alternates_average(user.mbr_id, 500, hitter_stats, 2025)

        # Only qualified player (0.300) should be included
        assert avg == pytest.approx(0.300, rel=0.01)


# ============== MLB LEADERS ENDPOINT TESTS ==============

@pytest.fixture
def setup_mlb_leaders_data():
    """Fixture for MLB leaders tests."""
    for i in range(5):
        MlbLeader.objects.create(
            season=2025,
            category='battingAverage',
            rank=i + 1,
            player_name=f"BA Leader {i + 1}",
            team="NYY",
            value=0.350 - (i * 0.01),
            api_player_id=10000 + i
        )
        MlbLeader.objects.create(
            season=2025,
            category='homeRuns',
            rank=i + 1,
            player_name=f"HR Leader {i + 1}",
            team="LAD",
            value=50 - (i * 2),
            api_player_id=20000 + i
        )
        MlbLeader.objects.create(
            season=2025,
            category='wins',
            rank=i + 1,
            player_name=f"Wins Leader {i + 1}",
            team="ATL",
            value=20 - i,
            api_player_id=30000 + i
        )
        MlbLeader.objects.create(
            season=2025,
            category='onBasePlusSlugging',
            rank=i + 1,
            player_name=f"OPS Leader {i + 1}",
            team="HOU",
            value=1.050 - (i * 0.02),
            api_player_id=40000 + i
        )
        MlbLeader.objects.create(
            season=2025,
            category='runsBattedIn',
            rank=i + 1,
            player_name=f"RBI Leader {i + 1}",
            team="PHI",
            value=130 - (i * 5),
            api_player_id=50000 + i
        )
        MlbLeader.objects.create(
            season=2025,
            category='stolenBases',
            rank=i + 1,
            player_name=f"SB Leader {i + 1}",
            team="MIA",
            value=70 - (i * 5),
            api_player_id=60000 + i
        )


@pytest.mark.django_db
def test_batters_mlb_leaders(setup_mlb_leaders_data):
    """Test batters MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/batters/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert 'leaders' in data
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['rank'] == 1
    assert data['leaders'][0]['player_name'] == "BA Leader 1"


@pytest.mark.django_db
def test_homeruns_mlb_leaders(setup_mlb_leaders_data):
    """Test home runs MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/homeruns/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['value'] == 50


@pytest.mark.django_db
def test_ops_mlb_leaders(setup_mlb_leaders_data):
    """Test OPS MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/ops/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['player_name'] == "OPS Leader 1"


@pytest.mark.django_db
def test_pitchers_mlb_leaders(setup_mlb_leaders_data):
    """Test pitchers MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/pitchers/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['player_name'] == "Wins Leader 1"


@pytest.mark.django_db
def test_rbi_mlb_leaders(setup_mlb_leaders_data):
    """Test RBI MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/rbi-champion/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['value'] == 130


@pytest.mark.django_db
def test_stolen_bases_mlb_leaders(setup_mlb_leaders_data):
    """Test stolen bases MLB leaders endpoint."""
    client = APIClient()
    response = client.get('/leaderboard/stolen-bases/mlb-leaders/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(data['leaders']) == 5
    assert data['leaders'][0]['value'] == 70


@pytest.mark.django_db
def test_mlb_leaders_empty_season():
    """Test MLB leaders returns empty for nonexistent season."""
    client = APIClient()
    response = client.get('/leaderboard/batters/mlb-leaders/?season=1900')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert data['leaders'] == []


@pytest.mark.django_db
def test_mlb_leaders_headshot_url(setup_mlb_leaders_data):
    """Test MLB leaders include headshot_url."""
    client = APIClient()
    response = client.get('/leaderboard/batters/mlb-leaders/?season=2025')
    data = response.json()

    assert data['leaders'][0]['headshot_url'] is not None
    assert "10000" in data['leaders'][0]['headshot_url']


# ============== EDGE CASE TESTS ==============

@pytest.mark.django_db
def test_batters_leaderboard_no_users():
    """Test batters leaderboard with no users."""
    client = APIClient()
    response = client.get('/leaderboard/batters/')
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.django_db
def test_ops_leaderboard_no_users():
    """Test OPS leaderboard with no users."""
    client = APIClient()
    response = client.get('/leaderboard/ops/')
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.django_db
def test_pitchers_leaderboard_no_users():
    """Test pitchers leaderboard with no users."""
    client = APIClient()
    response = client.get('/leaderboard/pitchers/')
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.django_db
def test_homeruns_leaderboard_no_users():
    """Test homeruns leaderboard with no users."""
    client = APIClient()
    response = client.get('/leaderboard/homeruns/')
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


@pytest.mark.django_db
def test_batters_endpoint_with_explicit_season():
    """Test batters endpoint accepts explicit season parameter."""
    Category.objects.create(id=1, name="batters")
    Category.objects.create(id=2, name="alternates")

    client = APIClient()
    response = client.get('/leaderboard/batters/?season=2025')
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_batters_endpoint_invalid_season_uses_default():
    """Test invalid season parameter uses default."""
    client = APIClient()
    response = client.get('/leaderboard/batters/?season=abc')
    assert response.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_dimaggio_with_actual_streak_set():
    """Test DiMaggio leaderboard when actual streak is set."""
    category_dimaggio = Category.objects.create(id=7, name="dimaggio")
    Category.objects.create(id=2, name="alternates")

    # Set actual streak
    SeasonStats.objects.create(year=2025, longest_hitting_streak=25, streak_holder_name="Hot Hitter")

    user1 = CustomUser.objects.create(name="Exact Match", mbr_id=1)
    user2 = CustomUser.objects.create(name="Wrong Guess", mbr_id=2)

    Pick.objects.create(user=user1, category=category_dimaggio, player_name="", pick_value=25, season=2025)
    Pick.objects.create(user=user2, category=category_dimaggio, player_name="", pick_value=30, season=2025)

    client = APIClient()
    response = client.get('/leaderboard/dimaggio/?season=2025')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert data['actual_longest_streak'] == 25
    assert data['streak_holder_name'] == "Hot Hitter"

    # User with exact match should be ranked
    exact_match_user = next(e for e in data['leaderboard'] if e['user_name'] == "Exact Match")
    assert exact_match_user['is_exact_match'] is True
    assert exact_match_user['rank'] == 1

    # User with wrong guess should not be ranked
    wrong_user = next(e for e in data['leaderboard'] if e['user_name'] == "Wrong Guess")
    assert wrong_user['is_exact_match'] is False
    assert wrong_user['rank'] is None


@pytest.mark.django_db
def test_rbi_leaderboard_empty():
    """Test RBI leaderboard with no data returns proper structure."""
    client = APIClient()
    response = client.get('/leaderboard/rbi-champion/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert 'actual_rbi_leader' in data
    assert 'leaderboard' in data


@pytest.mark.django_db
def test_stolen_bases_leaderboard_empty():
    """Test stolen bases leaderboard with no data returns proper structure."""
    client = APIClient()
    response = client.get('/leaderboard/stolen-bases/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert 'actual_sb_leader' in data
    assert 'leaderboard' in data