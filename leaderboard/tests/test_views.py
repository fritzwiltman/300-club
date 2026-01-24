# leaderboard/tests/test_views.py
import pytest
from datetime import date, datetime
from rest_framework.test import APIClient
from rest_framework import status
from leaderboard.models import CustomUser, Pick, Player, Hitter, Pitcher, Category
from leaderboard.views import calculate_pro_rated_plate_appearances
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
    category_alternates = Category.objects.create(id=2, name="batters_alternates")  # 🔹 Ensure category 2 exists
   
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
    
    # Create hitters with various stats
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
        # plate_appearances = 600 if i < 33 else 450  # 7 players meet threshold, 8 do not
        Hitter.objects.create(
            player_id=player.id,
            average=0.300 + (i * .002), # Slightly increasing averages
            ops=1.000 - (i * 0.002), # Slightly decreasing OPS
            plate_appearances=plate_appearances,
        )

    # User 1 Picks - 10 Regular, 5 Alternates
    for i in range(10):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_batters.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1)

    for i in range(10, 15):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    # User 2 Picks (10 Different Regulars)
    for i in range(10):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_batters.id, player_name=players[i+10].player_name, is_alternate=False, pick_order=i+1)

    for i in range(10, 15):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    # User 3 Picks (10 Different Regulars, 7 of which are disqualified)
    for i in range(10):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_batters.id, player_name=players[i+25].player_name, is_alternate=False, pick_order=i+1)

    for i in range(35, 40):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)
  
    return user1, user2, user3, players


@pytest.mark.django_db
def test_hitter_leaderboard_with_disqualified_alternate(setup_hitters_test_data):
    """Ensure that at least one alternate pick is disqualified due to low plate appearances."""
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
def test_hitter_leaderboard(setup_hitters_test_data):
    """
    Tests that hitter leaderboard correctly calculates rankings and includes disqualified users.
    """
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
def test_all_disqualified_users_are_present(setup_hitters_test_data):
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
    
    # Ensure User 3 has 0 qualified batters
    assert len(user3_entry["qualified_picks"]) == 8


@pytest.mark.django_db
def test_disqualified_users_have_picks_displayed(setup_hitters_test_data):
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
    test_cases = [
        (date(2024, 3, 28), 502 / 27),  # Opening Day (Week 1)
        (date(2024, 4, 28), 502 / 27 * 5),  # Week 4
        (date(2024, 9, 28), 502),  # End of Season
    ]

    for test_date, expected in test_cases:
        # Mock the date class itself, not just today()
        class MockDate(date):
            @classmethod
            def today(cls):
                return test_date

        monkeypatch.setattr("leaderboard.views.date", MockDate)  # ✅ Mocking date class
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
    category_alternates = Category.objects.create(id=2, name="batters_alternates")  # Ensuring alternates category exists

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
            average=0.280 + (i * 0.001),  # Slightly increasing averages
            ops=0.900 - (i * 0.005),  # Slightly decreasing OPS
            plate_appearances=600,  # All are qualified
            home_runs=20 + (i * 2)  # Increasing home runs for sorting
        )

    # User 1 Picks (4 Regulars)
    for i in range(4):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1)

    # User 2 Picks (4 Different Regulars)
    for i in range(4):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1)

    # User 3 Picks (4 picks)
    for i in range(8, 12):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_homeruns.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1)

    # Alternate Picks for User 1 (For second tiebreaker)
    for i in range(15, 18):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    # Alternate Picks for User 2 (For second tiebreaker)
    for i in range(16, 19):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    # Alternate Picks for User 3 (For second tiebreaker)
    for i in range(17, 20):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

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
def test_ops_leaderboard(setup_hitters_test_data):
    """
    Test the GET /leaderboard/ops/ endpoint returns a properly ranked OPS leaderboard.
    """
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
            wins=15 - i,  # Decreasing wins
            losses=5 + i,  # Increasing losses
            era=2.50 + (i * 0.25),  # Increasing ERA
            strikeouts=200 - (i * 10),  # Decreasing strikeouts
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
            average=0.300 - (i * 0.01),
            ops=0.900,
            plate_appearances=600,
        )

    # User 1 picks (first 4 pitchers - highest wins)
    for i in range(4):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1)

    # User 2 picks (next 4 pitchers - medium wins)
    for i in range(4, 8):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1)

    # User 3 picks (last 4 pitchers - lowest wins)
    for i in range(8, 12):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_pitchers.id, player_name=pitchers[i].player_name, pick_order=i+1)

    # Alternate picks for tiebreaker
    for i, user in enumerate([user1, user2, user3]):
        for j in range(3):
            Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name=hitters[j].player_name, is_alternate=True, pick_order=j+1)

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
            average=0.280,
            ops=0.850,
            plate_appearances=600,
            rbis=130 - (i * 5),  # Decreasing RBIs
            stolen_bases=10,
        )

    # User 1 picks correct player (RBI leader) with close prediction
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_rbi.id, player_name="RBI Player 1", pick_value=128)

    # User 2 picks correct player with farther prediction
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_rbi.id, player_name="RBI Player 1", pick_value=140)

    # User 3 picks wrong player
    Pick.objects.create(user_id=user3.mbr_id, category_id=category_rbi.id, player_name="RBI Player 2", pick_value=125)

    # Alternates for tiebreaker
    for user in [user1, user2, user3]:
        Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name="RBI Player 3", is_alternate=True)

    return user1, user2, user3, players


@pytest.mark.django_db
def test_rbi_champion_leaderboard(setup_rbi_test_data):
    """Test RBI champion leaderboard correctly ranks users."""
    client = APIClient()
    response = client.get('/leaderboard/rbi-champion/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert "actual_rbi_leader" in data
    assert data["actual_rbi_leader"]["player_name"] == "RBI Player 1"
    assert data["actual_rbi_leader"]["rbis"] == 130

    leaderboard = data["leaderboard"]
    assert len(leaderboard) == 3

    # User 1 should be first (correct player, closest prediction: |130-128|=2)
    assert leaderboard[0]["user_name"] == "User 1"
    assert leaderboard[0]["predicted_correct_player"] is True
    assert leaderboard[0]["rbi_difference"] == 2
    assert leaderboard[0]["rank"] == 1

    # User 2 should be second (correct player, farther prediction: |130-140|=10)
    assert leaderboard[1]["user_name"] == "User 2"
    assert leaderboard[1]["predicted_correct_player"] is True
    assert leaderboard[1]["rbi_difference"] == 10
    assert leaderboard[1]["rank"] == 2

    # User 3 picked wrong player - no rank
    user3_entry = next(e for e in leaderboard if e["user_name"] == "User 3")
    assert user3_entry["predicted_correct_player"] is False
    assert user3_entry["rank"] is None


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
            average=0.280,
            ops=0.800,
            plate_appearances=600,
            rbis=50,
            stolen_bases=70 - (i * 10),  # Decreasing SBs
        )

    # User 1 picks correct player
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_sb.id, player_name="SB Player 1", pick_value=68)

    # User 2 picks wrong player
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_sb.id, player_name="SB Player 2", pick_value=65)

    return user1, user2


@pytest.mark.django_db
def test_stolen_base_leaderboard(setup_sb_test_data):
    """Test stolen base champion leaderboard."""
    client = APIClient()
    response = client.get('/leaderboard/stolen-bases/')
    data = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert data["actual_sb_leader"]["player_name"] == "SB Player 1"
    assert data["actual_sb_leader"]["stolen_bases"] == 70

    leaderboard = data["leaderboard"]
    # User 1 picked correctly
    user1 = next(e for e in leaderboard if e["user_name"] == "User 1")
    assert user1["predicted_correct_player"] is True
    assert user1["sb_difference"] == 2  # |70-68|
    assert user1["rank"] == 1

    # User 2 picked wrong player
    user2 = next(e for e in leaderboard if e["user_name"] == "User 2")
    assert user2["predicted_correct_player"] is False
    assert user2["rank"] is None


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
            average=0.300 - (i * 0.01),
            ops=0.850,
            plate_appearances=600,
        )

    # DiMaggio picks (just predicted streak, no player)
    Pick.objects.create(user_id=user1.mbr_id, category_id=category_dimaggio.id, player_name="", pick_value=25)
    Pick.objects.create(user_id=user2.mbr_id, category_id=category_dimaggio.id, player_name="", pick_value=30)

    # Alternates for tiebreaker
    for user in [user1, user2]:
        Pick.objects.create(user_id=user.mbr_id, category_id=category_alternates.id, player_name="Alt Player 1", is_alternate=True)

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