# leaderboard/tests/test_views.py
import pytest
from datetime import date, datetime
from rest_framework.test import APIClient
from rest_framework import status
from leaderboard.models import CustomUser, Pick, Player, Hitter, Category
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