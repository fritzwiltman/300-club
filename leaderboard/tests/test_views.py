# leaderboard/tests/test_views.py
import pytest
from datetime import date, datetime
from rest_framework.test import APIClient
from rest_framework import status
from leaderboard.models import CustomUser, Pick, Player, Hitter, Category
from leaderboard.views import calculate_pro_rated_plate_appearances

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
        plate_appearances = 600 if i < 33 else 450  # 7 players meet threshold, 8 do not
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
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_batters.id, player_name=players[i+30].player_name, is_alternate=False, pick_order=i+1)

    for i in range(20, 25):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category_alternates.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)
  
    return user1, user2, user3, players

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