# leaderboard/tests/test_views.py
import pytest
from datetime import date, datetime
from rest_framework.test import APIClient
from rest_framework import status
from leaderboard.models import CustomUser, Pick, Player, Hitter, Category
from leaderboard.views import calculate_pro_rated_plate_appearances

# @pytest.mark.django_db
# def test_player_list_view():
#     """
#     Test the GET /leaderboard/players/ endpoint returns a list of players.
#     """
#     # Setup: create sample Player objects
#     Player.objects.create(player_name="Test Player 1", player_type="hitter", api_player_id=1)
#     Player.objects.create(player_name="Test Player 2", player_type="pitcher", api_player_id=2)

#     client = APIClient()
#     response = client.get('/leaderboard/players/')
    
#     assert response.status_code == status.HTTP_200_OK, response.content
#     data = response.json()
#     assert len(data) == 2
#     assert data[0]["player_name"] == "Test Player 1"
#     assert data[1]["player_name"] == "Test Player 2"

@pytest.fixture
def setup_hitters_test_data():
    """Fixture to set up test data before each test for hitters leaderboard"""
    category = Category.objects.create(id=1, name="batters")  # Added this line

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
        plate_appearances = 600 if i < 35 else 450  # 10 players meet threshold, 5 do not
        Hitter.objects.create(
            player_id=player.id,
            average=0.300 + (i * .002), # Slightly increasing averages
            ops=1.000 - (i * 0.002), # Slightly decreasing OPS
            plate_appearances=plate_appearances,
        )

    # User 1 Picks - 10 Regular, 5 Alternates
    for i in range(10):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category.id, player_name=players[i].player_name, is_alternate=False, pick_order=i+1)

    for i in range(10, 15):
        Pick.objects.create(user_id=user1.mbr_id, category_id=category.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    # User 2 Picks (10 Different Regulars)
    for i in range(10):
        Pick.objects.create(user_id=user2.mbr_id, category_id=1, player_name=players[i+10].player_name, is_alternate=False, pick_order=i+1)

    for i in range(10, 15):
        Pick.objects.create(user_id=user2.mbr_id, category_id=category.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

# User 3 Picks (10 Different Regulars)
    for i in range(20):
        Pick.objects.create(user_id=user3.mbr_id, category_id=1, player_name=players[i+10].player_name, is_alternate=False, pick_order=i+1)

    for i in range(20, 25):
        Pick.objects.create(user_id=user3.mbr_id, category_id=category.id, player_name=players[i].player_name, is_alternate=True, pick_order=i+1)

    return user1, user2, user3, players

@pytest.mark.django_db
def test_hitter_leaderboard(setup_hitters_test_data):
    client = APIClient()
    response = client.get('/leaderboard/batters/')
    data = response.json()

    # User 2 should be ranked 1 because they have a higher average
    assert response.status_code == 200
    assert len(data) == 2  # Only 2 users qualify
    assert data[0]["user_name"] == "User 2"
    assert data[0]["aggregate_average"] == pytest.approx(0.339, rel=1e-2) 

@pytest.mark.django_db
def test_pro_rated_plate_appearances(monkeypatch):
    """Test dynamic plate appearances calculation based on date."""
    test_cases = [
        (date(2024, 3, 28), 502 / 27),  # Opening Day (Week 1)
        (date(2024, 4, 28), 502 / 27 * 5),  # Week 4
        (date(2024, 9, 28), 502),  # End of Season
    ]

    for test_date, expected in test_cases:
        # ✅ Mock the date class itself, not just today()
        class MockDate(date):
            @classmethod
            def today(cls):
                return test_date

        monkeypatch.setattr("leaderboard.views.date", MockDate)  # ✅ Mocking date class
        result = calculate_pro_rated_plate_appearances()
        
        assert result == pytest.approx(expected, rel=1e-2), f"Failed for {test_date}"