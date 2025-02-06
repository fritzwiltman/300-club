import pytest
from django.db.utils import IntegrityError
from leaderboard.models import Category, CustomUser, Player, Hitter, Pitcher, Pick

@pytest.mark.django_db
def test_category_str():
    category = Category.objects.create(name="Test Category")
    assert str(category) == "Test Category"

@pytest.mark.django_db
def test_custom_user_str():
    user = CustomUser.objects.create(name="Test User", mbr_id=123)
    assert str(user) == "Test User"

@pytest.mark.django_db
def test_player_str():
    player = Player.objects.create(player_name="Test Player", player_type="hitter", api_player_id=1)
    assert str(player) == "Test Player"

@pytest.mark.django_db
def test_hitter_str():
    player = Player.objects.create(player_name="Test Player", player_type="hitter", api_player_id=1)
    hitter = Hitter.objects.create(player_id=player.id, average=0.300, ops=0.900, plate_appearances=100, home_runs=10, rbis=20, stolen_bases=5)
    assert str(hitter) == "Hitting stats for Test Player"

@pytest.mark.django_db
def test_pitcher_str():
    player = Player.objects.create(player_name="Test Player", player_type="pitcher", api_player_id=1)
    pitcher = Pitcher.objects.create(player_id=player.id, wins=10, losses=5, era=3.50, strikeouts=100)
    assert str(pitcher) == "Pitching stats for Test Player"

@pytest.mark.django_db
def test_pick_str():
    user = CustomUser.objects.create(name="Test User", mbr_id=123)
    category = Category.objects.create(name="Test Category")
    pick = Pick.objects.create(user_id=user.mbr_id, category_id=category.id, player_name="Test Player", is_alternate=False, pick_order=1, pick_value=100)
    assert str(pick) == "Test User - Test Category - Test Player"

@pytest.mark.django_db
def test_unique_mbr_id():
    CustomUser.objects.create(name="Test User 1", mbr_id=123)
    with pytest.raises(IntegrityError):
        CustomUser.objects.create(name="Test User 2", mbr_id=123)

@pytest.mark.django_db
def test_unique_api_player_id():
    Player.objects.create(player_name="Test Player 1", player_type="hitter", api_player_id=1)
    with pytest.raises(IntegrityError):
        Player.objects.create(player_name="Test Player 2", player_type="pitcher", api_player_id=1)