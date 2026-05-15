import pytest
from django.db.utils import IntegrityError
from leaderboard.models import Category, CustomUser, Player, Hitter, Pitcher, Pick, SeasonStats, MlbLeader

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
    hitter = Hitter.objects.create(player_id=player.id, average=0.300, ops=0.900, plate_appearances=100, home_runs=10, rbis=20, stolen_bases=5, season=2025)
    assert str(hitter) == "Hitting stats for Test Player (2025)"

@pytest.mark.django_db
def test_pitcher_str():
    player = Player.objects.create(player_name="Test Player", player_type="pitcher", api_player_id=1)
    pitcher = Pitcher.objects.create(player_id=player.id, wins=10, losses=5, era=3.50, strikeouts=100, season=2025)
    assert str(pitcher) == "Pitching stats for Test Player (2025)"

@pytest.mark.django_db
def test_pick_str():
    user = CustomUser.objects.create(name="Test User", mbr_id=123)
    category = Category.objects.create(name="Test Category")
    pick = Pick.objects.create(user_id=user.mbr_id, category_id=category.id, player_name="Test Player", is_alternate=False, pick_order=1, pick_value=100, season=2025)
    assert str(pick) == "Test User - Test Category - Test Player (2025)"

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


# ============== SEASON STATS MODEL TESTS ==============

@pytest.mark.django_db
def test_season_stats_str():
    """Test SeasonStats string representation."""
    stats = SeasonStats.objects.create(year=2025, longest_hitting_streak=25, streak_holder_name="Test Player")
    assert str(stats) == "Season Stats 2025"


@pytest.mark.django_db
def test_season_stats_nullable_fields():
    """Test SeasonStats with null streak fields."""
    stats = SeasonStats.objects.create(year=2025)
    assert stats.longest_hitting_streak is None
    assert stats.streak_holder_name is None


@pytest.mark.django_db
def test_season_stats_with_streak():
    """Test SeasonStats with streak data."""
    stats = SeasonStats.objects.create(
        year=2025,
        longest_hitting_streak=30,
        streak_holder_name="DiMaggio Jr."
    )
    assert stats.longest_hitting_streak == 30
    assert stats.streak_holder_name == "DiMaggio Jr."


# ============== MLB LEADER MODEL TESTS ==============

@pytest.mark.django_db
def test_mlb_leader_str():
    """Test MlbLeader string representation."""
    leader = MlbLeader.objects.create(
        season=2025,
        category='battingAverage',
        rank=1,
        player_name="Test Player",
        team="NYY",
        value=0.350,
        api_player_id=12345
    )
    assert str(leader) == "battingAverage #1: Test Player (2025)"


@pytest.mark.django_db
def test_mlb_leader_headshot_url():
    """Test MlbLeader headshot_url property with api_player_id."""
    leader = MlbLeader.objects.create(
        season=2025,
        category='homeRuns',
        rank=1,
        player_name="Test Player",
        value=50,
        api_player_id=12345
    )
    expected_url = "https://img.mlbstatic.com/mlb-photos/image/upload/w_180,q_100/v1/people/12345/headshot/silo/current"
    assert leader.headshot_url == expected_url


@pytest.mark.django_db
def test_mlb_leader_headshot_url_no_api_id():
    """Test MlbLeader headshot_url returns None when no api_player_id."""
    leader = MlbLeader.objects.create(
        season=2025,
        category='homeRuns',
        rank=1,
        player_name="Test Player",
        value=50,
        api_player_id=None
    )
    assert leader.headshot_url is None


@pytest.mark.django_db
def test_mlb_leader_all_categories():
    """Test MlbLeader can be created for all category types."""
    categories = ['battingAverage', 'onBasePlusSlugging', 'homeRuns', 'wins', 'runsBattedIn', 'stolenBases']
    for i, cat in enumerate(categories):
        leader = MlbLeader.objects.create(
            season=2025,
            category=cat,
            rank=1,
            player_name=f"Leader for {cat}",
            value=100 if cat in ['homeRuns', 'wins', 'runsBattedIn', 'stolenBases'] else 0.350
        )
        assert leader.category == cat


# ============== HITTER/PITCHER CONSTRAINT TESTS ==============

@pytest.mark.django_db
def test_hitter_unique_player_season():
    """Test Hitter unique constraint on (player, season)."""
    player = Player.objects.create(player_name="Test Hitter", player_type="hitter", api_player_id=100)
    Hitter.objects.create(player=player, season=2025, average=0.300)
    with pytest.raises(IntegrityError):
        Hitter.objects.create(player=player, season=2025, average=0.280)


@pytest.mark.django_db
def test_hitter_same_player_different_seasons():
    """Test same player can have stats for different seasons."""
    player = Player.objects.create(player_name="Multi Season", player_type="hitter", api_player_id=101)
    hitter_2024 = Hitter.objects.create(player=player, season=2024, average=0.280)
    hitter_2025 = Hitter.objects.create(player=player, season=2025, average=0.300)
    assert hitter_2024.season == 2024
    assert hitter_2025.season == 2025


@pytest.mark.django_db
def test_pitcher_unique_player_season():
    """Test Pitcher unique constraint on (player, season)."""
    player = Player.objects.create(player_name="Test Pitcher", player_type="pitcher", api_player_id=102)
    Pitcher.objects.create(player=player, season=2025, wins=15)
    with pytest.raises(IntegrityError):
        Pitcher.objects.create(player=player, season=2025, wins=10)


@pytest.mark.django_db
def test_pitcher_same_player_different_seasons():
    """Test same pitcher can have stats for different seasons."""
    player = Player.objects.create(player_name="Multi Season Pitcher", player_type="pitcher", api_player_id=103)
    pitcher_2024 = Pitcher.objects.create(player=player, season=2024, wins=12)
    pitcher_2025 = Pitcher.objects.create(player=player, season=2025, wins=15)
    assert pitcher_2024.season == 2024
    assert pitcher_2025.season == 2025


# ============== HITTER/PITCHER STRING REPRESENTATION WITH SEASON ==============

@pytest.mark.django_db
def test_hitter_str_includes_season():
    """Test that Hitter __str__ includes season year."""
    player = Player.objects.create(player_name="Season Test", player_type="hitter", api_player_id=104)
    hitter = Hitter.objects.create(player=player, season=2025)
    assert "(2025)" in str(hitter)
    assert "Season Test" in str(hitter)


@pytest.mark.django_db
def test_pitcher_str_includes_season():
    """Test that Pitcher __str__ includes season year."""
    player = Player.objects.create(player_name="Ace Pitcher", player_type="pitcher", api_player_id=105)
    pitcher = Pitcher.objects.create(player=player, season=2024)
    assert "(2024)" in str(pitcher)
    assert "Ace Pitcher" in str(pitcher)


# ============== PICK MODEL EDGE CASES ==============

@pytest.mark.django_db
def test_pick_with_null_user():
    """Test Pick can have null user."""
    category = Category.objects.create(name="test_category")
    pick = Pick.objects.create(category=category, player_name="Test Player", season=2025)
    assert pick.user is None


@pytest.mark.django_db
def test_pick_with_pick_value():
    """Test Pick with pick_value for prediction categories."""
    user = CustomUser.objects.create(name="Test User", mbr_id=200)
    category = Category.objects.create(name="rbi_champion")
    pick = Pick.objects.create(
        user=user,
        category=category,
        player_name="RBI King",
        pick_value=150,
        season=2025
    )
    assert pick.pick_value == 150


@pytest.mark.django_db
def test_pick_with_pick_order():
    """Test Pick with pick_order for ordered picks."""
    user = CustomUser.objects.create(name="Test User", mbr_id=201)
    category = Category.objects.create(name="batters")
    pick = Pick.objects.create(
        user=user,
        category=category,
        player_name="First Pick",
        pick_order=1,
        is_alternate=False,
        season=2025
    )
    assert pick.pick_order == 1
    assert pick.is_alternate is False


@pytest.mark.django_db
def test_pick_str_format():
    """Test Pick string format includes all components."""
    user = CustomUser.objects.create(name="John Doe", mbr_id=202)
    category = Category.objects.create(name="batters")
    pick = Pick.objects.create(
        user=user,
        category=category,
        player_name="Mike Trout",
        season=2025
    )
    result = str(pick)
    assert "John Doe" in result
    assert "batters" in result
    assert "Mike Trout" in result


# ============== PLAYER TYPE VALIDATION ==============

@pytest.mark.django_db
def test_player_type_hitter():
    """Test Player with hitter type."""
    player = Player.objects.create(player_name="Batter", player_type="hitter", api_player_id=300)
    assert player.player_type == "hitter"


@pytest.mark.django_db
def test_player_type_pitcher():
    """Test Player with pitcher type."""
    player = Player.objects.create(player_name="Thrower", player_type="pitcher", api_player_id=301)
    assert player.player_type == "pitcher"


@pytest.mark.django_db
def test_player_nullable_api_id():
    """Test Player can have null api_player_id."""
    player = Player.objects.create(player_name="Unknown API", player_type="hitter", api_player_id=None)
    assert player.api_player_id is None


# ============== HITTER/PITCHER NULLABLE STATS ==============

@pytest.mark.django_db
def test_hitter_nullable_stats():
    """Test Hitter can have all null stats."""
    player = Player.objects.create(player_name="No Stats", player_type="hitter", api_player_id=400)
    hitter = Hitter.objects.create(player=player, season=2025)
    assert hitter.average is None
    assert hitter.ops is None
    assert hitter.plate_appearances is None
    assert hitter.home_runs is None
    assert hitter.rbis is None
    assert hitter.stolen_bases is None


@pytest.mark.django_db
def test_pitcher_nullable_stats():
    """Test Pitcher can have all null stats."""
    player = Player.objects.create(player_name="No Stats Pitcher", player_type="pitcher", api_player_id=401)
    pitcher = Pitcher.objects.create(player=player, season=2025)
    assert pitcher.wins is None
    assert pitcher.losses is None
    assert pitcher.era is None
    assert pitcher.strikeouts is None