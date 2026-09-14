"""
Shared pytest fixtures for leaderboard tests.
"""
import pytest
from datetime import date
from leaderboard.models import (
    Category, CustomUser, Player, Hitter, Pitcher, Pick, SeasonStats, MlbLeader
)


@pytest.fixture
def base_categories():
    """Create all 7 standard categories."""
    categories = [
        Category.objects.create(id=1, name="batters"),
        Category.objects.create(id=2, name="alternate_batters"),
        Category.objects.create(id=3, name="pitchers"),
        Category.objects.create(id=4, name="home_run_hitters"),
        Category.objects.create(id=5, name="rbi_champion"),
        Category.objects.create(id=6, name="stolen_base_champion"),
        Category.objects.create(id=7, name="dimaggio"),
    ]
    return categories


@pytest.fixture
def sample_users():
    """Create 3 sample users for testing."""
    return [
        CustomUser.objects.create(name="User 1", mbr_id=1),
        CustomUser.objects.create(name="User 2", mbr_id=2),
        CustomUser.objects.create(name="User 3", mbr_id=3),
    ]


@pytest.fixture
def sample_hitters():
    """Create sample hitter players with stats."""
    players = []
    for i in range(15):
        player = Player.objects.create(
            player_name=f"Hitter {i+1}",
            player_type="hitter",
            api_player_id=10000 + i
        )
        Hitter.objects.create(
            player=player,
            season=2025,
            average=0.300 - (i * 0.005),
            ops=0.900 - (i * 0.01),
            plate_appearances=600 if i < 10 else 300,  # Some below threshold
            home_runs=30 - i,
            rbis=100 - (i * 3),
            stolen_bases=20 - i
        )
        players.append(player)
    return players


@pytest.fixture
def sample_pitchers():
    """Create sample pitcher players with stats."""
    players = []
    for i in range(8):
        player = Player.objects.create(
            player_name=f"Pitcher {i+1}",
            player_type="pitcher",
            api_player_id=20000 + i
        )
        Pitcher.objects.create(
            player=player,
            season=2025,
            wins=15 - i,
            losses=5 + i,
            era=3.00 + (i * 0.25),
            strikeouts=200 - (i * 10)
        )
        players.append(player)
    return players


@pytest.fixture
def season_stats_2025():
    """Create season stats for 2025 with a hitting streak."""
    return SeasonStats.objects.create(
        year=2025,
        longest_hitting_streak=25,
        streak_holder_name="Hitting Streak Leader"
    )


@pytest.fixture
def mlb_leaders_2025():
    """Create MLB leaders for various categories."""
    leaders = []
    categories_data = [
        ('battingAverage', 0.350, 0.005),
        ('onBasePlusSlugging', 1.050, 0.020),
        ('homeRuns', 50, 2),
        ('wins', 20, 1),
        ('runsBattedIn', 130, 5),
        ('stolenBases', 70, 5),
    ]

    for cat, base_value, decrement in categories_data:
        for rank in range(1, 6):
            leader = MlbLeader.objects.create(
                season=2025,
                category=cat,
                rank=rank,
                player_name=f"{cat} Leader {rank}",
                team="NYY" if rank % 2 == 1 else "LAD",
                value=base_value - ((rank - 1) * decrement),
                api_player_id=30000 + (rank * 10) + list(dict(MlbLeader.CATEGORY_CHOICES).keys()).index(cat)
            )
            leaders.append(leader)
    return leaders


@pytest.fixture
def mock_mid_season(monkeypatch):
    """Mock date to mid-season (July 1, 2026)."""
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 7, 1)

    monkeypatch.setattr("leaderboard.views.date", MockDate)
    return MockDate


@pytest.fixture
def mock_pre_season(monkeypatch):
    """Mock date to pre-season (March 1, 2026)."""
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 3, 1)

    monkeypatch.setattr("leaderboard.views.date", MockDate)
    return MockDate


@pytest.fixture
def mock_opening_day(monkeypatch):
    """Mock date to opening day (March 27, 2026)."""
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 3, 27)

    monkeypatch.setattr("leaderboard.views.date", MockDate)
    return MockDate


@pytest.fixture
def mock_end_of_season(monkeypatch):
    """Mock date to end of season (Oct 1, 2026) so PA threshold is full 502."""
    class MockDate(date):
        @classmethod
        def today(cls):
            return date(2026, 10, 1)

    monkeypatch.setattr("leaderboard.views.date", MockDate)
    return MockDate
