# leaderboard/tests/test_serializers.py
import pytest
from rest_framework.exceptions import ValidationError
from leaderboard.models import Player
from leaderboard.serializers import PlayerSerializer

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