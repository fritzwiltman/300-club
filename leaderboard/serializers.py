from rest_framework import serializers
from .models import Player

class PlayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Player
        fields = '__all__'

class HitterLeaderboardSerializer(serializers.Serializer):
    user_name = serializers.CharField()
    aggregate_average = serializers.DecimalField(max_digits=6, decimal_places=4, allow_null=True)
    alternate_average = serializers.DecimalField(max_digits=6, decimal_places=4, allow_null=True)
    aggregate_ops = serializers.DecimalField(max_digits=6, decimal_places=4, allow_null=True)
    rank = serializers.IntegerField()

    qualified_picks = serializers.ListField(
        child=serializers.DictField(
            child=serializers.CharField(),  # Allows both player_name (str) and average (str/float)
            required=True
        ), required=True
    )

    disqualified_picks = serializers.ListField(
        child=serializers.DictField(
            child=serializers.CharField(),  # Allows both player_name (str) and plate_appearances (str/float)
            required=True
        ), required=True
    )

class AlternateBatterSerializer(serializers.Serializer):
    player_name = serializers.CharField()
    average = serializers.FloatField()
    is_disqualified = serializers.BooleanField()

class HomerunLeaderboardSerializer(serializers.Serializer):
    user_name = serializers.CharField()
    top_three_total_homeruns = serializers.IntegerField()
    first_tiebreaker_homeruns = serializers.IntegerField(allow_null=True)
    second_tiebreaker_average = serializers.FloatField()
    rank = serializers.IntegerField(allow_null=True)

    # List of dictionaries: {"player_name": str, "home_runs": int}
    all_homerun_picks = serializers.ListField(
        child=serializers.DictField(
            child=serializers.CharField(),  # player_name (str)
            required=True
        ),
        required=True
    )

    # List of dictionaries: {"player_name": str, "average": float, "is_disqualified": bool}
    alternate_batters_picks = AlternateBatterSerializer(many=True)  # ✅ Use a nested serializer


class OpsLeaderboardSerializer(serializers.Serializer):
    user_name = serializers.CharField()
    aggregate_ops = serializers.DecimalField(max_digits=6, decimal_places=5, allow_null=True)
    alternate_average = serializers.DecimalField(max_digits=6, decimal_places=4, allow_null=True)
    rank = serializers.IntegerField()

    qualified_picks = serializers.ListField(
        child=serializers.DictField(child=serializers.CharField()),
        required=True
    )

    disqualified_picks = serializers.ListField(
        child=serializers.DictField(child=serializers.CharField()),
        required=True
    )


class PitcherPickSerializer(serializers.Serializer):
    player_name = serializers.CharField()
    wins = serializers.IntegerField(allow_null=True)
    losses = serializers.IntegerField(allow_null=True)
    era = serializers.FloatField(allow_null=True)
    strikeouts = serializers.IntegerField(allow_null=True)


class PitcherLeaderboardSerializer(serializers.Serializer):
    user_name = serializers.CharField()
    top_three_total_wins = serializers.IntegerField()
    first_tiebreaker_wins = serializers.IntegerField(allow_null=True)
    second_tiebreaker_win_pct = serializers.FloatField(allow_null=True)
    third_tiebreaker_era = serializers.FloatField(allow_null=True)
    fourth_tiebreaker_alt_avg = serializers.FloatField(allow_null=True)
    rank = serializers.IntegerField()
    pitcher_picks = PitcherPickSerializer(many=True)


class RbiChampionEntrySerializer(serializers.Serializer):
    user_name = serializers.CharField()
    predicted_player = serializers.CharField()
    predicted_rbis = serializers.IntegerField(allow_null=True)
    predicted_correct_player = serializers.BooleanField()
    rbi_difference = serializers.IntegerField(allow_null=True)
    alternates_average = serializers.FloatField(allow_null=True)
    rank = serializers.IntegerField(allow_null=True)


class RbiChampionLeaderboardSerializer(serializers.Serializer):
    actual_rbi_leader = serializers.DictField(allow_null=True)
    leaderboard = RbiChampionEntrySerializer(many=True)


class StolenBaseEntrySerializer(serializers.Serializer):
    user_name = serializers.CharField()
    predicted_player = serializers.CharField()
    predicted_stolen_bases = serializers.IntegerField(allow_null=True)
    predicted_correct_player = serializers.BooleanField()
    sb_difference = serializers.IntegerField(allow_null=True)
    alternates_average = serializers.FloatField(allow_null=True)
    rank = serializers.IntegerField(allow_null=True)


class StolenBaseLeaderboardSerializer(serializers.Serializer):
    actual_sb_leader = serializers.DictField(allow_null=True)
    leaderboard = StolenBaseEntrySerializer(many=True)


class DiMaggioEntrySerializer(serializers.Serializer):
    user_name = serializers.CharField()
    predicted_streak = serializers.IntegerField(allow_null=True)
    is_exact_match = serializers.BooleanField()
    alternates_average = serializers.FloatField(allow_null=True)
    rank = serializers.IntegerField(allow_null=True)


class DiMaggioLeaderboardSerializer(serializers.Serializer):
    actual_longest_streak = serializers.IntegerField(allow_null=True)
    streak_holder_name = serializers.CharField(allow_null=True)
    leaderboard = DiMaggioEntrySerializer(many=True)


class UserSerializer(serializers.Serializer):
    mbr_id = serializers.IntegerField()
    name = serializers.CharField()


class CategorySerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    picks_per_user = serializers.IntegerField()
    description = serializers.CharField()
