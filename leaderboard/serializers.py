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
