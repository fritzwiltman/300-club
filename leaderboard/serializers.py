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
