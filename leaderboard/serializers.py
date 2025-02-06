from rest_framework import serializers
from .models import Player

class PlayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Player
        fields = '__all__'

class HitterLeaderboardSerializer(serializers.Serializer):
    user_name = serializers.CharField()
    aggregate_average = serializers.FloatField()
    alternate_average = serializers.FloatField()
    qualified_batters = serializers.ListField(
        child=serializers.DictField(
            child=serializers.CharField()
        )
    )
    rank = serializers.IntegerField()
