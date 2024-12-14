from django.shortcuts import render

# Create your views here.
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Player
from .serializers import PlayerSerializer

@api_view(['GET'])
def player_list(request):
    players = Player.objects.all()
    serializer = PlayerSerializer(players, many=True)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=201)
    return Response(serializer.errors, status=400)