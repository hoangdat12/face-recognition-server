from django.urls import re_path 
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/device/turn-on', consumers.TurnOnDevice.as_asgi())
]