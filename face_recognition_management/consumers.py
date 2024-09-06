import json
from channels.generic.websocket import WebsocketConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

class TurnOnDevice(WebsocketConsumer):
    def connect(self):
        # Add the WebSocket connection to a group
        self.room_group_name = 'turn_on_device_group'
        self.channel_layer = get_channel_layer()
        
        # Join the group
        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )
        self.accept()

    def disconnect(self, close_code):
        # Remove the WebSocket connection from the group
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name
        )

    def receive(self, text_data):
        try:
            # Parse incoming WebSocket message
            data = json.loads(text_data)
            data["isTurnOn"] = data.get("status") == "TURN_ON"
            
            # Broadcast message to the group
            async_to_sync(self.channel_layer.group_send)(
                self.room_group_name,
                {
                    'type': 'device_status',
                    'message': data
                }
            )
        except json.JSONDecodeError:
            # Handle JSON decoding errors
            self.send(text_data=json.dumps({'error': 'Invalid JSON'}))
        except KeyError:
            # Handle missing key errors
            self.send(text_data=json.dumps({'error': 'Missing status key'}))

    def device_status(self, event):
        # Send message to WebSocket client
        self.send(text_data=json.dumps(event['message']))
