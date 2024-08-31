from rest_framework.decorators import api_view
from .decorators import permission
from rest_framework.decorators import api_view
from .responses import *

# Views
from .views_service.account_views import *
from .views_service.face_views import *
from .views_service.device_views import *
from .views_service.token_views import *
from .views_service.history_views import *
from .views_service.history_action_views import *

@api_view(['GET'])
def hello_server(request):
    print("Run")
    return ResponseOk(message="Hello from server")


    