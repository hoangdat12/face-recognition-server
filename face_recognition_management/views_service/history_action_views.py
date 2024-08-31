from rest_framework.decorators import api_view
from ..repository import HistoryActionRepository, DeviceRepository, UserRepository
from ..constants import HistoryAction
from rest_framework.decorators import api_view
from ..responses import *
from ..ultils.index import format_date, is_valid_date

@api_view(["GET"])
def get_history_type(request):
    actions = {action.name: action.value for action in HistoryAction}
    return ResponseOk(data = actions)

@api_view(['POST'])
def create_history_action(request):
    device_id = request.POST.get('deviceId') or request.data.get('deviceId')
    user_id = request.POST.get('userId') or request.data.get('userId')
    action = request.POST.get('action') or request.data.get('action')
    
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseBadRequest(message="Device not found")

    history_action = HistoryActionRepository.create_history(device_id=device_id, user_id=user_id, action=action)

    return ResponseOk(data=history_action)

@api_view(["GET"])
def get_history_action(request):
    device_id = request.GET.get('deviceId')
    user_id = request.GET.get('userId')
    date_str = request.GET.get('date')

    date = format_date(date_str)
    
    if not is_valid_date(date):
        return ResponseOk("Invalid Date")
    
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseBadRequest(message="Device not found")
    
    found_user_device = UserRepository.find_exist_device_with_device_id(device_id)
    if not found_user_device:
        return ResponseBadRequest(message="Device already has a master account")
    
    histories = HistoryActionRepository.get_history(device_id=device_id, user_id=user_id, date=date)

    return ResponseOk(data = histories)
    