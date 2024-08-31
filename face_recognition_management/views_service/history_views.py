from rest_framework.decorators import api_view
from rest_framework.decorators import api_view
from ..responses import *
from ..repository import DeviceRepository, HistoryRepository
from ..ultils.index import is_valid_date, format_date, get_start_key, get_histories_response

@api_view(["GET"])
# @permission([Role.HOST.value, Role.ADMIN.value, Role.SUPER.value])
def get_history(request, device_id):
    limit = int(request.GET.get('limit', 20))
    page = int(request.GET.get('page', 1))
    start_key_str= request.GET.get('startKey', None) 
    start_key_str = format_date(start_key_str)
    
    if start_key_str and not is_valid_date(start_key_str):
        return ResponseBadRequest("Invalid Date")
    
    start_key = get_start_key(start_key_str, device_id)
    
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    histories = HistoryRepository.get_history_of_device(
        device_id=device_id, 
        page=page, 
        limit=limit, 
        start_key=start_key
    );

    response_data = get_histories_response(histories)

    return ResponseOk(data=response_data)

@api_view(["GET"])
# @permission([Role.HOST.value, Role.ADMIN.value, Role.SUPER.value])
def get_history_by_date(request, device_id):
    limit = int(request.GET.get('limit', 20))
    page = int(request.GET.get('page', 1))
    start_key_str= request.GET.get('startKey', None) 
    date_str = request.GET.get('date', None)
    if not date_str:
        return ResponseBadRequest("Missing Date")

    date = format_date(date_str)
    start_key_str = get_start_key(start_key_str, device_id)
    
    if (start_key_str and not is_valid_date(start_key_str)) or not is_valid_date(date):
        return ResponseOk("Invalid Date")
    
    start_key = get_start_key(start_key_str)

    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    histories = HistoryRepository.get_history_by_date(
        device_id=device_id, 
        page=page, 
        limit=limit, 
        start_key=start_key,
        date=date
    );

    response_data = get_histories_response(histories)

    return ResponseOk(data=response_data)

@api_view(["GET"])
def generate_data(request):
    HistoryRepository.generate_test_data("BC5BPV21X0", page=4)
    return Response()