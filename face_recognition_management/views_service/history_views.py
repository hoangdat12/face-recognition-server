from rest_framework.decorators import api_view
from rest_framework.decorators import api_view
from ..responses import *
from ..repository import DeviceRepository, HistoryRepository, UserRepository
from ..ultils.index import is_valid_date, format_date, get_start_key, get_histories_response, generate_user_information, get_current_date
from ..constants import AuthenticateMethod
from datetime import datetime

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
def get_histories_by_date(request):
    date_str = request.GET.get('date', None)

    if not date_str:
        return ResponseBadRequest("Missing Date")

    timestamp = datetime.strptime(date_str, '%Y-%m-%d')
    date_query = timestamp.strftime('%Y-%m-%d')
    
    histories = HistoryRepository.get_histories_by_date(date=date_query)

    seen_ids = set()
    unique_histories = list(
        map(lambda x: x if x['id'] not in seen_ids and not seen_ids.add(x['id']) else None, histories)
    )
    unique_histories = [history for history in unique_histories if history is not None]

    return ResponseOk(data=unique_histories, message="Success")


@api_view(["GET"])
def get_detail_histories(request):
    date_str = request.GET.get('date', None)
    user_id = request.GET.get('userId', None)

    if not date_str or not user_id:
        return ResponseBadRequest("Missing Request Data")

    timestamp = datetime.strptime(date_str, '%Y-%m-%d')
    date_query = timestamp.strftime('%Y-%m-%d')
    
    histories = HistoryRepository.get_histories_detail(user_id=user_id, date_query=date_query)
    print(histories)

    return ResponseOk(data=histories, message="Success")

@api_view(["POST"])
def verify_rfid_id(request):
    rfid_id = request.POST.get('rfid_id') or request.data.get('rfid_id')

    found_account = UserRepository.find_by_rfid_id(rfid_id=rfid_id)
    if not found_account:
        return ResponseNotFound(message="Khong hop le")

    # found_account = UserRepository.find_by_id(user_id=rfid_id)
    # if not found_account:
    #     return ResponseNotFound(message="Khong hop le")

    current_date = get_current_date()

    latest_record = HistoryRepository.get_latest_record(found_account["id"], current_date)
    if (latest_record):
        current_status = latest_record["status"]
        if (current_status == "Check in"): status = "Check out";
        else: status = "Check in"
    else:
        status = "Check in";

    HistoryRepository.create_history(
        user_id=found_account["id"], 
        user_information=generate_user_information(found_account), 
        authenticate_with= AuthenticateMethod.RFID.value,
        status = status
    )

    return ResponseOk(data=None, message="Success!")

@api_view(["GET"])
def generate_data(request):
    HistoryRepository.generate_test_data("BC5BPV21X0", page=4)
    return Response()