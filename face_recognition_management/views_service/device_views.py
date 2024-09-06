import os
from rest_framework.decorators import api_view
from ..repository import UserRepository, DeviceRepository
from ..decorators import permission
from ..constants import Role
from ..services import S3Service
from rest_framework.decorators import api_view
from ..responses import *
from ..ultils.index import format_user, random_value
from ..constants import DeviceStatus

DEFAULT_DEVICE_ID_QUANTITY = 10;

# S3 bucket name
s3_bucket_employees = os.environ.get('AWS_S3_BUCKET_EMPLOYEES')
s3_bucket_guest = os.environ.get('AWS_S3_BUCKET_GUEST')

@api_view(['POST'])
def generate_device_id(request):
    try:
        devices_quantity = int(request.POST.get('devicesQuantity', DEFAULT_DEVICE_ID_QUANTITY))
        
        device_ids = []
        for _ in range(devices_quantity):
            device_id = random_value(length=10)
            device_ids.append(device_id)

        DeviceRepository.batch_device_id(device_ids)

        return ResponseCreated(message="Generate device ids successully!")

    except Exception as e:
        print(f"Error: {e}")
        return ResponseInternalServerError(message="Add device id to DB failure")
    
@api_view(['PUT'])
def update_device_information(request, device_id):
    device_info_name = request.POST.get('name') or request.data.get('name')
    device_automate = request.POST.get('isAutomate') or request.data.get('isAutomate')
    device_default_value = request.POST.get('defaultValue') or request.data.get('defaultValue')

    print(device_automate, device_info_name, device_default_value)

    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    for device_info in found_device["device_informations"]:
        if (device_info["name"] == device_info_name):
            if (device_info["is_automate"] == device_automate):
                return ResponseOk(message="Nothing change!")
            
            device_info["is_automate"] = device_automate
            device_info["default_value"] = device_default_value

    DeviceRepository.save(found_device)

    return ResponseOk(data=found_device)


@api_view(["GET"])
def get_device_detail(request, device_id):
    found_device_id = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device_id:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    return ResponseOk(data=found_device_id)

@api_view(["GET"])
# @permission([Role.HOST.value, Role.ADMIN.value, Role.SUPER.value])
def get_employee_in_device(request, device_id):
    found_device_id = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device_id:
        return ResponseNotFound(message=f"Device with id {device_id} not found")

    device_users = UserRepository.find_users_device(device_id)
    if not len(device_users):
        return ResponseBadRequest(message="Device id not found")
    
    for device_user in device_users:
        s3_file_name = device_user["image"];
        s3_url = S3Service.presigned_url(bucket_name=s3_bucket_employees, file_name=s3_file_name)
        device_user["image"] = s3_url
        device_user = format_user(device_user)
    
    return ResponseOk(data=device_users)

@api_view(["DELETE"])
# @permission([Role.ADMIN.value, Role.SUPER.value])
def disable_device(request, device_id):
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    found_device["status"] = DeviceStatus.INACTIVE.value
    DeviceRepository.update_device_status(found_device);

    # Disable account in Device
    device_users = UserRepository.find_users_device(device_id);

    # update status in dynamodb
    UserRepository.disable_users_in_device_batch(device_users);
    # for device_user in device_users:
    #     device_user["status"] = UserAccountStatus.DELETED.value
    #     UserRepository.update_user_status(device_user)

    return ResponseOk(message="Delete device success")