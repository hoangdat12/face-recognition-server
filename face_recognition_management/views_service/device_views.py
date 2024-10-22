import os
from rest_framework.decorators import api_view
from ..repository import UserRepository, DeviceRepository
from ..decorators import permission
from ..constants import Role, UserAccountStatus
from ..services import S3Service, AwsIoTService
from rest_framework.decorators import api_view
from ..responses import *
from ..ultils.index import format_user, random_value
from ..constants import DeviceStatus
from awscrt import mqtt
import json
import boto3

DEFAULT_DEVICE_ID_QUANTITY = 10;

# Initialize the AWS IoT Data client
iot_client = boto3.client('iot-data', region_name=os.environ.get('AWS_REGION')) 

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
    
@api_view(["GET"])
def get_device_shadow(request, device_id):
    # Find the device using DeviceRepository
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return Response({"message": f"Device with id {device_id} not found"}, status=404)
    
    thing_name = f"{AwsIoTService.PROVISIONING_TEMPLATE_NAME}_{device_id}"

    # Retrieve the current shadow state
    try:
        response = iot_client.get_thing_shadow(thingName=thing_name)
        current_shadow = json.loads(response['payload'].read().decode('utf-8'))

        return ResponseOk(data=current_shadow)
    except Exception as e:
        return ResponseInternalServerError(message=f"Failed to fetch current shadow state: {str(e)}")

@api_view(['PUT'])
def update_device_information(request, device_id):
    device_info_name = request.POST.get('name') or request.data.get('name')
    device_automate = request.POST.get('isAutomate') or request.data.get('isAutomate')
    device_default_value = request.POST.get('defaultValue') or request.data.get('defaultValue')

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

@api_view(['PUT'])
def update_device_shadow(request, device_id):
    # Find the device using DeviceRepository
    found_device = DeviceRepository.find_active_by_device_id(device_id)
    if not found_device:
        return Response({"message": f"Device with id {device_id} not found"}, status=404)
    
    thing_name = f"{AwsIoTService.PROVISIONING_TEMPLATE_NAME}_{device_id}"

    # Get device name and status from the request
    device_name = request.data.get('deviceName')  # Possible values: fan, door, light
    device_status = request.data.get('deviceStatus')  # Status information (e.g., status, speed, brightness)

    # Validate inputs
    if not device_name or not device_status:
        return ResponseNotFound(message="Missing deviceName or deviceStatus in the request")

    # Retrieve the current shadow state
    try:
        response = iot_client.get_thing_shadow(thingName=thing_name)
        current_shadow = json.loads(response['payload'].read().decode('utf-8'))
    except Exception as e:
        return ResponseInternalServerError(message=f"Failed to fetch current shadow state: {str(e)}")

    print(current_shadow)

    # Initialize the shadow update payload with the current state
    shadow_update = {
        "state": {
            "reported": {
            }
        }
    }

    shadow_update["state"]["reported"] = current_shadow["state"]["reported"]
    
    # Update the relevant device information
    shadow_update["state"]["reported"][device_name] = device_status
    
    # Publish the shadow update payload
    try:
        iot_client.update_thing_shadow(
            thingName=thing_name,
            payload=json.dumps(shadow_update)
        )
        return ResponseOk(data=shadow_update)
    except Exception as e:
        return ResponseInternalServerError(message= f"Failed to update device shadow: {str(e)}")


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

    updated_users = []
    for device_user in device_users:
        if device_user["role"] != Role.ADMIN.value and device_user["status"] != UserAccountStatus.DELETED.value:
            s3_file_name = device_user["image"]
            s3_url = S3Service.presigned_url(bucket_name=s3_bucket_employees, file_name=s3_file_name)
            device_user["image"] = s3_url
            updated_users.append(format_user(device_user))

    return ResponseOk(data=updated_users)

@api_view(["DELETE"])
# @permission([Role.ADMIN.value, Role.SUPER.value])
def disable_device(_, device_id):
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

@api_view(["GET"])
# @permission([Role.ADMIN.value, Role.SUPER.value])
def generate_certificate_for_device(_, device_id):
    found_device = DeviceRepository.find_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    response_ctf = AwsIoTService.generate_certificate(device_id)

    found_device["private_key"] = response_ctf["new_key_pem"]
    found_device["certificate"] = response_ctf["new_cert_pem"]
    found_device["public_key"] = response_ctf["new_public_key_pem"]
    found_device["status"] = DeviceStatus.ACTIVE.value

    update_response = DeviceRepository.save(found_device)
    if update_response is None:
        return ResponseInternalServerError(message="DB error")
    
    response_data = {
        "cert_pem": response_ctf["new_cert_pem"],
        "private_key": response_ctf["new_key_pem"],
    }

    return ResponseOk(message="Generate Certificate success!", data=response_data)

@api_view(["POST"])
def control_device_door(request):
    device_id = request.POST.get('deviceId') or request.data.get('deviceId')
    door_status = request.POST.get('doorStatus') or request.data.get('doorStatus')

    found_device = DeviceRepository.find_by_device_id(device_id)
    if not found_device:
        return ResponseNotFound(message=f"Device with id {device_id} not found")
    
    message = {
        "type": "Control/Door",
        "message": door_status,
        "device_id": device_id
    }
    aws_iot_cdt = "pbl/device/control/door"

    isSuccess = AwsIoTService.publish_message(topic=aws_iot_cdt, message=message)
    if (isSuccess):
        return ResponseOk(message="Success")
    else:
        return ResponseInternalServerError(message="Can't not publish message!")

@api_view(["POST"])
def take_picture(request):
    client_id = request.POST.get('clientId') or request.data.get('clientId')

    message = {
        "type": "Control/Camera",
        "message": "Take picture",
        "clientId": client_id
    }

    aws_iot_cdt = "pbl/device/control/camera"

    isSuccess = AwsIoTService.publish_message(topic=aws_iot_cdt, message=message)
    if (isSuccess):
        return ResponseOk(message="Success")
    else:
        return ResponseInternalServerError(message="Can't not publish message!")