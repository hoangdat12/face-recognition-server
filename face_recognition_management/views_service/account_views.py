from django.http import JsonResponse
from rest_framework.decorators import api_view
import os
import time
import json
from ..repository import UserRepository, HistoryRepository
from ..decorators import permission
from ..constants import UserAccountStatus, Role, AuthenticateMethod
from ..services import TokenService, S3Service
from rest_framework.decorators import api_view
from ..responses import *
from ..ultils.index import check_password, format_user, generate_user_information, password_encrypt

# S3 bucket name
s3_bucket_employees = os.environ.get('AWS_S3_BUCKET_EMPLOYEES')
s3_bucket_guest = os.environ.get('AWS_S3_BUCKET_GUEST')


@api_view(['POST'])
def authenticate_account(request):
    if request.method == 'POST':
        try:
            username = request.POST.get('username') or request.data.get('username')
            password = request.POST.get('password') or request.data.get('password')
            found_account = UserRepository.find_by_username(username)
            if not found_account:
                return ResponseNotFound(message="User not found")
            
            # Verify password
            is_correct_pw = check_password(password, found_account["password"]);
            if not is_correct_pw:
                return ResponseBadRequest(message="Wrong password")
            
            
            token_service = TokenService()
            token_pairs = token_service.generate(found_account)

            # Generate token
            if 'password' in found_account:
                del found_account['password']
            
            HistoryRepository.create_history(
                device_id=found_account["device_id"], 
                user_information=generate_user_information(found_account), 
                authenticate_with= AuthenticateMethod.ACCOUNT.value
            )

            found_account["image"] = S3Service.presigned_url(s3_bucket_employees, found_account["image"])
            data = {
                'user': found_account,
                'refresh': token_pairs["refresh_token"],
                'access': token_pairs["access_token"],
            }

            return ResponseOk(data, message="Success")

        except json.JSONDecodeError:
            return ResponseBadRequest(message="Invalid JSON data")
        except Exception as e:
            print(f"Error: {e}")
            return ResponseInternalServerError(message="Error authenticate account")

    return JsonResponse({'error': 'Invalid request method'}, status=405)


@api_view(["DELETE"])
# @permission([Role.HOST.value, Role.ADMIN.value, Role.SUPER.value])
def disable_employee_in_device(request, employee_id):
    found_user = UserRepository.find_active_user_by_id(employee_id);
    if not found_user:
        return ResponseNotFound(message="User not found")

    found_user["status"] = UserAccountStatus.DELETED.value
    UserRepository.update_user_status(found_user);
    
    return ResponseOk(message="Delete user success")

@api_view(["GET"])
def get_user_information(request, user_id):
    found_user = UserRepository.find_active_user_by_id(user_id);
    if not found_user:
        return ResponseNotFound(message="User not found")
    
    found_user["image"] = S3Service.presigned_url(s3_bucket_employees, found_user["image"])

    return ResponseOk(data=format_user(found_user))

@api_view(["GET"])
def get_roles(request):
    roles = {role.name: role.value for role in Role}
    return ResponseOk(data = roles)

@api_view(["PUT"])
def update_account_information(request, user_id):
    cur_password = request.POST.get('curPassword') or request.data.get('curPassword')
    new_password = request.POST.get('newPassword') or request.data.get('newPassword')
    confirm_password = request.POST.get('confirmPassword') or request.data.get('confirmPassword')
    first_name = request.POST.get('firstName') or request.data.get('firstName')
    last_name = request.POST.get('lastName') or request.data.get('lastName')
    position = request.POST.get('position') or request.data.get('position')
    gender = request.POST.get('gender') or request.data.get('gender')


    found_user = UserRepository.find_active_user_by_id(user_id);
    if not found_user:
        return ResponseNotFound(message="User not found")
    
    # update password
    if cur_password:
        if new_password != confirm_password:
            return ResponseBadRequest(message="New password and confirm password not match!")
        
        # Verify password
        is_correct_pw = check_password(cur_password, found_user["password"]);
        if not is_correct_pw:
            return ResponseBadRequest(message="Wrong password")
        
        found_user["password"] = password_encrypt(password=new_password);

    if last_name != found_user["last_name"] and last_name: found_user["last_name"] = last_name
    if first_name != found_user["first_name"] and last_name: found_user["first_name"] = first_name
    if position != found_user["position"] and last_name: found_user["position"] = position
    if gender != found_user["gender"] and last_name: found_user["gender"] = gender

    #Save
    UserRepository.save(found_user)

    return ResponseOk(data=format_user(found_user))

@api_view(["PUT"])
def update_account_avatar(request, user_id):
    image_data = request.FILES['image'].read()  
    found_user = UserRepository.find_active_user_by_id(user_id);
    if not found_user:
        return ResponseNotFound(message="User not found")
    
    image_filename = f"{found_user['device_id']}/{int(time.time() * 1000)}-{found_user['username']}.jpg"

    isSuccess = S3Service.put_object(s3_bucket_employees, image_filename, image_data)
    if not isSuccess:
        return ResponseInternalServerError(message="Upload failure")
    
    found_user["image"] = image_filename;

    UserRepository.save(found_user);

    found_user["image"] = S3Service.presigned_url(bucket_name=s3_bucket_employees, file_name=image_filename)

    return ResponseOk(data=format_user(found_user));
