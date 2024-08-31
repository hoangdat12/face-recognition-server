from django.http import JsonResponse
from rest_framework.decorators import api_view
import time
import json
import os
from botocore.exceptions import ClientError
from ..repository import UserRepository, DeviceRepository, HistoryRepository
from ..decorators import permission
from ..constants import Role, Prefix, UserAccountStatus, AuthenticateMethod
from ..services import S3Service, RekognitionService
from datetime import datetime
from rest_framework.decorators import api_view
from ..responses import *
from ..ultils.index import password_encrypt, generate_user_information, format_user

# S3 bucket name
s3_bucket_employees = os.environ.get('AWS_S3_BUCKET_EMPLOYEES')
s3_bucket_guest = os.environ.get('AWS_S3_BUCKET_GUEST')

@api_view(['POST'])
def registor_master_account(request):
    try:
        device_id = request.POST.get('deviceId') or request.data.get('deviceId')
        username = request.POST.get('username') or request.data.get('username')
        password = request.POST.get('password') or request.data.get('password')
        image_data = request.FILES['image'].read()  
        
        # Check device
        found_device = DeviceRepository.find_active_by_device_id(device_id)
        if not found_device:
            return ResponseBadRequest(message="Device not found")

        # Check master account with device
        found_user_device = UserRepository.find_exist_device_with_device_id(device_id)
        if found_user_device:
            return ResponseBadRequest(message="Device already has a master account")
        
        # Check if account exist with username
        found_account = UserRepository.find_by_username(username)
        if found_account:
            return ResponseBadRequest(message="Username is already exist")

        image_filename = f"{device_id}/{int(time.time() * 1000)}-{username}.jpg"

        # S3 upload
        isSuccess = S3Service.put_object(s3_bucket_employees, image_filename, image_data)
        if not isSuccess:
            return ResponseInternalServerError(message="Upload failure")

        # Create collection for device
        collection_id = f'{device_id}-{Prefix.REKOGNITION_COLLECTION_PREFIX.value}'
        isSuccess = RekognitionService.create_collection(collection_id)
        if not isSuccess:
            return ResponseBadRequest(message="Collection with deviceId {device_id} already exists")

        # face indexing
        index_face_response = RekognitionService.index_face(image_filename, username, collection_id)
        if not index_face_response["isSuccess"]:
            return ResponseInternalServerError(message=index_face_response["message"])

        # Encrypt password
        encrypted_password = password_encrypt(password)

        # DynamoDB put_item
        UserRepository.create_user({
                'id': index_face_response["face_id"],  # Rekognition Face ID as the primary key
                'device_id': device_id,
                'username': username,
                'password': encrypted_password,
                'image': image_filename,
                'creation_time': datetime.now().isoformat(),
                'role': Role.HOST.value,
                'status': UserAccountStatus.ACTIVE.value
            })

        return ResponseOk(message=f'Registration successful: Welcome, {username}')
    except Exception as e:
        print(f"Error: {e}")
        return ResponseInternalServerError()

@api_view(['POST'])
def registration_employees(request):
    if request.method == 'POST':
        try:
            # Extract JSON body
            registor_id = request.POST.get('registorId') or request.data.get('registorId')
            username = request.POST.get('username') or request.data.get('username')
            password = request.POST.get('password') or request.data.get('password')
            first_name = request.POST.get('firstName') or request.data.get('firstName')
            last_name = request.POST.get('lastName') or request.data.get('lastName')
            position = request.POST.get('position') or request.data.get('position')
            gender = request.POST.get('gender') or request.data.get('gender')
            image_data = request.FILES['image'].read()  
            # Query DynamoDB
            found_registor = UserRepository.find_active_user_by_id(registor_id)
            # Check if any items were found
            if not found_registor:
                return ResponseNotFound(message="Registor not found!")
            device_id = found_registor["device_id"]

           # DynamoDB scan
            found_account = UserRepository.find_by_username(username)
            if found_account:
                return ResponseBadRequest(message="Username is already exist")

            # Encrypt password
            encrypted_password = password_encrypt(password)
            
            # Generate a unique filename
            image_filename = f"{device_id}/{int(time.time() * 1000)}-{username}.jpg"

            # Upload the image to S3
            isSuccess = S3Service.put_object(s3_bucket_employees, image_filename, image_data)
            if not isSuccess:
                return ResponseInternalServerError(message="Upload failure")

            # Indexing face
            collection_id = f'{device_id}-{Prefix.REKOGNITION_COLLECTION_PREFIX.value}'

            index_face_response = RekognitionService.index_face(image_filename, username, collection_id)
            if not index_face_response["isSuccess"]:
                return ResponseInternalServerError(message=index_face_response["message"])

            # Use Face ID as the primary key (id) in DynamoDB
            UserRepository.create_user({
                'id': index_face_response["face_id"],  # Rekognition Face ID as the primary key
                'device_id': device_id,
                'username': username,
                'password': encrypted_password,
                'image': image_filename,
                'creation_time': datetime.now().isoformat(),
                'role': Role.EMPLOYEE.value,
                'status': UserAccountStatus.ACTIVE.value,
                'first_name': first_name,
                'last_name': last_name,
                'position': position,
                'gender': gender,
            })
            
            return ResponseOk(message="User registered successfully!")

        except json.JSONDecodeError:
            return ResponseBadRequest(message="Invalid JSON data")
        except Exception as e:
            print(f"Error: {e}")
            return ResponseInternalServerError(message="Error registering new employee")

    return JsonResponse({'error': 'Invalid request method'}, status=405)

@api_view(['POST'])
def authenticate_employees(request):
    try:
        image_data = request.FILES['image'].read()  
        device_id = request.POST.get('deviceId') or request.data.get('deviceId')
        # Extract image data and generate a unique filename
        collection_id = f'{device_id}-{Prefix.REKOGNITION_COLLECTION_PREFIX.value}'

        # Search for the face in the Rekognition collection
        face_id = RekognitionService.authenticate(collection_id, image_data)
        if not face_id:
            return ResponseUnAuthorized(message="Authentication failed: No matching face found.")
        
        # Check if any items were found
        found_user = UserRepository.find_active_user_by_id(face_id)
        if not found_user:
            return ResponseNotFound(message="User not found!")
        
        if (found_user["device_id"] != device_id):
            return ResponseUnAuthorized(message="User not exist in devices")
        
        # Save history
        HistoryRepository.create_history(
            device_id=device_id, 
            user_information=generate_user_information(found_user), 
            authenticate_with= AuthenticateMethod.FACE_RECOGNITION.value
        )
        
        return ResponseOk(data=format_user(found_user), message=f'Authentication successful: Welcome, {found_user["username"]}')
    
    except ClientError as e:
        return ResponseInternalServerError(message=f'Error retrieving user: {e.response["Error"]["Message"]}')
    
    except Exception as e:
        return ResponseInternalServerError()