import random
import string
import bcrypt
from boto3.dynamodb.types import Binary
from datetime import datetime
from ..services import S3Service
import os

s3_bucket_employees = os.environ.get('AWS_S3_BUCKET_EMPLOYEES')

def password_encrypt(password):
    # Encrypt the password using bcrypt
    bytes = password.encode('utf-8') 
    salt = bcrypt.gensalt() 
    return bcrypt.hashpw(bytes, salt) 

def check_password(plain_password, hashed_password):
    if isinstance(hashed_password, Binary):
        hashed_password = hashed_password.value
    if isinstance(hashed_password, str):
        hashed_password = hashed_password.encode('utf-8')
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def random_value(length):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def format_user(user_data):
    if 'password' in user_data:
        del user_data["password"]
    return user_data;

def format_date(date_str):
    if (date_str == None): return date_str
    formats = ['%Y-%m-%d', '%Y/%m/%d']
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

# Hàm kiểm tra ngày hợp lệ
def is_valid_date(date_str):
    if not date_str:
        return False
    formats = ['%Y-%m-%d', '%Y/%m/%d']
    for fmt in formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    return False

# Hàm lấy start_key
def get_start_key(start_key_str, device_id=None):
    if start_key_str:
        return {
            "id": device_id,
            "created_at": start_key_str
        }
    return None

def generate_user_information(user_device):
    return {
        "id": user_device["id"],
        "name": f"{user_device['first_name']} {user_device['last_name']}",
        "image": user_device["image"]
    }

def get_histories_response(histories):
    response_data = {
        "histories": [],
        "start_key": histories[1]
    }
    # Remember these user is occur in history response array 
    memo = set()

    for history in histories[0]:
        date_str = history["created_at"].split('T')[0]  # Kết quả là '2024-07-07'
        key = f"${history['employee_information']['id']}_${date_str}"

        if key not in memo:
            history["quantity"] = history.get("quantity", 0) + 1
            history["employee_information"]["image"] = S3Service.presigned_url(
                bucket_name=s3_bucket_employees, 
                file_name=history["employee_information"]["image"]
            )
            response_data["histories"].append(history)
            memo.add(key)

    return response_data;