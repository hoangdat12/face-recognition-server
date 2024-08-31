import boto3
import os
from ..constants import UserAccountStatus

dynamodb_client = boto3.resource('dynamodb', os.environ.get('AWS_REGION'))

user_table_name = os.environ.get('AWS_DYNAMODB_TABLE_NAME')
user_table = dynamodb_client.Table(user_table_name)

class UserRepository:
    @staticmethod
    def create_user(user_data):
        user_table.put_item(
            Item= user_data
        )

    @staticmethod
    def find_by_id(user_id):
        find_user_response = user_table.query(
            KeyConditionExpression = boto3.dynamodb.conditions.Key('id').eq(user_id)
        )
        if not find_user_response["Items"]:
            return None
                
        return find_user_response["Items"][0];

    @staticmethod
    def find_active_user_by_id(user_id):
        find_user_response = user_table.query(
            KeyConditionExpression = boto3.dynamodb.conditions.Key('id').eq(user_id)
        )
        if not find_user_response["Items"] or find_user_response["Items"][0]["status"] != UserAccountStatus.ACTIVE.value:
            return None
                
        return find_user_response["Items"][0];

    @staticmethod
    def find_by_username(username):
        found_account = user_table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('username').eq(username)
        )
        if not found_account["Items"]:
            return None
        return found_account["Items"][0]
        

    @staticmethod
    def find_exist_device_with_device_id(device_id):
        found_device = user_table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('device_id').eq(device_id)
        )
        if not found_device["Items"]:
            return None
        return found_device["Items"][0]
    
    @staticmethod
    def find_users_device(device_id):
        found_users_device = user_table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('device_id').eq(device_id)
        )
        return found_users_device["Items"];

    @staticmethod
    def update_user_status(user):
        response = user_table.update_item(
            Key={
                'id': user["id"]  # Thay bằng khóa chính của bạn
            },
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={
                '#status': 'status'  # Sử dụng tên đại diện cho thuộc tính
            },
            ExpressionAttributeValues={
                ':status': user["status"]  # Thay bằng giá trị mới
            },
            ReturnValues='UPDATED_NEW'  # Trả về các thuộc tính đã cập nhật
        )

        return response
    
    @staticmethod
    def disable_users_in_device_batch(users):
        with user_table.batch_writer() as batch:
            for user in users:
                user["status"] = UserAccountStatus.DELETED.value
                batch.put_item(Item=user)

    @staticmethod
    def save(user):
        response = user_table.put_item(Item=user)
        return response