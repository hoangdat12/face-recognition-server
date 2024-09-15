import boto3
import os
from datetime import datetime
from ..constants import DeviceStatus

dynamodb_client = boto3.resource('dynamodb', os.environ.get('AWS_REGION'))
# DynamoDB table name
device_id_table_name = os.environ.get('AWS_DYNAMODB_TABLE_DEVICE_ID')
device_id_table = dynamodb_client.Table(device_id_table_name)

    
class DeviceRepository:
    @staticmethod
    def batch_device_id(device_ids):
        current_time = datetime.now().isoformat()
        with device_id_table.batch_writer() as batch:
            for device_id in device_ids:
                batch.put_item(
                    Item={
                        'device_id': device_id,
                        'creation_date': current_time,  
                        'status': DeviceStatus.ACTIVE.value,
                        'device_informations': [{
                            'name': 'light',
                            'is_automate': False,
                            "default_value": '0730',
                            'title': 'Auto turn on Light'
                        },
                        {
                            'name': 'temperature',
                            "is_automate": False,
                            'default_value': '35',
                            'title': 'Auto adjust Temperature'
                        }]
                    }
                )

    @staticmethod
    def find_by_device_id(device_id):
        found_device_id = device_id_table.query(
            KeyConditionExpression = boto3.dynamodb.conditions.Key('device_id').eq(device_id)
        )
        if not found_device_id["Items"]:
            return False
        return found_device_id["Items"][0]
    
    @staticmethod
    def find_active_by_device_id(device_id):
        found_device_id = device_id_table.query(
            KeyConditionExpression = boto3.dynamodb.conditions.Key('device_id').eq(device_id)
        )
        if not found_device_id["Items"] or found_device_id["Items"][0]["status"] != DeviceStatus.ACTIVE.value:
            return False
        return found_device_id["Items"][0]
    
    @staticmethod
    def update_device_status(device):
        response = device_id_table.update_item(
            Key={
                'device_id': device["device_id"]  # Thay bằng khóa chính của bạn
            },
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={
                '#status': 'status'  # Tên đại diện cho thuộc tính bị xung đột
            },
            ExpressionAttributeValues={
                ':status': device["status"]  # Thay bằng giá trị mới
            },
            ReturnValues='UPDATED_NEW'  # Trả về các thuộc tính đã cập nhật
        )

        return response
    
    @staticmethod
    def update_device_information(device_id, device_informations):
        response = device_id_table.update_item(
            Key={
                'device_id': device_id
            },
            UpdateExpression="SET device_informations = :di",
            ExpressionAttributeValues={
                ':di': device_informations
            },
            ReturnValues="UPDATED_NEW"
        )

        return response
    
    @staticmethod
    def save(device):
        # Check if required fields are present
        required_fields = ["device_id", "device_informations", "status", "private_key", "certificate", "public_key"]
        for field in required_fields:
            if field not in device:
                raise ValueError(f"'{field}' must be provided.")

        try:
            # Update multiple attributes in DynamoDB
            response = device_id_table.update_item(
                Key={
                    'device_id': device["device_id"]  # Assuming 'device_id' is your primary key
                },
                UpdateExpression="""SET device_informations = :information, 
                                    #status = :status, 
                                    private_key = :private_key, 
                                    certificate = :certificate, 
                                    public_key = :public_key""",
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':information': device["device_informations"],
                    ':status': device["status"],
                    ':private_key': device["private_key"],
                    ':certificate': device["certificate"],
                    ':public_key': device["public_key"]
                },
                ReturnValues="UPDATED_NEW"
            )

            return response
        
        except Exception as e:
            print(f"Failed to update device: {str(e)}")
            return None  # Return None on failure

