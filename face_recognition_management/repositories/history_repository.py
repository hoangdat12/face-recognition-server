import boto3
import os
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import random
import string

dynamodb_client = boto3.resource('dynamodb', os.environ.get('AWS_REGION'))
# DynamoDB table name
history_table_name = os.environ.get('AWS_DYNAMODB_TABLE_HISTORY')
history_table = dynamodb_client.Table(history_table_name)

def generate_random_string(length=10):
        """Generate a random string of fixed length."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_history_item(device_id, item_number):
        """Create a history item with a specific device ID and item number."""
        timestamp = datetime.now() - timedelta(days=item_number)  # Create timestamps from the past
        created_at = timestamp.strftime('%Y-%m-%dT%H:%M:%S')  # Format as ISO 8601 string
        item = {
            'id': device_id,
            'created_at': created_at,
            'employee_information': {
                'id': generate_random_string(),
                'name': f'Employee {item_number}',
                'image': f'image_{item_number}.jpg'
            },
            'updated_at': created_at  # Just for example; typically updated_at would be different
        }
        return item

class HistoryRepository:
    @staticmethod
    def create_history(user_id, user_information, authenticate_with, status = "Check In"):
        timestamp = datetime.now() 
        created_at = timestamp.strftime('%Y-%m-%dT%H:%M:%S')  
        created_date = timestamp.strftime('%Y-%m-%d')
        item = {
            'id': user_id,
            'created_at': created_at,
            'authenticate_with': authenticate_with,
            'employee_information': user_information,
            'created_date': created_date,
            'check_in': True, # Present or absent
            'status': status
        }
        history_table.put_item(Item=item)
        return item

    @staticmethod
    def get_history_of_device(device_id, limit=20, page=1, start_key=None):
        # Tính số lượng mục cần bỏ qua
        start_from = (page - 1) * limit

        if page > 1 and not start_key:
            # Bước 1: Truy vấn để lấy đủ số lượng mục để bỏ qua
            response = history_table.query(
                KeyConditionExpression=Key('id').eq(device_id),
                ScanIndexForward=False,  # Sắp xếp giảm dần theo created_at
                Limit=start_from + limit  # Lấy số lượng mục để bỏ qua và thêm limit
            )
            
            # Nếu không có đủ dữ liệu để lấy số lượng cần thiết
            if not response['Items']:
                return [], None
            
            # Lấy LastEvaluatedKey từ kết quả, nếu có
            start_key = response.get('LastEvaluatedKey', None)
            
            if start_key is None and len(response['Items']) < start_from + limit:
                # Nếu không có LastEvaluatedKey và số lượng mục ít hơn start_from + limit, nghĩa là không đủ dữ liệu
                return [], None
            
        # Bước 2: Truy vấn từ phần tử thứ (start_from) trở đi
        query_params = {
            'KeyConditionExpression': Key('id').eq(device_id),
            'ScanIndexForward': False,  # Sắp xếp giảm dần theo created_at
            'Limit': limit,  # Giới hạn số lượng mục trả về
        }

        if start_key:
            query_params['ExclusiveStartKey'] = start_key
        
        response = history_table.query(**query_params)
        
        items = response['Items']
        last_evaluated_key = response.get('LastEvaluatedKey', None)
        
        return items, last_evaluated_key

    @staticmethod
    def get_history_by_date(device_id, date, limit=20, page=1, start_key=None):
        # Tính số lượng mục cần bỏ qua
        start_from = (page - 1) * limit

        start_date = f"{date}T00:00:00"
        end_date = f"{date}T23:59:59"

        if page > 1 and not start_key:
            response = history_table.query(
                KeyConditionExpression=Key('id').eq(device_id) & Key('created_at').between(start_date, end_date),
                ScanIndexForward=False,  
                Limit=start_from + limit  
            )
            
            # Nếu không có đủ dữ liệu để lấy số lượng cần thiết
            if not response['Items']:
                return [], None
            
            # Lấy LastEvaluatedKey từ kết quả, nếu có
            start_key = response.get('LastEvaluatedKey', None)
            
            if start_key is None and len(response['Items']) < start_from + limit:
                # Nếu không có LastEvaluatedKey và số lượng mục ít hơn start_from + limit, nghĩa là không đủ dữ liệu
                return [], None

        # Bước 2: Truy vấn từ phần tử thứ (start_from) trở đi
        query_params = {
            'KeyConditionExpression': Key('id').eq(device_id) & Key('created_at').between(start_date, end_date),
            'ScanIndexForward': False,  # Sắp xếp giảm dần theo created_at
            'Limit': limit,  # Giới hạn số lượng mục trả về
        }
        
        if start_key:
            query_params['ExclusiveStartKey'] = start_key
        
        response = history_table.query(**query_params)
        
        items = response['Items']
        last_evaluated_key = response.get('LastEvaluatedKey', None)
        
        return items, last_evaluated_key
    
    @staticmethod
    def get_histories_by_date(date):
        histories = history_table.query(
            IndexName='created_date-created_at-index', 
            KeyConditionExpression=Key('created_date').eq(date)
        )
        return histories.get('Items', [])
    
    @staticmethod
    def get_histories_detail(user_id, date_query):
        histories = history_table.scan(
            FilterExpression="employee_information.id = :eid AND created_date = :cdate",
            ExpressionAttributeValues={
                ":eid": user_id,
                ":cdate": date_query
            }
        )
        return histories.get('Items', [])
    
    @staticmethod
    def get_latest_record(user_id, created_at):
        start_date = f"{created_at}T00:00:00"
        end_date = f"{created_at}T23:59:59"

        try:
            response = history_table.query(
                KeyConditionExpression=Key('id').eq(user_id) & Key('created_at').between(start_date, end_date),
                ScanIndexForward=False,  # Sort in descending order to get the most recent record first
                Limit=1  # Only get the most recent record of the day
            )

            if response['Items']:
                return response['Items'][0]  # Return the most recent record of the day
            else:
                print("No data found for the day")
                return None
            
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def generate_test_data(device_id, number_of_items=50):
        """Generate test data and put items into DynamoDB."""
        for i in range(number_of_items):
            item = create_history_item(device_id, i + 1)
            response = history_table.put_item(Item=item)