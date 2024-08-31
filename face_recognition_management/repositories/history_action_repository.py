import boto3
import os
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
dynamodb_client = boto3.resource('dynamodb', os.environ.get('AWS_REGION'))
# DynamoDB table name
history_action_table_name = os.environ.get('AWS_DYNAMODB_TABLE_HISTORY_ACTION')
history_action_table = dynamodb_client.Table(history_action_table_name)

class HistoryActionRepository:
    @staticmethod
    def create_history(device_id, user_id, action):
        device_id_user_id = f"{device_id}#{user_id}"
        timestamp = datetime.now() 
        created_at = timestamp.strftime('%Y-%m-%dT%H:%M:%S')  
        item = {
            'device_id_user_id': device_id_user_id,
            'created_at': created_at,
            'action': action,
            'updated_at': created_at  
        }
        history_action_table.put_item(Item=item)
        return item
    
    @staticmethod
    def get_history(device_id, user_id, date):
        device_id_user_id = f"{device_id}#{user_id}"

        start_date = f"{date}T00:00:00"
        end_date = f"{date}T23:59:59"

        response = history_action_table.query(
            KeyConditionExpression=Key('device_id_user_id').eq(device_id_user_id) &
                                Key('created_at').between(start_date, end_date),
            ScanIndexForward=True  # Ascending order (oldest to newest)
        )

        return response['Items']

   