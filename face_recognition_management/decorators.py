# decorators.py
from functools import wraps
from django.http import JsonResponse
import jwt
import boto3
import os
from .services import TokenService
from .constants import Role

dynamodb_client = boto3.resource('dynamodb', os.environ.get('AWS_REGION'))
# DynamoDB table name
user_table_name = os.environ.get('AWS_DYNAMODB_TABLE_NAME')

# DynamoDB table
user_table = dynamodb_client.Table(user_table_name)

def permission(allowed_roles=None):
    if allowed_roles is None:
        allowed_roles = [Role.SUPER.value, Role.ADMIN.value, Role.HOST.value, Role.EMPLOYEE.value]  # Default to "USER" role
    
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            token_service = TokenService()
            token = request.headers.get('Authorization')
            if token:
                try:
                    if token.startswith('Bearer '):
                        token = token[7:]

                    verify_token_response = token_service.verify_access_token(token)
                    if not verify_token_response["isSuccess"]:
                        return JsonResponse({'message': verify_token_response["payload"]}, status=401)
                    
                    payload = verify_token_response['payload']

                    find_user_response = user_table.query(
                        KeyConditionExpression=boto3.dynamodb.conditions.Key('id').eq(payload["id"])
                    )

                    if not find_user_response["Items"]:
                        return JsonResponse({"message": "User not found!"}, status=404)
                    
                    found_user = find_user_response["Items"][0]
                    request.user = found_user

                    user_role = found_user.get('role')

                    if user_role not in allowed_roles:
                        return JsonResponse({'message': 'Permission denied'}, status=403)

                except jwt.ExpiredSignatureError:
                    return JsonResponse({'message': 'Token expired'}, status=401)
                except jwt.InvalidTokenError:
                    return JsonResponse({'message': 'Invalid token'}, status=401)
            else:
                return JsonResponse({'message': 'Authorization header missing'}, status=401)

            return view_func(request, *args, **kwargs)

        return _wrapped_view
    return decorator


def verify_token(view_func):
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        token_service = TokenService()
        token = request.headers.get('Authorization')
        if token:
            try:
                # Assuming 'Bearer ' prefix is used
                if token.startswith('Bearer '):
                    token = token[7:]

                # Verify Access Token
                verify_token_response = token_service.verify_access_token(token)
                if not verify_token_response["isSuccess"]:
                    return JsonResponse({'message': verify_token_response["payload"]}, status=401) 
                
                payload = verify_token_response['payload']


                # Find user in db
                find_user_response = user_table.query(
                    KeyConditionExpression = boto3.dynamodb.conditions.Key('id').eq(payload["id"])
                )
                if not find_user_response["Items"]:
                    return JsonResponse({"message": "User not found!"})
                
                found_user = find_user_response["Items"][0];

                # Add to request
                request.user = found_user
            
            except jwt.ExpiredSignatureError:
                return JsonResponse({'message': 'Token expired'}, status=401)
            except jwt.InvalidTokenError:
                return JsonResponse({'message': 'Invalid token'}, status=401)
        else:
            return JsonResponse({'message': 'Authorization header missing'}, status=401)

        return view_func(request, *args, **kwargs)

    return _wrapped_view
