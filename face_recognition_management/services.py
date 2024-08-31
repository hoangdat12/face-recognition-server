import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.exceptions import ClientError
import jwt
from datetime import datetime, timedelta, timezone

# Create S3 client
s3_client = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
s3_bucket_employees = os.environ.get('AWS_S3_BUCKET_EMPLOYEES')
# Rekognition
rekognition_client = boto3.client('rekognition', os.environ.get('AWS_REGION'))

class S3Service:
    @staticmethod
    def put_object(s3_bucket, image_filename, image_data):
        try:
            # Upload to S3
            s3_client.put_object(Bucket=s3_bucket, Key=image_filename, Body=image_data)
            return True
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f'Credentials error: {e}')
            return False
        except Exception as e:
            print(f'Error uploading file: {e}')
            return False
        
    @staticmethod
    def presigned_url(bucket_name, file_name, expired_in=3600):
        # URL for download
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ResponseContentDisposition': 'inline'
            },
            ExpiresIn=expired_in
        )
        return url


class RekognitionService:
    @staticmethod
    def create_collection(collection_id):
        try:
            rekognition_client.create_collection(CollectionId=collection_id)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False
        
    @staticmethod
    def authenticate(collection_id, image_data):
        # Search for the face in the Rekognition collection
        search_response = rekognition_client.search_faces_by_image(
            CollectionId=collection_id,
            Image={'Bytes': image_data},
            MaxFaces=1,
            FaceMatchThreshold=90
        )

        if not search_response['FaceMatches']:
            return False
    
        return search_response['FaceMatches'][0]['Face']['FaceId']

    @staticmethod
    def index_face(image_filename, username, collection_id):
        response_message = {
            "isSuccess": False,
            "message": "",
            "face_id": None
        }
        try:
            is_unique_face = rekognition_client.detect_faces(
                Image={'S3Object': {'Bucket': s3_bucket_employees, 'Name': image_filename}},
                Attributes=['ALL']
            )

            if len(is_unique_face['FaceDetails']) != 1:
                response_message["message"] = "Too many faces in one picture!"
                return response_message

            found_face = rekognition_client.search_faces_by_image(
                CollectionId=collection_id,
                Image={'S3Object': {'Bucket': s3_bucket_employees, 'Name': image_filename}},
                MaxFaces=1
            )

            if found_face["FaceMatches"]:
                response_message["message"] = "Face already exists!"
                return response_message

            index_response = rekognition_client.index_faces(
                CollectionId=collection_id,
                Image={'S3Object': {'Bucket': s3_bucket_employees, 'Name': image_filename}},
                ExternalImageId=username  
            )

            if index_response['FaceRecords']:
                response_message["face_id"] = index_response['FaceRecords'][0]['Face']['FaceId']
                response_message["message"] = "Success!"
                response_message["isSuccess"] = True
                return response_message
            else:
                response_message["message"] = "Face indexing failed!"
                return response_message

        except ClientError as e:
            print(f"Error: {e}")
            response_message["message"] = "Face indexing failed!"
            return response_message
        

class TokenService:
    def __init__(self, algorithm='HS256', access_token_lifetime=15, refresh_token_lifetime=1440):
        self.access_token_secret_key=os.environ.get('ACCESS_TOKEN_SECRET_KEY')
        self.refresh_token_secret_key=os.environ.get('REFRESH_TOKEN_SECRET_KEY')
        self.algorithm = algorithm
        self.access_token_lifetime = access_token_lifetime  # in minutes
        self.refresh_token_lifetime = refresh_token_lifetime  # in minutes

    def generate(self, user_data):
        access_token = self.generate_access_token(user_data)
        refresh_token = self.generate_refresh_token(user_data)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token
        }
    
    def verify_access_token(self, token):
        """
        Verify a JWT token and return the payload.
        """
        try:
            payload = jwt.decode(token, self.access_token_secret_key, algorithms=[self.algorithm])
            return {
                "payload": payload,
                "isSuccess": True
            }
        except jwt.ExpiredSignatureError:
            return {
                "payload": "Token expired",
                "isSuccess": False
            }
        except jwt.InvalidTokenError:
            return {
                "payload": "Invalid token",
                "isSuccess": False
            }
        
    def verify_refresh_token(self, token):
        """
        Verify a JWT token and return the payload.
        """
        try:
            payload = jwt.decode(token, self.refresh_token_secret_key, algorithms=[self.algorithm])
            return {
                "payload": payload,
                "isSuccess": True
            }
        except jwt.ExpiredSignatureError:
            return {
                "payload": "Token expired",
                "isSuccess": False
            }
        except jwt.InvalidTokenError:
            return {
                "payload": "Invalid token",
                "isSuccess": False
            }
    

    def generate_access_token(self, user_data):
        """
        Generate a JWT access token.
        """
        payload = {
            'id': user_data.get('id'),
            'username': user_data.get('username'),
            'role': user_data.get('role', 'user'),
            'exp': datetime.now(timezone.utc) + timedelta(minutes=self.access_token_lifetime),
            'iat': datetime.now(timezone.utc),
        }
        return jwt.encode(payload, self.access_token_secret_key, algorithm=self.algorithm)

    def generate_refresh_token(self, user_data):
        """
        Generate a JWT refresh token.
        """
        payload = {
            'id': user_data.get('id'),
            'username': user_data.get('username'),
            'role': user_data.get('role', 'user'),
            'exp': datetime.now(timezone.utc) + timedelta(minutes=self.access_token_lifetime),
            'iat': datetime.now(timezone.utc),
        }
        return jwt.encode(payload, self.refresh_token_secret_key, algorithm=self.algorithm)