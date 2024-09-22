from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
from awscrt.exceptions import AwsCrtError
import jwt
import sys
import time
import os
import boto3
import json

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
        
class AwsIoTService:
    mqtt_connection = None  # Class-level attribute

    # AWS IoT endpoint and file paths for certificates 
    TARGET_EP = os.environ.get('AWS_IOT_TARGET_EP')
    CLAIM_CERT_FILEPATH = os.path.join(os.getcwd(), 'face_recognition_management/secret/a5dae91d0da844e7c555593ff16a7b23f148b76e569cc507531332e1952b4043-certificate.pem.crt')
    CLAIM_PRIVATE_KEY_FILE_PATH = os.path.join(os.getcwd(), 'face_recognition_management/secret/a5dae91d0da844e7c555593ff16a7b23f148b76e569cc507531332e1952b4043-private.pem.key')
    CA_FILEPATH = os.path.join(os.getcwd(), 'face_recognition_management/secret/AmazonRootCA1.pem')

    # AWS Resources name
    POLICY_NAME = os.environ.get('AWS_IOT_POLICY_NAME')
    PROVISIONING_TEMPLATE_NAME = os.environ.get('AWS_IOT_PROVISIONING_TEMPLATE_NAME')
    AWS_REGION = os.environ.get('AWS_REGION')
    AWS_IOT_THING_GROUP_NAME=os.environ.get('AWS_IOT_THING_GROUP_NAME')

    # Setup for AWS IoT MQTT connection
    EVENT_LOOP_GROUP = io.EventLoopGroup(1)
    HOST_RESOLVER = io.DefaultHostResolver(EVENT_LOOP_GROUP)
    CLIENT_BOOTSTRAP = io.ClientBootstrap(EVENT_LOOP_GROUP, HOST_RESOLVER)

    @staticmethod
    def generate_certificate(device_id):
        """
        Generate a new certificate by connecting with the claim certificate, 
        requesting new keys and certificate from AWS IoT Core, and attaching 
        the new certificate to a thing and policy.
        
        Returns:
            bool: True if the process was successful, False otherwise.
        """
        thing_name = f"{AwsIoTService.PROVISIONING_TEMPLATE_NAME}_{device_id}"

        # Connect to IoT Core using the claim certificate
        if not AwsIoTService.mqtt_connection:
            AwsIoTService.connect_mqtt()

        # Create an IoT client to request new keys and certificate
        iot_client = boto3.client('iot', region_name=AwsIoTService.AWS_REGION)

        # Request new keys and certificate from AWS IoT Core
        response = iot_client.create_keys_and_certificate(setAsActive=True)

        # Extract the new certificate and private key
        new_cert_arn = response['certificateArn']
        new_cert_pem = response['certificatePem']
        new_key_pem = response['keyPair']['PrivateKey']
        new_public_key_pem = response['keyPair']['PublicKey']

        # Attach the new certificate to a thing
        iot_client.create_thing(thingName=thing_name)
        iot_client.attach_thing_principal(
            thingName=thing_name,
            principal=new_cert_arn
        )

        # Attach policy to the new certificate
        iot_client.attach_policy(
            policyName=AwsIoTService.POLICY_NAME,
            target=new_cert_arn
        )

         # Add the thing to a thing group
        try:
            iot_client.add_thing_to_thing_group(
                thingGroupName=AwsIoTService.AWS_IOT_THING_GROUP_NAME,
                thingName=thing_name
            )
            
        except Exception as e:
            print(f"Failed to add {thing_name} to group {AwsIoTService.AWS_IOT_THING_GROUP_NAME}. Error: {e}")
            return None
        
         # Create and update the device shadow
        shadow_update = {
            "state": {
                "reported": {
                    "fan": {
                        "status": "off",  # Possible values: "on", "off"
                        "speed": 3  # Fan speed (e.g., 1-5)
                    },
                    "door": {
                        "status": "off"  # Possible values: "on", "off"
                    },
                    "light": {
                        "status": "on",  # Possible values: "on", "off"
                        "brightness": 75,  # Light brightness percentage (0-100)
                        "automate": "off",  # Possible values: "on", "off"
                        "default_value": "19" # Possible values: 0 - 24 - The time to turn on Light
                    }
                }
            }
        }
        shadow_topic = f"$aws/things/{thing_name}/shadow/update"
        
        AwsIoTService.mqtt_connection.publish(
            topic=shadow_topic,
            payload=json.dumps(shadow_update),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )
        
        response_data = {
            "new_cert_arn": new_cert_arn,
            "new_cert_pem": new_cert_pem,
            "new_key_pem": new_key_pem,
            "new_public_key_pem": new_public_key_pem
        }

        return response_data
    
    @staticmethod
    def publish_message(topic, message):
        """
        Phương thức này publish tin nhắn JSON lên một topic MQTT.
        
        :param topic: Topic MQTT mà bạn muốn gửi tin nhắn tới
        :param message: Dữ liệu tin nhắn, dưới dạng dictionary (sẽ được chuyển thành JSON)
        """
        print(AwsIoTService.mqtt_connection)
        if not AwsIoTService.mqtt_connection:
            AwsIoTService.connect_mqtt()

        try:
            # Attempt to publish the message
            AwsIoTService.mqtt_connection.publish(
                topic=topic,
                payload=json.dumps(message),  # Convert message dictionary to JSON string
                qos=mqtt.QoS.AT_LEAST_ONCE    # Set QoS to ensure the message is delivered at least once
            )
            print(f"Published message to {topic}: {json.dumps(message)}")
            return True
        
        except AwsCrtError as e:
            # Catch any errors related to AWS IoT or MQTT
            print(f"Failed to publish message to {topic}: {str(e)}")
            return False

        except Exception as e:
            # Catch any other general exceptions
            print(f"An unexpected error occurred: {str(e)}")
            return False
        
        return True

    @staticmethod
    def connect_mqtt():
        if AwsIoTService.mqtt_connection is not None:
            # Check if the connection is still active
            if AwsIoTService.mqtt_connection.is_connected():
                return AwsIoTService.mqtt_connection
            
        AwsIoTService.mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=AwsIoTService.TARGET_EP,
            port=8883,
            cert_filepath=AwsIoTService.CLAIM_CERT_FILEPATH,
            pri_key_filepath=AwsIoTService.CLAIM_PRIVATE_KEY_FILE_PATH,
            client_bootstrap=AwsIoTService.CLIENT_BOOTSTRAP,
            ca_filepath=AwsIoTService.CA_FILEPATH,
            on_connection_interrupted=AwsIoTService.on_connection_interrupted,
            on_connection_resumed=AwsIoTService.on_connection_resumed,
            client_id=AwsIoTService.PROVISIONING_TEMPLATE_NAME,
            clean_session=True,
            keep_alive_secs=30
        )

        max_retries = 5
        attempt = 0

        while attempt < max_retries:
            try:
                connect_future = AwsIoTService.mqtt_connection.connect()
                connect_future.result()  # Wait until connected
            except Exception as e:  # Log the exception details
                attempt += 1
                print(f"Connection to IoT Core failed (attempt {attempt}/{max_retries})... retrying in 5s. Error: {str(e)}")
                time.sleep(5)
                if attempt >= max_retries:
                    print("Max retries reached. Exiting.")
                    return None  # Return None on failure
            else:
                AwsIoTService.mqtt_connection = AwsIoTService.mqtt_connection
                return AwsIoTService.mqtt_connection  # Return the successful connection
        
        return None

    @staticmethod
    def on_connection_interrupted(connection, error, **kwargs):
        """
        Callback for when the connection is interrupted.
        
        Args:
            connection (awsiot.mqtt.MqttConnection): The connection that was interrupted.
            error (Exception): The error that caused the interruption.
        """
        print("Connection interrupted. error: {}".format(error))

    @staticmethod
    def on_connection_resumed(connection, return_code, session_present, **kwargs):
        """
        Callback for when the connection is resumed.
        
        Args:
            connection (awsiot.mqtt.MqttConnection): The connection that was resumed.
            return_code (int): The return code from the server.
            session_present (bool): Whether the session was present.
        """
        print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            print("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = connection.resubscribe_existing_topics()
            resubscribe_future.add_done_callback(AwsIoTService.on_resubscribe_complete)

    @staticmethod
    def on_resubscribe_complete(resubscribe_future):
        """
        Callback for when resubscription to topics is complete.
        
        Args:
            resubscribe_future (concurrent.futures.Future): The future representing the resubscription result.
        """
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

    @staticmethod
    def on_message_received(topic, payload, dup, qos, retain, **kwargs):
        """
        Callback for when a message is received on a subscribed topic.
        
        Args:
            topic (str): The topic on which the message was received.
            payload (bytes): The message payload.
            dup (bool): Whether the message is a duplicate.
            qos (awsiot.mqtt.QoS): The Quality of Service level.
            retain (bool): Whether the message is retained.
        """
        print("Received message from topic '{}': {}".format(topic, payload))
        

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