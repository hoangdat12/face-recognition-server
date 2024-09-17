import asyncio
import websockets
import binascii
from io import BytesIO
from PIL import Image, UnidentifiedImageError
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import time
import os
import threading

# Configuration for AWS IoT Core
target_ep = 'aibvj7aq7b6hf-ats.iot.ap-southeast-1.amazonaws.com'
thing_name = 'face_recognition_root_thing'
cert_filepath = os.path.join(os.getcwd(), 'a5dae91d0da844e7c555593ff16a7b23f148b76e569cc507531332e1952b4043-certificate.pem.crt')
private_key_filepath = os.path.join(os.getcwd(), 'a5dae91d0da844e7c555593ff16a7b23f148b76e569cc507531332e1952b4043-private.pem.key')
ca_filepath = os.path.join(os.getcwd(), 'AmazonRootCA1.pem')

sub_topic = 'app/data'

# Global variables
received_all_event = threading.Event()
mqtt_message = None

# Function to check if the image is valid
def is_valid_image(image_bytes):
    try:
        Image.open(BytesIO(image_bytes))
        return True
    except UnidentifiedImageError:
        print("Image invalid")
        return False

# MQTT callbacks
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    global mqtt_message
    mqtt_message = payload
    print("Received message from topic '{}': {}".format(topic, payload))

# WebSocket handling
async def handle_connection(websocket, path):
    global mqtt_message
    while True:
        try:
            if mqtt_message:
                message = mqtt_message
                mqtt_message = None  # Reset the message after processing

                print(len(message))
                if len(message) > 5000:
                    if is_valid_image(message):
                        with open("image.jpg", "wb") as f:
                            f.write(message)
                print()
            
            # WebSocket message processing
            message = await websocket.recv()
            print(f"Received WebSocket message of length: {len(message)} bytes")

            if len(message) > 5000:
                if is_valid_image(message):
                    with open("image.jpg", "wb") as f:
                        f.write(message)
        except websockets.exceptions.ConnectionClosed:
            break

# MQTT client setup
def setup_mqtt_client():
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    proxy_options = None

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=target_ep,
        port=8883,
        cert_filepath=cert_filepath,
        pri_key_filepath=private_key_filepath,
        client_bootstrap=client_bootstrap,
        ca_filepath=ca_filepath,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=thing_name,
        clean_session=True,
        keep_alive_secs=30,
        http_proxy_options=proxy_options)

    print("Connecting to {} with client ID '{}'...".format(target_ep, thing_name))

    while True:
        try:
            connect_future = mqtt_connection.connect()
            connect_future.result()
        except Exception as e:
            print("Connection to IoT Core failed... retrying in 5s.")
            time.sleep(5)
            continue
        else:
            print("Connected!")
            break

    print("Subscribing to topic " + sub_topic)
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=sub_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))
    
    return mqtt_connection

# Main function to start WebSocket and MQTT
async def main():
    # Setup MQTT client
    mqtt_connection = setup_mqtt_client()

    # Start WebSocket server
    ws_server = await websockets.serve(handle_connection, '0.0.0.0', 3001)
    
    try:
        await asyncio.Event().wait()  # Keep the event loop running
    except KeyboardInterrupt:
        print("Disconnecting...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Disconnected!")

# Run the main function
asyncio.run(main())
