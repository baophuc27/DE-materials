import argparse
import datetime
import os
import time
import json
import jwt
import paho.mqtt.client as mqtt
import random

def create_jwt(project_id,private_key_file,algorithm):
    """
    Create a JWT token to establish MQTT connection.
    """
    token = {
        'iat' : datetime.datetime.utcnow(),
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
        'aud' : project_id
    }

    with open(private_key_file, 'r') as f:
        private_key = f.read()
    
    return jwt.encode(token,private_key,algorithm=algorithm)

def error_str(rc):
    """Convert error to a readable string"""
    return "{}: {}".format(rc,mqtt.erro_string(rc))

def on_connect(unused_client,unused_userdata,unused_flags,rc):
    """Callback when connecting a device"""
    print("Connect",error_str(rc))

def on_disconnect(unused_client,unused_userdata,rc):
    """Callback when a device disconnect"""
    print("Disconnect",error_str(rc))

def on_publish(unused_client,unused_userdata,unused_mid):
    """Callback when a message is sent"""
    print("Published")

def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=('IOT pipeline with MQTT'))
    parser.add_argument(
            '--project_id',
            default=os.environ.get('GOOGLE_CLOUD_PROJECT'))
    parser.add_argument(
            '--registry_id', required=True, help='Cloud IoT Core registry id')
    parser.add_argument(
            '--device_id', required=True, help='Cloud IoT Core device id')
    parser.add_argument(
            '--private_key_file',
            required=True, help='Path to private key file.')
    parser.add_argument(
            '--algorithm',
            choices=('RS256', 'ES256'),
            required=True)
    parser.add_argument(
            '--cloud_region', default='us-central1', help='GCP cloud region')
    parser.add_argument(
            '--ca_certs',
            default='roots.pem')
    parser.add_argument(
            '--num_messages',
            type=int,
            default=100,
            help='Number of messages to publish.')
    parser.add_argument(
            '--message_type',
            choices=('event', 'state'),
            default='event',
            required=True)
    parser.add_argument(
            '--mqtt_bridge_hostname',
            default='mqtt.googleapis.com',
            help='MQTT bridge hostname')
    parser.add_argument(
            '--mqtt_bridge_port',
            default=8883,
            type=int,
            help='MQTT bridge port.')

    return parser.parse_args()

def main():
    args = parse_command_line_args()

    client = mqtt.Client(
            client_id=('projects/{}/locations/{}/registries/{}/devices/{}'
                       .format(
                               args.project_id,
                               args.cloud_region,
                               args.registry_id,
                               args.device_id)))
    
    client.username_pw_set(
            username='unused',
            password=create_jwt(
                    args.project_id, args.private_key_file, args.algorithm))

    client.tls_set(ca_certs = args.ca_certs)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.connect(args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    client.loop_start()

    sub_topic = 'events' if args.message_type == 'event' else 'state'

    mqtt_topic = '/devices/{}/{}'.format(args.device_id, sub_topic)

    random.seed(args.device_id)  

    simulated_temp = 10 + random.random() * 20

    if random.random() > 0.5:
        temperature_trend = +1     
    else:
        temperature_trend = -1     
    
    for i in range(1, args.num_messages + 1):

        simulated_temp = simulated_temp + temperature_trend * random.normalvariate(0.01,0.005)
        payload = {"timestamp": int(time.time()), "device": args.device_id, "temperature": simulated_temp}
        print('Publishing message {} of {}: \'{}\''.format(
                i, args.num_messages, payload))
        jsonpayload =  json.dumps(payload,indent=4)
        client.publish(mqtt_topic, jsonpayload, qos=1)

        time.sleep(1 if args.message_type == 'event' else 5)

    client.loop_stop()
    print('Finished.')