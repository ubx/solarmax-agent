import socket
import sys
import time

import paho.mqtt.publish as publish

## This agent reads from the solarmax inverter socket and
## publishes the data to an MQTT listener.

# Constants
inverter_ip = "192.168.1.90"
inverter_port = 12345

mqtt_broker_ip = "192.168.1.24"
mqtt_broker_port = 1883
mqtt_broker_url = f"http://{mqtt_broker_ip}:{mqtt_broker_port}"

mqtt_topic = "/data/inverter"

iot_broker = "k4cp0.messaging.internetofthings.ibmcloud.com"
iot_port = 1883
iot_topic = "iot-2/evt/status/fmt/json"

IDC = "IDC"  # DC Current
UL1 = "UL1"  # Voltage Phase 1
TKK = "TKK"  # Inverter operating temp
IL1 = "IL1"  # Current phase 1
SYS = "SYS"  # 4E28 = 17128
TNF = "TNF"  # Generated frequency (Hz)
UDC = "UDC"  # DC voltage (V DC)
PAC = "PAC"  # AC power being generated * 2 (W)
PRL = "PRL"  # Relative output (%)
KT0 = "KT0"  # Total yield (kWh)

field_map = {
    IDC: 'dc_current',
    UL1: 'voltage_phase1',
    TKK: 'inverter_temp',
    IL1: 'current_phase1',
    SYS: 'sys',
    TNF: 'frequency',
    UDC: 'dc_voltage',
    PAC: 'power_output',
    PRL: 'relative_output',
    KT0: 'total_yield'
}

req_data = "{FB;01;3E|64:IDC;UL1;TKK;IL1;SYS;TNF;UDC;PAC;PRL;KT0;SYS|0F66}"


def publish_message(topic, payload):
    """Publish the message to the MQTT broker."""
    try:
        publish.single(
            topic,
            payload,
            hostname=iot_broker,
            client_id="d:k4cp0:raspberrypi:b827ebc2478d",
            auth={"username": "use-token-auth", "password": "Sz2(u_6!+h_MIe@&7Z"}
        )
    except Exception as ex:
        print(f"Publish.single: An exception of type {type(ex).__name__} occurred. Arguments: {ex.args}")
        raise


def gen_data(s):
    """Convert a pair: <field>=<0xdata> to a list with name mapping and value scaling."""
    t = s.split('=')
    f = t[0]

    if f == SYS:  # Remove the trailing ,0
        v = int(t[1][:t[1].find(',')], 16)
    else:
        v = int(t[1], 16)

    if f == PAC:  # PAC values are *2
        v = v / 2

    if f in [UL1, UDC]:  # Voltage levels need to be divided by 10
        v = v / 10.0

    if f in [IDC, TNF]:  # Current & frequency need to be divided by 100
        v = v / 100.0

    return [field_map[f], v]


def convert_to_json(data):
    """Convert the inverter message to JSON."""
    ev = [gen_data(s) for s in data[data.find(':') + 1:data.find('|', data.find(':'))].split(';')]
    out_str = '{ "d": { '  # Format to satisfy IBM IoT conventions
    for e in ev:
        out_str += f'"{e[0]}" : {e[1]},'
    out_str = out_str.rstrip(',') + '} }'  # Remove trailing comma and close braces
    return out_str


def check_msg(msg):
    """Check that the message is valid."""
    return msg


def publish_data(data):
    print(f'Publishing: {data}\n')
    json_data = convert_to_json(data)
    print(f'Published: {json_data}\n')
    publish_message(iot_topic, json_data)


def connect_to_inverter():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((inverter_ip, inverter_port))
    except socket.error as msg:
        print(f'Failed to create socket: Error: {msg}')
        sys.exit()
    return s


def read_data(sock, request):
    sock.send(request.encode('utf-8'))
    data_received = False
    response = ""
    while not data_received:
        buf = sock.recv(1024)
        if len(buf) > 0:
            response += buf.decode('utf-8')
            data_received = True

    response = check_msg(response)
    return response


def main():
    print("Starting...")
    inv_s = connect_to_inverter()
    print("Connected...")
    try:
        while True:
            data = read_data(inv_s, req_data)
            publish_data(data)
            time.sleep(10)
    except Exception as ex:
        print(f"An exception of type {type(ex).__name__} occurred. Arguments: {ex.args}")
    finally:
        inv_s.close()


if __name__ == "__main__":
    main()
