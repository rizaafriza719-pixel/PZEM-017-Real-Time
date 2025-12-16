# PZEM-017 reader
# by nyoman yudi kurniawan (adapted)
# Install: pip install minimalmodbus pyserial paho-mqtt

import minimalmodbus
import serial
import time
import json
import paho.mqtt.client as mqtt

# Configuration - adjust as needed
SERIAL_PORT = 'COM5'  # change to your COM port
SLAVE_ADDR = 1
BAUDRATE = 9600
MQTT_BROKER = 'test.mosquitto.org'
MQTT_PORT = 1883
MQTT_TOPIC_PREFIX = 'pzem'

# Initialize PZEM instrument
instrument = minimalmodbus.Instrument(SERIAL_PORT, SLAVE_ADDR)
instrument.serial.baudrate = BAUDRATE
instrument.serial.bytesize = 8
instrument.serial.parity = serial.PARITY_NONE
instrument.serial.stopbits = 1
instrument.serial.timeout = 1
instrument.mode = minimalmodbus.MODE_RTU
instrument.debug = False

# Read once or publish periodically
def read_pzem():
    try:
        voltage = instrument.read_register(0x0000, 2, functioncode=4)
        current = instrument.read_register(0x0001, 3, functioncode=4)
        power = instrument.read_register(0x0002, 1, functioncode=4) / 10.0
        # energy is 32-bit unsigned long at 0x0004, functioncode 4 - use read_long
        energy = instrument.read_long(0x0004, functioncode=4) / 1000.0

        return {
            'timestamp': time.time(),
            'voltage': voltage,
            'current': current,
            'power': power,
            'energy': energy
        }
    except Exception as e:
        print('Failed to read PZEM:', e)
        return None

# MQTT publish loop
client = mqtt.Client()

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    while True:
        data = read_pzem()
        if data:
            payload = json.dumps(data)
            client.publish(f"{MQTT_TOPIC_PREFIX}/all_data", payload)
            client.publish(f"{MQTT_TOPIC_PREFIX}/voltage", json.dumps({'value': data['voltage']}))
            client.publish(f"{MQTT_TOPIC_PREFIX}/current", json.dumps({'value': data['current']}))
            client.publish(f"{MQTT_TOPIC_PREFIX}/power", json.dumps({'value': data['power']}))
            client.publish(f"{MQTT_TOPIC_PREFIX}/energy", json.dumps({'value': data['energy']}))
            print('Published:', payload)

except KeyboardInterrupt:
    print('\nStopping...')
finally:
    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass
