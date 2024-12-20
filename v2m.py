import asyncio
import json
import logging
import os
import signal
import sys
from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt

# Constants
CURRENT_TEMPERATURE_UUID = "10110001-5354-4f52-5a26-4249434b454c"
MQTT_TOPICS = {
    "connect": "homeassistant/button/volcano_connect/set",
    "disconnect": "homeassistant/button/volcano_disconnect/set",
}
TEMPERATURE_TOPIC = "homeassistant/sensor/volcano_vaporiser_current_temperature/state"
CONNECT_TIMEOUT = 10
DISCOVERY_PREFIX = "homeassistant"

CONFIG_FILE = "volcano_config.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VolcanoMQTT")

# Bluetooth client state
bluetooth_client = None


class MessageQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def put(self, task):
        await self.queue.put(task)

    async def process(self):
        while True:
            coro = await self.queue.get()
            try:
                await coro
            except Exception as e:
                logger.error(f"Error processing task: {e}")
            finally:
                self.queue.task_done()


async def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


def on_connect(client, userdata, flags, rc):
    logger.info("MQTT Connected.")
    for topic in MQTT_TOPICS.values():
        client.subscribe(topic)


async def mqtt_listener(client, message_queue, loop):
    def on_message(client, userdata, msg):
        logger.info(f"MQTT Message Received: {msg.topic}")
        if msg.topic == MQTT_TOPICS["connect"]:
            loop.call_soon_threadsafe(asyncio.create_task, message_queue.put(connect_bluetooth(client)))
        elif msg.topic == MQTT_TOPICS["disconnect"]:
            loop.call_soon_threadsafe(asyncio.create_task, message_queue.put(disconnect_bluetooth()))

    client.on_message = on_message


def clear_retained_message(mqtt_client, topic):
    """Clear retained message for a specific MQTT topic."""
    mqtt_client.publish(topic, payload=None, retain=True)
    logger.info(f"Cleared retained message for topic {topic}")


async def publish_auto_discovery(mqtt_client, device):
    """Publish auto-discovery message to HA for the Current Temperature sensor."""
    # Using the same identifiers as the connect button
    discovery_topic = f"{DISCOVERY_PREFIX}/sensor/volcano_vaporiser_current_temperature/config"
    
    # Clear retained message for the discovery topic before publishing
    clear_retained_message(mqtt_client, discovery_topic)
    
    # Discovery payload for current temperature
    discovery_payload = {
        "name": "Volcano Vaporiser Current Temperature",
        "unique_id": "volcano_vaporiser_current_temperature",  # Unique ID for the sensor
        "state_topic": "homeassistant/sensor/volcano_vaporiser_current_temperature/state",  # State topic for temperature updates
        "unit_of_measurement": "°C",
        "value_template": "{{ value_json.temperature }}",  # Template to extract temperature from payload
        "device": {
            "identifiers": ["volcano_device"],  # Shared identifier for all devices
            "name": "Volcano Vaporiser",  # Name of the device
            "manufacturer": "Storz & Bickel",
            "model": "Volcano Vaporiser",
            "sw_version": "1.0",
        },
    }
    
    # Publish the auto-discovery message
    mqtt_client.publish(discovery_topic, json.dumps(discovery_payload), retain=True)
    logger.info("Published MQTT auto-discovery message for Current Temperature.")


async def publish_button_discovery(mqtt_client, button_name, button_topic, unique_id):
    """Publish auto-discovery message for a button (connect/disconnect)."""
    discovery_topic = f"homeassistant/button/{unique_id}/config"
    discovery_payload = {
        "name": button_name,
        "unique_id": unique_id,
        "command_topic": button_topic,
        "device": {
            "identifiers": ["volcano_device"],  # Shared identifier for all devices (temperature sensor and buttons)
            "name": "Volcano Vaporiser",
            "model": "Storz & Bickel Volcano",
            "manufacturer": "Storz & Bickel",
            "sw_version": "1.0",  # Include sw_version here as well
        }
    }
    mqtt_client.publish(discovery_topic, json.dumps(discovery_payload), retain=True)
    logger.info(f"Published auto-discovery message for {button_name}.")


async def publish_bluetooth_status_discovery(mqtt_client):
    """Publish auto-discovery message for Bluetooth status sensor."""
    discovery_topic = "homeassistant/sensor/volcano_bluetooth_status/config"
    discovery_payload = {
        "name": "Bluetooth Status",
        "unique_id": "volcano_bluetooth_status",
        "state_topic": "homeassistant/sensor/volcano_bluetooth_status/state",
        "device": {
            "identifiers": ["volcano_device"],
            "name": "Volcano Vaporiser",
            "model": "Storz & Bickel Volcano",
            "manufacturer": "Storz & Bickel"
        }
    }
    mqtt_client.publish(discovery_topic, json.dumps(discovery_payload), retain=True)
    logger.info("Published MQTT auto-discovery message for Bluetooth status.")


async def read_temperature(client, mqtt_client):
    """Reads and publishes the temperature every second."""
    while bluetooth_client:
        try:
            logger.info("Attempting to read temperature from Bluetooth device...")
            value = await client.read_gatt_char(CURRENT_TEMPERATURE_UUID)
            logger.debug(f"Raw temperature data: {value}")

            # Check the length of the raw data and interpret it
            if len(value) >= 2:
                raw_temp = int.from_bytes(value, byteorder='little')
                # Adjust temperature (divide by 10 for proper scaling, assuming it's in tenths of a degree)
                temp = raw_temp / 10  # Adjust this depending on the actual encoding
                temp_str = f"{temp:.1f}"  # Publish only numeric value (e.g., 54.0), without the degree symbol
                logger.info(f"Current Temperature: {temp_str}°C")

                # Publish to MQTT under the new topic for 'Current Temperature' under Volcano vaporiser
                mqtt_client.publish("homeassistant/sensor/volcano_vaporiser_current_temperature/state", json.dumps({"temperature": temp_str}))
                logger.info(f"Published temperature {temp_str} to MQTT under volcano vaporiser.")
            else:
                logger.error("Received invalid temperature data")

        except Exception as e:
            logger.error(f"Failed to read temperature: {e}")
        await asyncio.sleep(1)


async def connect_bluetooth(mqtt_client):
    """Connect to the Bluetooth device."""
    global bluetooth_client
    logger.info("Connecting to Bluetooth device with a timeout...")
    try:
        # Discover devices with 'volcano' in their name
        devices = await BleakScanner.discover()
        volcano_devices = [
            device for device in devices if device.name and "volcano" in device.name.lower()
        ]
        
        if volcano_devices:
            # Connect to the first available volcano device
            device = volcano_devices[0]
            logger.info(f"Connecting to Bluetooth device: {device.name} ({device.address})")
            async with BleakClient(device.address) as client:
                bluetooth_client = client  # Store client reference globally
                logger.info(f"Connected to {device.name}")
                await publish_auto_discovery(mqtt_client, device)
                await read_temperature(client, mqtt_client)
        else:
            logger.error("No Bluetooth devices with 'volcano' in the name were found.")
    except Exception as e:
        logger.error(f"Bluetooth connection failed: {e}")


async def disconnect_bluetooth():
    """Disconnect from Bluetooth device."""
    global bluetooth_client
    if bluetooth_client:
        logger.info("Disconnecting from Bluetooth device...")
        await bluetooth_client.disconnect()
        bluetooth_client = None
        logger.info("Disconnected from Bluetooth device.")
        # Update Bluetooth status in MQTT
        mqtt_client.publish("homeassistant/sensor/volcano_bluetooth_status/state", "Disconnected")
    else:
        logger.info("No Bluetooth client connected.")


async def main():
    config = await load_config()
    if not config:
        print("No configuration found. Run with 'configure' to set up.")
        return

    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(config["mqtt_username"], config["mqtt_password"])
    mqtt_client.on_connect = on_connect

    if config.get("ssl_enabled"):
        mqtt_client.tls_set(config.get("ssl_cert"))

    mqtt_client.connect_async(config["mqtt_host"], config["mqtt_port"])
    mqtt_client.loop_start()

    message_queue = MessageQueue()
    loop = asyncio.get_event_loop()

    asyncio.create_task(message_queue.process())
    asyncio.create_task(mqtt_listener(mqtt_client, message_queue, loop))

    try:
        await connect_bluetooth(mqtt_client)  # Start by connecting to Bluetooth
        await asyncio.Event().wait()  # Keep the program running until shutdown

    except asyncio.CancelledError:
        logger.info("Application terminating gracefully...")

    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
