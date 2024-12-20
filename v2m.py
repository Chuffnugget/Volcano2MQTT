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


async def connect_bluetooth(mqtt_client):
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
                logger.info(f"Connected to {device.name}")
                await publish_auto_discovery(mqtt_client, device)
                await read_temperature(client, mqtt_client)
        else:
            logger.error("No Bluetooth devices with 'volcano' in the name were found.")
    except Exception as e:
        logger.error(f"Bluetooth connection failed: {e}")


async def publish_auto_discovery(mqtt_client, device):
    """Publish auto-discovery message to HA for the Current Temperature sensor."""
    discovery_topic = f"{DISCOVERY_PREFIX}/sensor/volcano_vaporiser_current_temperature/config"
    discovery_payload = {
        "name": "Volcano Vaporiser Current Temperature",
        "state_topic": "homeassistant/sensor/volcano_vaporiser_current_temperature/state",  # Updated state_topic
        "unit_of_measurement": "°C",
        "value_template": "{{ value_json.temperature }}",
        "device": {
            "identifiers": [f"{device.address}"],
            "name": "Volcano Vaporiser",
            "manufacturer": "S&B",
            "model": "Volcano Vaporiser",
            "sw_version": "1.0",
        },
    }
    mqtt_client.publish(discovery_topic, json.dumps(discovery_payload), retain=True)
    logger.info("Published MQTT auto-discovery message for Current Temperature.")



async def read_temperature(client, mqtt_client):
    """Reads and publishes the temperature every second."""
    while True:
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



async def disconnect_bluetooth():
    logger.info("Disconnecting from Bluetooth device...")
    # Add Bluetooth disconnection logic here


async def bluetooth_worker(config, message_queue, mqtt_client):
    while True:
        coro = await message_queue.queue.get()
        try:
            await coro()
        except Exception as e:
            logger.error(f"Bluetooth error: {e}")
        finally:
            message_queue.queue.task_done()


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

    await connect_bluetooth(mqtt_client)

    def shutdown():
        logger.info("Shutting down...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        for task in asyncio.all_tasks():
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: shutdown())

    try:
        await asyncio.Event().wait()  # Run until interrupted
    except asyncio.CancelledError:
        logger.info("Application terminated.")


if __name__ == "__main__":
    asyncio.run(main())
