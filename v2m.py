import asyncio
import json
import logging
import os
import sys
from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt

# Constants for UUIDs
HEAT_ON_UUID = "1011000f-5354-4f52-5a26-4249434b454c"
HEAT_OFF_UUID = "10110010-5354-4f52-5a26-4249434b454c"
FAN_ON_UUID = "10110013-5354-4f52-5a26-4249434b454c"
FAN_OFF_UUID = "10110014-5354-4f52-5a26-4249434b454c"
CURRENT_TEMPERATURE_UUID = "10110001-5354-4f52-5a26-4249434b454c"
WRITE_TEMPERATURE_UUID = "10110003-5354-4f52-5a26-4249434b454c"

# Configuration file path
CONFIG_FILE = "volcano_config.json"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VolcanoMQTT")

# Bluetooth and MQTT Queue
class MessageQueue:
    def __init__(self):
        self.queue = asyncio.Queue()

    async def put(self, coro):
        await self.queue.put(coro)

    async def process(self):
        while True:
            coro = await self.queue.get()
            try:
                await coro
            except Exception as e:
                logger.error(f"Error processing task: {e}")
            finally:
                self.queue.task_done()

# Load or create config
async def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

async def configure_wizard():
    config = {}

    print("--- Volcano Configuration Wizard ---")
    config["mqtt_host"] = input("Enter MQTT Host: ")
    config["mqtt_port"] = int(input("Enter MQTT Port (default 1883): ") or 1883)
    config["mqtt_username"] = input("Enter MQTT Username: ")
    config["mqtt_password"] = input("Enter MQTT Password: ")
    config["ssl_enabled"] = input("Enable SSL? (yes/no): ").lower() == "yes"
    if config["ssl_enabled"]:
        config["ssl_cert"] = input("Enter path to SSL certificate: ")

    print("Scanning for Bluetooth devices...")
    devices = await BleakScanner.discover()
    if devices is None:
        print("Bluetooth scan failed. Please check your Bluetooth setup.")
        config["bluetooth_address"] = input("Enter Bluetooth address manually: ")
    else:
        volcano_devices = [d for d in devices if d.name and "Volcano" in d.name]
        if volcano_devices:
            print("Found devices:")
            for idx, device in enumerate(volcano_devices):
                print(f"[{idx}] {device.name} - {device.address}")
            selected = int(input("Select device (enter number): "))
            config["bluetooth_address"] = volcano_devices[selected].address
        else:
            print("No Volcano devices found. Please enter the address manually.")
            config["bluetooth_address"] = input("Enter Bluetooth address: ")

    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)
    print("Configuration saved to", CONFIG_FILE)

async def main():
    # Check if 'configure' is passed as an argument
    if len(sys.argv) > 1 and sys.argv[1].lower() == "configure":
        await configure_wizard()
        return

    config = await load_config()

    if not config:
        print("Configuration file is missing. Run the program with 'configure' argument to set it up.")
        return

    mqtt_client = mqtt.Client()
    message_queue = MessageQueue()

    async def on_connect(client, userdata, flags, rc):
        logger.info("MQTT Connected.")

        # Home Assistant MQTT Discovery for Temperature
        temperature_config = {
            "name": "Volcano Temperature",
            "unique_id": "volcano_temperature",
            "state_topic": "homeassistant/sensor/volcano_temperature/state",
            "unit_of_measurement": "°C",
            "device": {
                "identifiers": ["volcano_device"],
                "name": "Volcano Vaporiser",
                "model": "Storz & Bickel Volcano",
                "manufacturer": "Storz & Bickel"
            }
        }
        client.publish(
            "homeassistant/sensor/volcano_temperature/config",
            json.dumps(temperature_config),
            retain=True,
        )

        # Home Assistant MQTT Discovery for Fan
        fan_config = {
            "name": "Volcano Fan",
            "unique_id": "volcano_fan",
            "command_topic": "homeassistant/fan/volcano_fan/set",
            "state_topic": "homeassistant/fan/volcano_fan/state",
            "device": {
                "identifiers": ["volcano_device"],
                "name": "Volcano Vaporiser",
                "model": "Storz & Bickel Volcano",
                "manufacturer": "Storz & Bickel"
            }
        }
        client.publish(
            "homeassistant/fan/volcano_fan/config",
            json.dumps(fan_config),
            retain=True,
        )

        client.publish("homeassistant/status", "online", retain=True)

    mqtt_client.username_pw_set(config.get("mqtt_username"), config.get("mqtt_password"))
    mqtt_client.on_connect = on_connect

    if config.get("ssl_enabled"):
        mqtt_client.tls_set(config.get("ssl_cert"))

    mqtt_client.connect_async(config["mqtt_host"], config["mqtt_port"], 60)

    mqtt_client.loop_start()
    asyncio.create_task(message_queue.process())

    async def read_temperature():
        client = BleakClient(config["bluetooth_address"])
        await client.connect()
        value = await client.read_gatt_char(CURRENT_TEMPERATURE_UUID)
        temp = int.from_bytes(value, byteorder="little") / 10
        mqtt_client.publish("homeassistant/sensor/volcano_temperature/state", temp)
        await client.disconnect()

    await message_queue.put(read_temperature())

if __name__ == "__main__":
    asyncio.run(main())
