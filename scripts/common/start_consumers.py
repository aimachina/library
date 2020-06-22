# pylint: disable=import-error
# pylint: disable=no-name-in-module

from importlib import import_module

from utils.configmanager import ConfigManager

consumers = ConfigManager.get_config_value("event_consumers")

for _, consumer in consumers.items():
    consumer_module = import_module(consumer["consumer_module"])
    consumer_task = getattr(consumer_module, consumer["consumer_task"])
    print(f"Starting {consumer['workers']} workers...")
    for _ in range(consumer["workers"]):
        consumer_task.delay(consumer["consumer_group"])
        print(f"Consumer started for {consumer['consumer_group']}")
