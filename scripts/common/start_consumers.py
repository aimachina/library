# pylint: disable=import-error
# pylint: disable=no-name-in-module

import os
from importlib import import_module

from utils.configmanager import ConfigManager

consumers = ConfigManager.get_config_value("event_consumers")

task_ids = []
for _, consumer in consumers.items():
    consumer_module = import_module(consumer["consumer_module"])
    consumer_task = getattr(consumer_module, consumer["consumer_task"])
    print(f"Starting {consumer['workers']} workers...")
    for index in range(consumer["workers"]):
        try:
            consumer_id = os.environ["HOSTNAME"] + "-" + str(index)
            task = consumer_task.delay(consumer["consumer_group"], consumer_id=consumer_id)
        except TypeError:
            task = consumer_task.delay(consumer["consumer_group"])
        print(f"Consumer started for {consumer['consumer_group']}")
        task_ids.append(f'{task}')

with open(f'{os.environ["HOME"]}/tasks_ids', 'w') as f:
    f.write(','.join(task_ids))
