# pylint: disable=import-error
# pylint: disable=no-name-in-module
import redis
import pickle
import io
import time
from billiard import Pool, cpu_count  # Celery's multiprocessing fork

from utils.common import uuid_factory, extract_attr
from utils.configmanager import ConfigManager


class RedisStream:
    __broker = None

    @classmethod
    def get_broker(cls):
        if not cls.__broker:
            redis_config = ConfigManager.get_config_value("events-stream", "broker")
            cls.__broker = redis.StrictRedis(
                host=redis_config["host"],
                port=redis_config["port"],
                db=redis_config["db"],
                password=redis_config["password"],
            )
        return cls.__broker


def produce_one(name, event, maxlen=1000):
    r = RedisStream.get_broker()
    key = event.uuid
    value = event_to_bytes(event)
    id_ = r.xadd(name, {key: value}, maxlen=maxlen)
    return id_


def event_to_bytes(event):
    return pickle.dumps(event)


def bytes_to_event(bytes_):
    return pickle.loads(bytes_)


def consume_one(name):
    r = RedisStream.get_broker()
    e = r.xrange(name, count=1)
    if not e:
        return None
    bytes_ = [v for v in e[0][1].values()][0]
    return bytes_to_event(bytes_)


def maybe_create_consumer_groups(broker, consumer_groups_config):
    streams = consumer_groups_config["streams"]
    group_name = consumer_groups_config["name"]
    for stream in streams:
        if not broker.exists(stream) or not any(
            [group_name in group["name"].decode("utf-8") for group in broker.xinfo_groups(stream)]
        ):
            try:
                broker.xgroup_create(stream, group_name, mkstream=True)
            except:
                pass  # Not pretty, but handles the issue of a race for creating a CG


def decode_item(item):
    stream_name, events = item
    event_ids, event_dicts = zip(*[(event_id, event_dict) for event_id, event_dict in events])
    events = tuple(
        [bytes_to_event(event_bytes) for event_dict in event_dicts for event_bytes in event_dict.values()]
    )
    return stream_name, event_ids, events


def digest_event(stream_name, event, event_id, registered_handlers, args={}):
    if event.event_type in registered_handlers:
        handler = registered_handlers[event.event_type]
        handler(stream_name, event, event_id, **args)
    else:
        print("Ignoring event: {}".format(event.event_type))


def start_redis_consumer(consumer_group_config, registered_handlers, start_from=">"):
    broker = RedisStream.get_broker()
    streams_dict = {s: start_from for s in consumer_group_config["streams"]}
    maybe_create_consumer_groups(broker, consumer_group_config)

    group_name = consumer_group_config["name"]
    consumer_name = uuid_factory(group_name + "-consumer")()
    batch_size = consumer_group_config["batch_size"]

    while True:
        item = broker.xreadgroup(group_name, consumer_name, streams_dict, count=batch_size, block=1000)
        if item:
            stream_name, event_ids, events = decode_item(item[0])  # TODO: Handle batchsize > 1
            for event_id, event in zip(event_ids, events):
                digest_event(stream_name, event, event_id, registered_handlers)
                broker.xack(stream_name, group_name, event_id)


def retrieve_event(stream_name, event_id):  # TODO: Handle case for retrieving batch of events
    broker = RedisStream.get_broker()
    events = broker.xrange(stream_name, event_id, event_id, count=1)
    if not len(events):
        return None
    _, event_dict = events[0]
    event_bytes = next(iter(event_dict.values()))
    return bytes_to_event(event_bytes)


def source_event(stream_name, filters_dict={}, batch_size=128, latest_first=True, max_iter=1000):
    broker = RedisStream.get_broker()
    next_id = "+" if latest_first else "-"
    i = 0
    while i < max_iter:
        i += 1
        if latest_first:
            event_tuples = broker.xrevrange(stream_name, max=next_id, count=batch_size)
        else:
            event_tuples = broker.xrange(stream_name, min=next_id, count=batch_size)
        n = len(event_tuples)
        if not n:
            return None
        event_ids, event_dicts = zip(*event_tuples)
        with Pool(parallel_jobs(n)) as pool:
            event_bytes = pool.map(event_from_dict, event_dicts)
            events = pool.map(bytes_to_event, event_bytes)
            args = zip(*(events, [filters_dict] * len(events)))
            matches = pool.starmap(match_event, args)
            if any(matches):
                index = matches.index(True)
                return events[index]
        next_id = decrement_id(event_ids[-1]) if latest_first else increment_id(event_ids[-1])
    return None


def increment_id(id_str):
    ts, idx = id_str.decode("utf-8").split("-")
    ts, idx = int(ts), int(idx)
    idx += 1
    new_id = "{}-{}".format(ts, idx)
    return bytes(new_id, "utf-8")


def decrement_id(id_str):
    ts, idx = id_str.decode("utf-8").split("-")
    ts, idx = int(ts), int(idx)
    if idx != 0:
        idx -= 1
    else:
        ts -= 1
    new_id = "{}-{}".format(ts, idx)
    return bytes(new_id, "utf-8")


def match_event(event, filters_dict):
    for attr_name, value in filters_dict.items():
        attr = extract_attr(event, attr_name)
        if not attr or attr != value:
            return False
    return True


def source_item_from_list_in_event(
    stream_name, list_name, field, value, batch_size=1000, max_iter=1000,
):
    broker = RedisStream.get_broker()
    next_id = "+"
    i = 0
    while i < max_iter:
        i += 1
        event_tuples = broker.xrevrange(stream_name, max=next_id, count=batch_size)
        n = len(event_tuples)
        if not n:
            return None, None, None
        event_ids, event_dicts = zip(*event_tuples)

        with Pool(parallel_jobs(n)) as pool:
            event_bytes = pool.map(event_from_dict, event_dicts)
            events = pool.map(bytes_to_event, event_bytes)
            args = zip(*(events, [list_name] * len(events)))
            dicts = pool.starmap(extract_attr, args)

            args = zip(*(dicts, [field] * len(dicts), [value] * len(dicts)))
            matches = pool.starmap(find_first_by, args)

            for i, detection in enumerate(matches):
                if detection:
                    return detection, event_ids[i], events[i].correlations
        next_id = decrement_id(event_ids[-1])
    return None, None, None


def find_first_by(dicts, field, value):
    for d in dicts:
        if d[field] == value:
            return d
    return None


def event_from_dict(x):
    return next(iter(x.values()))


parallel_jobs = lambda x: min(x, cpu_count())
