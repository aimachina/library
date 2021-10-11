import os
import pickle
from datetime import datetime
from contextvars import ContextVar, copy_context

from redis import StrictRedis
from typing import Any, Dict
from aim_library.utils.common import extract_attr, uuid_factory, make_jsend_response
from aim_library.utils.configmanager import ConfigManager


class RedisStream:
    __broker = None

    @classmethod
    def get_broker(cls) -> StrictRedis:
        if not cls.__broker:
            redis_config = ConfigManager.get_config_value("events-stream", "broker")
            cls.__broker = StrictRedis(
                host=redis_config["host"],
                port=redis_config["port"],
                db=redis_config["db"],
                password=redis_config["password"],
            )
        return cls.__broker


def produce_one(name: str, event: Any, maxlen: int = None) -> str:
    broker = RedisStream.get_broker()
    key = str(event.uuid)
    event.correlations = correlations_context.get()
    event.causations = causations_context.get()
    value = event_to_bytes(event)
    id_ = broker.xadd(name, {key: value}, maxlen=maxlen)  # type: ignore
    return id_


def event_to_bytes(event: Any) -> bytes:
    return pickle.dumps(event)


def bytes_to_event(bytes_: bytes) -> Any:
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
            [group_name == group["name"].decode("utf-8") for group in broker.xinfo_groups(stream)]
        ):
            try:
                broker.xgroup_create(stream, group_name, mkstream=True)
                print(f"Consumer group '{group_name}' created for stream {stream}")
            except:
                pass  # Not pretty, but handles the issue of a race for creating a CG


def decode_item(item):
    stream_name, events = item
    event_ids, event_dicts = zip(*events)
    events = tuple(
        bytes_to_event(event_bytes) for event_dict in event_dicts for event_bytes in event_dict.values()
    )
    return stream_name, event_ids, events


correlations_context: ContextVar[dict] = ContextVar("correlations_context", default={})
causations_context: ContextVar[list] = ContextVar("correlations_context", default=[])

consumer_context: ContextVar[dict] = ContextVar("consumer_context", default={})
event_context: ContextVar[dict] = ContextVar("event_context", default={})


def set_event_context(correlation_id: str, user_access: Dict[str, Any]) -> None:

    event_context.set({"correlation_id": correlation_id, "user_access": user_access})


def set_consumer_context(stream_name: str, event_type: str, event_id: str, handler_name: str) -> None:
    ctx = {
        "start_time": datetime.utcnow(),
        "stream_name": stream_name,
        "handler": handler_name,
        "event_id": event_id,
        "event_type": event_type,
        "hostname": os.getenv("HOSTNAME") or "UNKNOWN_HOST",
    }
    consumer_context.set(ctx)


def maybe_retrieve_correlation_id(event: Any) -> str:
    return (
        extract_attr(event, "document_id")
        or extract_attr(event, "source_id")
        or extract_attr(event, "payload.uuid")
        or extract_attr(event, "payload.source_id")
        or ""
    )


def get_event_context(event=None):
    ctx = event_context.get()
    if not ctx and event is not None:
        ctx["correlation_id"] = maybe_retrieve_correlation_id(event)
        ctx["user_access"] = extract_attr(event, "user_access")
    return ctx


def digest_event(stream_name: str, event: Any, event_id: str, registered_handlers: dict) -> None:
    if event.event_type in registered_handlers:
        handler = registered_handlers[event.event_type]
        correlations_context.set(event.update_correlations({stream_name: event_id}))
        causations_context.set(event.update_causations({stream_name: event_id}))
        set_consumer_context(stream_name, event.event_type, event_id, handler.__name__)
        try:
            handler(stream_name, event, event_id)
            if os.getenv("LOG_ALL_EVENTS") and stream_name not in ["logging"]:
                produce_log_message(message=make_jsend_response(), set_end=True)
        except Exception as exc:
            # Produce error event
            print(8 * "*" + "PRODUCING ERROR EVENT" + 8 * "*")
            produce_error_event(stream_name, event, exc)
            raise exc from None
    else:
        if os.getenv("PRINT_IGNORED_EVENTS"):
            print("Ignoring event: {}".format(event.event_type))


def produce_log_message(message: dict, set_end: bool = False):
    from aim_common.events.base_event import BaseEvent
    from aim_common.events.event_type import EventType

    log_event = BaseEvent(event_type=EventType.LOGGING_EVENT)
    log_event.data = {
        "msg": message,
        "event_context": get_event_context(),
        "consumer_context": consumer_context.get(),
    }
    if set_end:
        log_event.data["consumer_context"]["end_time"] = datetime.utcnow()
    produce_one("logging", log_event, maxlen=1000)


def produce_error_event(stream_name, event, exc):
    import traceback as tb
    from aim_common.events.base_event import BaseEvent
    from aim_common.events.event_type import EventType

    dead_letter_id = produce_one("dead-letter", event, maxlen=1000)

    error_event = BaseEvent(event_type=EventType.ERROR_PROCESSING_EVENT)
    error_event.data = {
        "dead_letter_id": dead_letter_id,
        "exception": repr(exc),
        "traceback": "".join(tb.format_exception(None, exc, exc.__traceback__)),
        "event_context": get_event_context(event),
        "consumer_context": {**consumer_context.get(), "end_time": datetime.utcnow()},
    }
    produce_one(stream_name, error_event)


def make_consumer_name(uuid, group_name):
    if uuid:
        return group_name + "_consumer-" + str(uuid)
    return uuid_factory(group_name + "-consumer")()


def discard_max_retries_from_pel(stream_name, group_name, consumer_name, max_retries):
    r = RedisStream.get_broker()
    start_from = "-"
    discarded = []
    while messages := r.xpending_range(stream_name, group_name, start_from, "+", 10, consumer_name):
        for message in messages:
            if message["times_delivered"] > max_retries:
                r.xack(stream_name, group_name, message["message_id"])
                discarded.append(message["message_id"])
        start_from = increment_id(messages[-1]["message_id"])
    return discarded


def discard_from_pel_by_stream(group_name, consumer_name, streams, max_retries):
    discarded_list = []
    for stream_name in streams:
        discarded = discard_max_retries_from_pel(stream_name, group_name, consumer_name, max_retries)
        discarded_list.append((stream_name, discarded))
    return list(filter(is_really_not_empty, discarded_list))


def process_pending_messages(broker, group_name, consumer_name, streams, handlers, max_retries):
    discarded = discard_from_pel_by_stream(group_name, consumer_name, streams, max_retries)
    if discarded:
        print(f"Discarded unprocessable events: {discarded} for {consumer_name}")

    while messages_by_stream := pending_messages_by_stream(broker, group_name, consumer_name, streams, 1):
        for message in messages_by_stream:
            decode_and_digest(broker, message, group_name, handlers)


def is_really_not_empty(item):
    return len(item[1]) if len(item) > 1 else False


def pending_messages_by_stream(broker, group_name, consumer_name, streams, batch_size):
    streams_dict = {s: "0" for s in streams}
    messages_by_stream = broker.xreadgroup(group_name, consumer_name, streams_dict, count=batch_size)
    messages_by_stream = filter(is_really_not_empty, messages_by_stream)
    return list(messages_by_stream)


def new_messages_by_stream(broker, group_name, consumer_name, streams, start_from, batch_size):
    streams_dict = {s: start_from for s in streams}
    messages_by_stream = broker.xreadgroup(
        group_name, consumer_name, streams_dict, count=batch_size, block=1000
    )
    return messages_by_stream


def start_redis_consumer(
    consumer_group_config, registered_handlers, start_from=">", consumer_id=None, max_retries=1
):
    broker = RedisStream.get_broker()
    maybe_create_consumer_groups(broker, consumer_group_config)

    group_name = consumer_group_config["name"]
    consumer_name = make_consumer_name(consumer_id, group_name)
    batch_size = consumer_group_config["batch_size"]
    streams = consumer_group_config["streams"]

    process_pending_messages(broker, group_name, consumer_name, streams, registered_handlers, max_retries)
    while True:
        for message in new_messages_by_stream(
            broker, group_name, consumer_name, streams, start_from, batch_size
        ):
            decode_and_digest(broker, message, group_name, registered_handlers)


def decode_and_digest(broker, message, group_name, handlers):
    stream_name, event_ids, events = decode_item(message)
    for event_id, event in zip(event_ids, events):
        ctx = copy_context()
        ctx.run(digest_event, stream_name, event, event_id, handlers)
        broker.xack(stream_name, group_name, event_id)


def retrieve_event(stream_name, event_id):  # TODO: Handle case for retrieving batch of events
    broker = RedisStream.get_broker()
    events = broker.xrange(stream_name, event_id, event_id, count=1)
    if not len(events):
        return None
    _, event_dict = events[0]
    event_bytes = next(iter(event_dict.values()))
    return bytes_to_event(event_bytes)


def source_event(stream_name, filters_dict={}, batch_size=128, latest_first=True):
    from billiard import Pool, cpu_count  # Celery's multiprocessing fork

    broker = RedisStream.get_broker()
    next_id = "+" if latest_first else "-"
    i = 0
    max_iter = int(1000 / batch_size) + 1
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
    stream_name,
    list_name,
    field,
    value,
    batch_size=1000,
):
    from billiard import Pool, cpu_count  # Celery's multiprocessing fork

    broker = RedisStream.get_broker()
    next_id = "+"
    i = 0
    max_iter = int(1000 / batch_size) + 1
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


parallel_jobs = lambda x: min(x, 16)
