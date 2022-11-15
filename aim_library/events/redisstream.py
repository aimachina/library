import os
import pickle
from datetime import datetime
from contextvars import ContextVar, copy_context
from functools import partial

from redis import StrictRedis
from typing import Any, Dict, Optional
from aim_library.utils.common import enabled_by_env, extract_attr, uuid_factory
from aim_library.utils.configmanager import ConfigManager
from aim_library.utils.result import Result, Ok, Error

from aim_library.utils.status import set_document_status


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
    key = event.event_type.name + str(event.uuid)
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


def is_accepted_by(message, registered_handlers):
    _, event_dict = message
    k = next(iter(event_dict))
    parts = k.split(":")
    if len(parts) == 1:
        return True  # Bypass filter, no type in key, event needs to be decoded
    event_type = parts[0]
    return event_type in registered_handlers


def split_by_handler(messages, registered_handlers):
    is_accepted = partial(is_accepted_by, registered_handlers=registered_handlers)
    accepted = filter(is_accepted, messages)
    rejected = filter(lambda m: not is_accepted(m), messages)
    return accepted, rejected


def handle_rejected(*, stream, group_name, rejected):
    broker = RedisStream.get_broker()
    ids, _ = zip(*rejected)
    broker.xack(stream, group_name, ids)


def decode_item(item):
    stream_name, events = item
    event_ids, event_dicts = zip(*events)
    events = tuple(bytes_to_event(event_bytes) for event_dict in event_dicts for event_bytes in event_dict.values())
    return stream_name, event_ids, events


correlations_context: ContextVar[dict] = ContextVar("correlations_context", default={})
causations_context: ContextVar[list] = ContextVar("correlations_context", default=[])

consumer_context: ContextVar[dict] = ContextVar("consumer_context", default={})
event_context: ContextVar[dict] = ContextVar("event_context", default={})


def set_event_context(correlation_id: str, user_access: Dict[str, Any], produce_errors_to: str = "") -> None:
    ctx = event_context.get()
    ctx["correlation_id"] = correlation_id
    ctx["user_access"] = user_access
    ctx["produce_errors_to"] = produce_errors_to or os.getenv("PRODUCE_ERRORS_TO") or ctx.get("stream_name", "")
    event_context.set(ctx)


def set_event_context_start(event_id, event_type, stream_name, handler, dt=None):
    ctx = event_context.get()
    ctx["start_time"] = dt or datetime.utcnow()
    ctx["stream_name"] = maybe_decode(stream_name)
    ctx["event_id"] = maybe_decode(event_id)
    ctx["event_type"] = event_type.name
    ctx["handler"] = handler
    event_context.set(ctx)


def set_consumer_context(consumer_name, group_name) -> None:
    ctx = consumer_context.get()
    ctx["hostname"] = os.getenv("HOSTNAME") or "UNKNOWN_HOST"
    ctx["consumer_name"] = consumer_name
    ctx["group_name"] = group_name
    consumer_context.set(ctx)


def set_event_context_end(dt: Optional[datetime] = None) -> None:
    ctx = event_context.get()
    ctx["end_time"] = dt or datetime.utcnow()
    event_context.set(ctx)


def maybe_retrieve_correlation_id(event: Any) -> str:
    return (
        extract_attr(event, "document_id")
        or extract_attr(event, "source_id")
        or extract_attr(event, "payload.uuid")
        or extract_attr(event, "payload.source_id")
        or ""
    )


def ensure_event_context(event):
    ctx = event_context.get()
    if not ctx:
        correlation_id = maybe_retrieve_correlation_id(event)
        user_access = extract_attr(event, "user_access") or {}
        set_event_context(correlation_id=correlation_id, user_access=user_access)


def reset_event_context():
    event_context.set({})


def get_event_context(event=None):
    ctx = event_context.get()
    return ctx


def ensure_result(result):
    if isinstance(result, Result):
        return result
    if result is not None:
        return Error(value=result)
    return Ok()


def digest_event(stream_name: str, event: Any, event_id: str, registered_handlers: dict) -> None:
    if event.event_type not in registered_handlers:
        if enabled_by_env("PRINT_IGNORED_EVENTS"):
            print("Ignoring event: {}".format(event.event_type))
            return
    handler = registered_handlers[event.event_type]
    correlations_context.set(event.update_correlations({stream_name: event_id}))
    causations_context.set(event.update_causations({stream_name: event_id}))
    reset_event_context()
    ensure_event_context(event)
    set_event_context_start(event_id, event.event_type, stream_name, handler.__name__)
    try:
        result = ensure_result(handler(stream_name, event, event_id))
        set_event_context_end()
        if enabled_by_env("LOG_ALL_EVENTS") and stream_name not in ["logs"]:
            produce_from_result(result)

    except Exception as exc:
        set_event_context_end()
        # dead_letter_id = produce_one("dead-letter", event, maxlen=1000)
        produce_from_result(Error(value=exc), stream_name=stream_name, dead_letter_id="")
        if not enabled_by_env("PREVENT_CONSUMER_CRASH"):
            raise exc from None


def digest_batch(stream_name, batch, registered_handlers):
    event = next(iter(batch))
    handler = registered_handlers[event.event_type]  # Assume a single handler for the whole batch
    reset_event_context()
    try:
        result = ensure_result(handler(stream_name, batch, None))
        set_event_context_end()
        if enabled_by_env("LOG_ALL_EVENTS"):
            produce_from_result(result)
    except Exception as exc:
        set_event_context_end()
        if not enabled_by_env("PREVENT_CONSUMER_CRASH"):
            raise exc from None


def produce_from_result(result, stream_name=None, dead_letter_id=None):
    if result.is_ok():
        produce_log_event(result, is_user_log=False)
    else:
        produce_error_event(stream_name, dead_letter_id, result)


def produce_user_log_event(result: Result) -> None:
    if not enabled_by_env("ENABLED_BY_ENV"):
        return
    if not isinstance(result, (Ok, Error)):
        raise ValueError("Parameter 'result' must be a Result [Ok or Error].")
    produce_log_event(result, is_user_log=True)


def produce_log_event(
    result: Result, stream_name: str = "logs", is_user_log: bool = True, make_uuid=uuid_factory("LOG")
):
    from aim_common.events.base_event import BaseEvent
    from aim_common.events.event_type import EventType

    log_event = BaseEvent(event_type=EventType.LOGGING_EVENT)
    log_event.data = {
        "uuid": make_uuid(),
        "result": result.as_dict(),
        "event_context": get_event_context(),
        "consumer_context": consumer_context.get(),
        "is_user_log": bool(is_user_log),
    }
    produce_one(stream_name, log_event, maxlen=1000)


def produce_error_event(
    stream_name: str, dead_letter_id: str, result: Error, make_uuid=uuid_factory("LOG"), is_user_log=False
):
    from aim_common.events.base_event import BaseEvent
    from aim_common.events.event_type import EventType

    ctx = get_event_context()
    produce_errors_to = ctx["produce_errors_to"] or os.getenv("PRODUCE_ERRORS_TO", stream_name)
    print(8 * "*" + f"PRODUCING ERROR EVENT TO '{produce_errors_to}'" + 8 * "*")
    error_event = BaseEvent(event_type=EventType.ERROR_PROCESSING_EVENT)
    error_event.data = {
        "uuid": make_uuid(),
        "result": result.err(),
        "event_context": ctx,
        "consumer_context": consumer_context.get(),
        "dead_letter_id": maybe_decode(dead_letter_id),
        "exception": repr(result.exc()),
        "traceback": result.traceback(),
        "is_user_log": bool(is_user_log),
    }
    produce_one(produce_errors_to, error_event)
    produce_one("logs", error_event)
    set_document_status(
        document_id=ctx["correlation_id"],
        status="EXCEPTION",
        description=repr(result.exc()),
        organization=ctx["user_access"]["organization_id"],
        meta="",
    )


def maybe_decode(s):
    if isinstance(s, (bytes, bytearray)):
        s = s.decode("utf-8")
    return s


def make_consumer_name(consumer_id, group_name):
    if consumer_id:
        return str(consumer_id)
    return uuid_factory(os.getenv("HOSTNAME", group_name))()


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
    messages_by_stream = broker.xreadgroup(group_name, consumer_name, streams_dict, count=batch_size, block=1000)
    return messages_by_stream


def decode_batch(batch):
    decoded = []
    for event_dict in batch:
        event_bytes = next(iter(event_dict.values()))
        decoded.append(bytes_to_event(event_bytes))
    return decoded


def consume_batches_forever(*, consumer_group_config, consumer_id, registered_handlers, start_from, max_retries):
    broker = RedisStream.get_broker()
    maybe_create_consumer_groups(broker, consumer_group_config)
    group_name = consumer_group_config["name"]
    consumer_name = make_consumer_name(consumer_id, group_name)
    batch_size = consumer_group_config["batch_size"]
    streams = consumer_group_config["streams"]
    retry_after = consumer_group_config.get("retry_after", 60000)
    set_consumer_context(consumer_name, group_name)
    start_id = "0-0"
    handler_names = [k.name for k in registered_handlers.keys()]
    while True:
        for stream in streams:
            stream_messages = new_messages_by_stream(
                broker=broker,
                group_name=group_name,
                consumer_name=consumer_name,
                streams=[stream],
                start_from=start_from,
                batch_size=batch_size,
            )
            start_id, claimed_messages, _ = broker.xautoclaim(
                stream, group_name, consumer_name, retry_after, start_id=start_id
            )
            accepted, rejected = split_by_handler(claimed_messages + stream_messages, registered_handlers)
            handle_rejected(stream=stream, group_name=group_name, rejected=rejected)
            accepted_ids, accepted_bytes = zip(*accepted)
            accepted_messages = decode_batch(batch=accepted_bytes)
            ctx = copy_context()
            ctx.run(digest_batch, stream, accepted_messages, accepted_ids, registered_handlers)
            if broker.xack(stream, group_name, *accepted_ids) > 0:
                break


def start_redis_consumer(consumer_group_config, registered_handlers, start_from=">", consumer_id=None, max_retries=1):
    if consumer_group_config["batch_size"] > 1:
        consume_batches_forever(
            consumer_group_config=consumer_group_config,
            consumer_id=consumer_id,
            registered_handlers=registered_handlers,
            start_from=start_from,
            max_retries=max_retries,
        )
    else:
        consume_single_messages_forever(
            consumer_group_config=consumer_group_config,
            consumer_id=consumer_id,
            registered_handlers=registered_handlers,
            start_from=start_from,
            max_retries=max_retries,
        )


def consume_single_messages_forever(
    *, consumer_group_config, consumer_id, registered_handlers, start_from, max_retries
):
    broker = RedisStream.get_broker()
    maybe_create_consumer_groups(broker, consumer_group_config)
    group_name = consumer_group_config["name"]
    consumer_name = make_consumer_name(consumer_id, group_name)
    batch_size = consumer_group_config["batch_size"]
    streams = consumer_group_config["streams"]
    set_consumer_context(consumer_name, group_name)
    process_pending_messages(broker, group_name, consumer_name, streams, registered_handlers, max_retries)
    while True:
        for stream in streams:
            priority_message = new_messages_by_stream(
                broker, group_name, consumer_name, [stream], start_from, batch_size
            )
            if priority_message:
                decode_and_digest(broker, priority_message, group_name, registered_handlers)
                break


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


def find_first_by(dicts, field, value):
    for d in dicts:
        if d[field] == value:
            return d
    return None


def event_from_dict(x):
    return next(iter(x.values()))
