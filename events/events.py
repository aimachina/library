# pylint: disable=import-error
# pylint: disable=no-name-in-module
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from Typing import Optional

from utils.common import uuid_factory

# pylint: enable=import-error


class EventType(Enum):
    # Generic
    GENERIC_EVENT = auto()
    GENERIC_MODEL_CREATED = auto()
    GENERIC_MODEL_DELETED = auto()
    GENERIC_MODEL_UPDATED = auto()

    # Metrics
    METRICS_EVENT = auto()

    # Streams
    STREAM_CREATED = auto()
    STREAM_DELETED = auto()
    STREAM_UPDATED = auto()
    FRAMEPRODUCER_STARTED = auto()
    FRAMEPRODUCER_STOPPED = auto()
    FRAMEPRODUCER_FAILED = auto()

    # Frames
    FRAME_GRABBED = auto()
    FRAMEPRODUCER_METRICS = auto()

    # Detector
    OBJECT_DETECTED = auto()  # Detection
    OBJECTS_DETECTED = auto()  # DetectionsList

    # Encoder
    OBJECT_ENCODED = auto()  # Detection
    OBJECTS_ENCODED = auto()  # DetectionsList

    # Tracker
    OBJECT_TRACKING_ACTIVE = auto()
    OBJECT_TRACKING_MERGED = auto()
    OBJECT_TRACKING_ENDED = auto()

    # Trackable objects
    TRACKABLE_CREATED = auto()
    TRACKABLE_UPDATED = auto()
    TRACKABLE_DELETED = auto()
    TRACKABLE_MATCHING_SET_ACTIVE = auto()
    TRACKABLE_MATCHING_SET_INACTIVE = auto()
    # Matcher
    MATCH_FOUND = auto()

    # Files
    FILES_UPLOADED = auto()
    IMAGE_UPLOADED = auto()
    EXCEL_UPLOADED = auto()
    ARCHIVE_UPLOADED = auto()

    # Transactions
    TRANSACTION_LOADED = auto()
    RECEIPT_CREATED = auto()

    # OCR
    TEXT_DETECTED = auto()
    TEXT_UPDATED = auto()
    TEXT_EXTRACTED = auto()

    # Documents
    DOCUMENT_CREATED = auto()
    DOCUMENT_UPDATED = auto()
    DOCUMENT_DELETED = auto()

    # Search
    DOCUMENT_INDEXED = auto()
    DOCUMENT_UPDATED_IN_INDEX = auto()
    DOCUMENT_DELETED_FROM_INDEX = auto()

    # Transaction Indexed
    TRANSACTION_INDEXED = auto()

    # Storage
    OBJECT_STORED = auto()

    # ML
    TEXT_LINES_CLASSIFIED = auto()
    TEXT_LINES_RECOGNIZED = auto()
    DOCUMENT_IMAGE_CLASSIFIED = auto()

    # Redactor
    LINES_REDACTED = auto()


class CommandType(Enum):

    GENERIC_COMMAND = auto()
    # ML Commands
    CLASSIFY_TEXT_LINES = auto()
    RECOGNIZE_TEXT_LINES = auto()
    CLASSIFY_DOCUMENT_IMAGE = auto()
    REDACT_LINES = auto()
    EXTRACT_TEXT = auto()


@dataclass
class BaseEvent:
    timestamp: float = field(default=0.0)
    uuid: str = field(default="")
    event_type: Optional[EventType] = field(default=None)
    correlations: dict = field(default_factory=dict)
    causations: list = field(default_factory=list)
    prefix: str = field(default="")

    def __post_init__(self) -> None:
        self.timestamp = self.timestamp or time.time()
        self.uuid = self.uuid or uuid_factory(self.prefix or "EVENT")()
        self.event_type = self.event_type or EventType.GENERIC_EVENT

    def update_correlations(self, correlations: dict) -> dict:
        new_correlations = self.correlations.copy()
        new_correlations.update(correlations)
        return new_correlations

    def update_causations(self, new_causation: dict) -> list:
        updated_causations = self.causations[:]
        updated_causations.append(new_causation)
        return updated_causations


@dataclass
class BaseCommand(BaseEvent):
    event_type: CommandType = field(default=None)
    payload: dict = field(default_factory=dict)

    def __post_init__(self):
        self.timestamp = self.timestamp or time.time()
        # pylint: disable=no-member
        self.uuid = self.uuid or uuid_factory(self.prefix or "COMMAND")()
        self.event_type = self.event_type or CommandType.GENERIC_COMMAND


@dataclass
class MLCommand(BaseCommand):
    prefix: str = "ML-COMMAND"

    def __init__(self, event_type: CommandType, payload: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_type = event_type
        self.payload = payload or {}


@dataclass
class RedactorCommand(BaseCommand):
    prefix: str = "REDACTOR-COMMAND"

    def __init__(self, event_type: CommandType, payload: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_type = event_type
        self.payload = payload or {}


@dataclass
class GenericEvent(BaseEvent):
    prefix: str = "GENERIC-EVENT"
    generic_model_data: dict = field(default_factory=dict)


@dataclass
class MetricsEvent(BaseEvent):
    metrics: dict = field(default_factory=dict)
    prefix: str = "METRICS-EVENT"

    def __init__(self, metrics: dict, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = metrics
        self.event_type = event_type or EventType.METRICS_EVENT


@dataclass
class StreamEvent(BaseEvent):
    stream_data: dict = field(default_factory=dict)
    prefix = "STREAM-EVENT"

    def __init__(self, stream_data: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_data = stream_data


@dataclass
class FrameEvent(BaseEvent):
    try:
        from PIL import Image
    except ModuleNotFoundError:
        Image = object

    prefix: str = "FRAME-EVENT"
    frame: Image = field(default_factory=None)
    worker_id: str = field(default_factory=None)
    stream_id: str = field(default_factory=None)

    def __init__(self, frame: Image, worker_id: str, stream_id: str, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.frame = frame
        self.worker_id = worker_id
        self.stream_id = stream_id
        self.event_type = event_type or EventType.FRAME_GRABBED


@dataclass
class DetectionEvent(BaseEvent):
    prefix: str = "DETECTION-EVENT"
    detections: dict = field(default_factory=dict)

    def __init__(self, detections: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.detections = detections
        self.uuid = detections.get("id", None) or detections["uuid"]
        self.event_type = event_type or EventType.OBJECT_DETECTED
        self.correlations = correlations


@dataclass
class EmbeddingEvent(BaseEvent):
    prefix: str = "EMBEDDING-EVENT"
    embeddings: list = field(default_factory=dict)

    def __init__(self, embeddings: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.embeddings = embeddings
        self.uuid = embeddings["id"]
        self.event_type = event_type or EventType.OBJECT_ENCODED
        self.correlations = correlations


@dataclass
class TrackingEvent(BaseEvent):
    prefix: str = "TRACKING-EVENT"
    detection: dict = field(default_factory=dict)
    replaced: dict = field(default_factory=dict)

    def __init__(
        self, detection: dict, replaced: dict, correlations: dict = {}, event_type=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.detection = detection
        self.replaced = replaced
        self.event_type = event_type or EventType.OBJECT_TRACKING_ACTIVE
        self.correlations = correlations


@dataclass
class TrackableEvent(BaseEvent):
    prefix: str = "TRACKABLE-EVENT"
    trackable: dict = field(default_factory=dict)

    def __init__(
        self, trackable: dict, correlations: dict = {}, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.trackable = trackable
        self.correlations = correlations
        self.event_type = event_type or EventType.TRACKABLE_CREATED


@dataclass
class MatchEvent(BaseEvent):
    prefix: str = "MATCH-EVENT"
    anchor: str = field(default_factory=str)
    test: str = field(default_factory=str)
    metrics: dict = field(default_factory=dict)

    def __init__(self, test: str, matches: dict, event_type: EventType = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test = test
        self.matches = matches
        self.event_type = event_type or EventType.MATCH_FOUND


@dataclass
class FileEvent(BaseEvent):
    prefix: str = "FILES-EVENT"

    def __init__(
        self, route: str, user_id: str, event_type: EventType = None, data: bytes = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.route = route
        self.data = data
        self.event_type = event_type or EventType.FILES_UPLOADED


@dataclass
class ReceiptEvent(BaseEvent):
    prefix: str = "RECEIPT-EVENT"

    def __init__(self, receipt_data: dict, user_id: str, event_type: EventType = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.receipt_data = receipt_data
        self.event_type = event_type or EventType.RECEIPT_CREATED


@dataclass
class TransactionEvent(BaseEvent):
    prefix: str = "TRANSACTION-EVENT"

    def __init__(self, transaction_data: dict, user_id: str, event_type: EventType = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.transaction_data = transaction_data
        self.event_type = event_type or EventType.TRANSACTION_LOADED


@dataclass
class DocumentEvent(BaseEvent):
    prefix: str = "DOCUMENT-EVENT"
    document_id: str = field(default_factory=str)
    signature: str = field(default_factory=str)
    data: dict = field(default_factory=dict)
    user_id: str = field(default="")

    def __init__(
        self,
        document_id: str,
        signature: str = "",
        data: dict = {},
        user_id: str = "",
        event_type: EventType = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.document_id = document_id
        self.signature = signature
        self.data = data
        self.user_id = user_id
        self.event_type = event_type or EventType.DOCUMENT_CREATED


@dataclass
class StorageEvent(BaseEvent):
    prefix: str = "STORAGE-EVENT"
    user_id: str = field(default="")
    object_key: str = field(default="")
    signature: str = field(default="")

    def __init__(
        self, object_key: str, signature: str, user_id: str, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.object_key = object_key
        self.signature = signature
        self.event_type = event_type or EventType.OBJECT_STORED


@dataclass
class OCREvent(BaseEvent):
    prefix: str = "OCR-EVENT"
    user_id: str = field(default="")
    signature: str = field(default="")
    data: bytes = field(default=None)

    def __init__(
        self, signature: str, data: bytes, user_id: str, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.signature = signature
        self.data = data
        self.user_id = user_id
        self.event_type = event_type or EventType.TEXT_EXTRACTED


@dataclass
class MLEvent(BaseEvent):
    prefix: str = "ML-EVENT"
    user_id: str = field(default="")
    data: dict = field(default_factory=dict)
    source_id: str = field(default="")

    def __init__(
        self, user_id: str, source_id: str, data: dict = None, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_id = source_id
        self.data = data or {}
        self.user_id = user_id
        self.event_type = event_type or EventType.TEXT_LINES_CLASSIFIED


@dataclass
class RedactorEvent(BaseEvent):
    prefix: str = "REDACTOR-EVENT"
    user_id: str = field(default="")
    data: dict = field(default_factory=dict)
    source_id: str = field(default="")

    def __init__(
        self, user_id: str, source_id: str, data: dict = None, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_id = source_id
        self.data = data or {}
        self.user_id = user_id
        self.event_type = event_type or EventType.LINES_REDACTED
