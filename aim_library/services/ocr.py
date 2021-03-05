import boto3

from services.trp import Document
import PIL
from utils.image import image_to_bytes
from utils.configmanager import ConfigManager
from utils.common import make_url


class Textract:
    __client = None
    _db = None

    @classmethod
    def __get_client(cls):
        if not cls.__client:
            session = boto3.session.Session()
            client = session.client("textract")
            cls.__client = client
        return cls.__client

    def detect_text(self, document, raw_response=False):
        client = self.__get_client()
        if not isinstance(document, dict):
            document = self.__make_document_dict(document)
        response = client.detect_document_text(Document=document)
        if raw_response:
            return response
        return Document(response)

    @staticmethod
    def __make_document_dict(document):
        if isinstance(document, PIL.Image.Image):
            document = image_to_bytes(document)
        if type(document) is bytes or type(document) is bytearray:
            return {"Bytes": document}
        raise TypeError("document should be a stream of bytes.")

    def persist_to_db(self, document):
        from database.mongo import db

        db["raw_text_detecions"].insert_one(document)

