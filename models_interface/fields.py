from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class StringFieldDetails(BaseModel):
    value: str = None
    uuid_line: str = None
    field_label: Optional[str] 


class DateTimedDetails(BaseModel):
    value: datetime = None
    uuid_line: str = None
    field_label: Optional[str]