from pydantic import BaseModel, Field
from typing import Optional

class Line(BaseModel):
    uuid: str = ""
    text: str = ""
    text_clean: str = ""
    text_len: int = 0
    bbox: Optional[dict]