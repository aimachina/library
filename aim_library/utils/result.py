from http.client import responses
import traceback as tb
from typing import runtime_checkable, TypeVar, Any, Protocol, Dict, Tuple

T = TypeVar("T", covariant=True)
E = TypeVar("E", covariant=True)
U = TypeVar("U")

@runtime_checkable
class Result(Protocol):
    def is_ok(self) -> bool:
        ...

    def is_err(self) -> bool:
        ...

    def ok(self) -> Any:
        ...

    def err(self) -> None:
        ...

    @property
    def value(self) -> Any:
        ...

    @property
    def code(self) -> int:
        ...

    @property
    def status(self) -> str:
        ...

    def as_dict(self) -> Any:
        ...

    def as_response(self) -> Tuple[Dict[str, Any], int]:
        ...

    def from_dict(self, data: Dict[Any, Any]) -> "Result":
        ...


class Ok:
    def __init__(self, value: Any = True, code: int = 200, status: str = "") -> None:
        self._value = value
        self._code = code
        self._status = status or responses.get(code, "OK")

    def __repr__(self) -> str:
        return f"Ok({repr(self._value)})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.value == other.value

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((True, self._value))

    def is_ok(self) -> bool:
        return True

    def is_err(self) -> bool:
        return False

    def ok(self) -> Any:
        return self.as_dict()

    def err(self) -> None:
        return None

    @property
    def value(self) -> Any:
        return self._value

    @property
    def code(self) -> int:
        return self._code

    @property
    def status(self) -> str:
        return self._status

    def as_dict(self) -> Any:
        return {
            "value": self._value,
            "code": self._code,
            "status": self._status,
        }

    def as_response(self) -> Tuple[Dict[str, Any], int]:
        resp = self.as_dict()
        code = resp.pop("code")
        return resp, code

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Ok":
        return cls(**data)


class Error:
    def __init__(self, value: Any, code: int = 500, status: str = "") -> None:
        if not isinstance(value, Exception):
            value = ConsumerError(value)
        self._value = value
        self._code = code
        self._status = status or responses.get(code, "Internal Server Error")

    def __repr__(self) -> str:
        return f"Err({repr(self._value)})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Error) and self.value == other.value

    def __ne__(self, other: Any) -> bool:
        return not (self == other)

    def __hash__(self) -> int:
        return hash((False, self._value))

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True

    def ok(self) -> None:
        return None

    def err(self) -> Any:
        return self.as_dict()

    @property
    def value(self) -> Any:
        return self._value

    @property
    def code(self) -> int:
        return self._code

    @property
    def status(self) -> str:
        return self._status

    def as_dict(self) -> Any:
        return {
            "value": repr(self._value),
            "code": self._code,
            "status": self._status,
        }

    def exc(self) -> Exception:
        return self.value

    def as_response(self) -> Tuple[Dict[str, Any], int]:
        resp = self.as_dict()
        code = resp.pop("code")
        return resp, code

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Error":
        return cls(**data)

    def traceback(self) -> str:
        return "".join(tb.format_exception(None, self.value, self.value.__traceback__))


class ConsumerError(Exception):
    pass
