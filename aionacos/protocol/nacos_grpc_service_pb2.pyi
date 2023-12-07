from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Metadata(_message.Message):
    __slots__ = ["clientIp", "headers", "type"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CLIENTIP_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    clientIp: str
    headers: _containers.ScalarMap[str, str]
    type: str
    def __init__(self, type: _Optional[str] = ..., clientIp: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ["body", "metadata"]
    BODY_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    body: _any_pb2.Any
    metadata: Metadata
    def __init__(self, metadata: _Optional[_Union[Metadata, _Mapping]] = ..., body: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
