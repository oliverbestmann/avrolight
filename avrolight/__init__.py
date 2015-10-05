import json

from io import BytesIO

from avrolight.io import Reader, Writer
from avrolight.container import read_container
from avrolight.container import ContainerWriter
from avrolight.container import append_to_container
from avrolight.schema import Schema

__all__ = ("Reader", "Writer", "read", "write", "read_container", "ContainerWriter", "Schema", "append_to_container")


def read(schema, fp):
    if isinstance(fp, bytes):
        fp = BytesIO(fp)

    return Reader(schema).read(fp)


def write(schema, fp, value):
    return Writer(schema).write(fp, value)
