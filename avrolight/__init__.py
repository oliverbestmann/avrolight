__author__ = 'Oliver Bestmann'

from io import BytesIO

from .io import Reader, Writer
from .container import read_container
from .container import ContainerWriter
from .container import append_to_container
from .schema import Schema

__all__ = ("Reader", "Writer", "read", "write", "read_container", "ContainerWriter", "Schema", "append_to_container")

def read(schema, fp):
    if isinstance(fp, bytes):
        fp = BytesIO(fp)

    return Reader(schema).read(fp)


def write(schema, fp, value):
    return Writer(schema).write(fp, value)
