import json
import os
from io import BytesIO

from .io import Reader, read_long
from .io import Writer, write_long

HEADER_SCHEMA = {
    "type": "record",
    "name": "org.apache.avro.file.Header",
    "fields": [
        {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
        {"name": "meta", "type": {"type": "map", "values": "bytes"}},
        {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
    ]
}


def read_container(fp):
    header = Reader(HEADER_SCHEMA).read(fp)
    if header["magic"] != b"Obj\x01":
        raise IOError("Not a valid avro container file")

    sync_marker = header["sync"]

    # parse the schema from the header
    schema = json.loads(header["meta"]["avro.schema"].decode("utf8"))
    reader = Reader(schema)

    while True:
        try:
            count = read_long(fp)
        except EOFError:
            break

        # skip the block size, we dont need that
        read_long(fp)

        for _ in range(count):
            yield reader.read(fp)

        if fp.read(16) != sync_marker:
            raise IOError("sync marker expected")


def append_to_container(fp):
    # read the header to get the schema
    header = Reader(HEADER_SCHEMA).read(fp)
    if header["magic"] != b"Obj\x01":
        raise IOError("Not a valid avro container file")

    # the info from the file
    schema = json.loads(header["meta"]["avro.schema"].decode("utf8"))
    sync_marker = header["sync"]

    # move to the end of the file
    fp.seek(0, os.SEEK_END)

    return ContainerWriter(fp, schema, sync_marker=sync_marker)


class ContainerWriter(object):
    def __init__(self, fp, schema, sync_marker=None):
        self.writer = Writer(schema)
        self.fp = fp
        self.sync_marker = sync_marker or os.urandom(16)
        self.header_written = sync_marker is not None

        self.records = 0
        self.buffer = BytesIO()

    def write_header(self):
        header = {
            "magic": b"Obj\x01",
            "meta": {
                "avro.schema": json.dumps(self.writer.schema.json).encode("utf8"),
                "avro.codec": b"null"
            },
            "sync": self.sync_marker
        }

        Writer(HEADER_SCHEMA).write(self.fp, header)

    def write(self, message):
        self.writer.write(self.buffer, message)
        self.records += 1

        if self.buffer.tell() > 1024 ** 2:
            self.flush()

    def flush(self):
        if not self.header_written:
            self.write_header()
            self.header_written = True

        if not self.records:
            return

        write_long(self.fp, self.records)
        write_long(self.fp, self.buffer.tell())
        self.fp.write(self.buffer.getbuffer())
        self.fp.write(self.sync_marker)
        self.fp.flush()

        self.records = 0
        self.buffer = BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
