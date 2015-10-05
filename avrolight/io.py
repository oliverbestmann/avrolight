"""
This file contains functions to read and write avro packed data.
"""

import binascii
from functools import partial
import struct

from avrolight.schema import Schema

BYTES = [bytearray((idx,)) for idx in range(256)]


def write_null(out, value):
    pass


def write_boolean(out, value):
    out.write(b"\x01" if value else b"\x00")


def write_long(out, value):
    value = (value << 1) ^ (value >> 63)
    while value & ~0x7F:
        out.write(BYTES[(value & 0x7f) | 0x80])
        value >>= 7

    out.write(BYTES[value])


def write_float(out, value):
    out.write(struct.pack(b"<f", value))


def write_double(out, value):
    out.write(struct.pack(b"<d", value))


def write_bytes(out, value):
    write_long(out, len(value))
    out.write(value)


def write_string(out, value):
    write_bytes(out, value.encode("utf8"))


def write_crc32(out, value):
    struct.pack(">I", binascii.crc32(value) & 0xffffffff)


def read_null(fp):
    return None


def read_byte(fp):
    value = fp.read(1)
    if not value:
        raise EOFError()

    return value[0]


def read_boolean(fp):
    return read_byte(fp) != 0


def read_long(fp):
    b = read_byte(fp)
    n = b & 0x7F
    shift = 7
    while b & 0x80:
        b = read_byte(fp)
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


def read_float(fp):
    return struct.unpack("<f", fp.read(4))[0]


def read_double(fp):
    return struct.unpack("<d", fp.read(8))[0]


def read_bytes(fp):
    return fp.read(read_long(fp))


def read_string(fp):
    return read_bytes(fp).decode("utf8")


def read_crc32(fp):
    return struct.unpack(">I", fp.read(4))


def read_blocks(read_item, fp):
    def read_one_block():
        count = read_long(fp)
        if count < 0:
            count = abs(count)
            _ = read_long(fp)

        items = []
        for idx in range(count):
            items.append(read_item())

        return items

    while True:
        items = read_one_block()
        if not items:
            break

        for item in items:
            yield item


PRIMITIVE_READERS = {
    "null": read_null,
    "bytes": read_bytes,
    "boolean": read_boolean,
    "int": read_long,
    "long": read_long,
    "float": read_float,
    "double": read_double,
    "string": read_string
}

PRIMITIVE_WRITERS = {
    "null": write_null,
    "bytes": write_bytes,
    "boolean": write_boolean,
    "int": write_long,
    "long": write_long,
    "float": write_float,
    "double": write_double,
    "string": write_string
}


def remove_schema_parameter(func):
    """Removes the first parameter from a method invocation."""
    return lambda schema, *args: func(*args)


class Writer(object):
    def __init__(self, schema):
        """Creates a new writer for avro packed messages.

        To create a new writer you need to provide a schema for the file to write.
        The schema could be a :class:`avrolight.schema.Schema` instance. If not, it will be
        given to the constructor of :class:`avrolight.schema.Schema`.
        """
        self.schema = schema if isinstance(schema, Schema) else Schema(schema)

        self.writers = dict({key: remove_schema_parameter(func) for key, func in PRIMITIVE_WRITERS.items()}, **{
            "record": self.write_record,
            "array": self.write_array,
            "fixed": self.write_fixed,
            "map": self.write_map,
            "enum": self.write_enum
        })

    def write(self, fp, value):
        """Serializes an object using this writer.

        The object is serialized and written to the given file-like object. The value
        must match the schema of this writer.
        """
        self.write_any(self.schema.toplevel_type, fp, value)

    def write_any(self, schema, out, value):
        if not isinstance(schema, dict):
            schema = {"type": schema}

        field_type = schema["type"]

        # if it is a union field, read the type index first
        if isinstance(field_type, (list, tuple)):
            index, field_type = choose_union_type(field_type, value)
            write_long(out, index)
            return self.write_any(field_type, out, value)

        if isinstance(field_type, str) and field_type not in self.writers:
            schema = self.schema.get_type_schema(field_type)
            return self.write_any(schema, out, value)

        if field_type in self.writers:
            return self.writers[field_type](schema, out, value)
        else:
            raise ValueError("Invalid field type: {}".format(field_type))

    def write_record(self, schema, out, value):
        for field in schema["fields"]:
            field_type = field["type"]
            field_value = value[field["name"]]
            self.write_any(field_type, out, field_value)

    def write_array(self, schema, out, array):
        if array:
            item_schema = schema["items"]
            write_long(out, len(array))
            for value in array:
                self.write_any(item_schema, out, value)

        write_long(out, 0)

    def write_map(self, schema, out, mapping):
        if mapping:
            value_schema = schema["values"]
            write_long(out, len(mapping))
            for key, value in mapping.items():
                write_string(out, key)
                self.write_any(value_schema, out, value)

        write_long(out, 0)

    def write_fixed(self, schema, out, value):
        out.write(value)
        if len(value) != schema["size"]:
            raise ValueError("Invalid length for 'write fixed'")

    def write_enum(self, schema, out, value):
        index = schema["symbols"].index(value)
        write_long(out, index)


TYPES = (
    (dict, "record"),
    (int, "int"),
    (int, "long"),
    (str, "string"),
    (float, "float"),
    (float, "double"),
    (dict, "map"),
    (list, "array"),
    (tuple, "array"),
    (str, "enum"),
    (bytes, "bytes")
)


# noinspection PyShadowingBuiltins
def choose_union_type(union, value):
    if value is None and "null" in union:
        return union.index("null"), "null"

    union_types = {
        type["type"] if isinstance(type, dict) else type: idx
        for idx, type in enumerate(union)
    }

    for type, type_name in TYPES:
        if type_name in union_types and isinstance(value, type):
            index = union_types[type_name]
            return index, union[index]

    raise ValueError("Could not guess union value type")


class Reader(object):
    def __init__(self, schema):
        """Initializes a new reader from a schema.

        See :class:`avrolight.io.Writer` for more information about schema handling
        """
        self.schema = schema if isinstance(schema, Schema) else Schema(schema)

        self.reader = dict({key: remove_schema_parameter(func) for key, func in PRIMITIVE_READERS.items()}, **{
            "record": self.read_record,
            "enum": self.read_enum,
            "array": self.read_array,
            "fixed": self.read_fixed,
            "map": self.read_map
        })

    def read(self, fp):
        """Reads one value using :attr:`schema` from the given file-like object."""
        return self.read_any(self.schema.toplevel_type, fp)

    def read_record(self, schema, fp):
        result = {}
        for field in schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            result[field_name] = self.read_any(field_type, fp)

        return result

    def read_array(self, schema, fp):
        return list(read_blocks(partial(self.read_any, schema["items"], fp), fp))

    def read_map(self, schema, fp):
        item_schema = schema["values"]

        def read_entry():
            name = read_string(fp)
            value = self.read_any(item_schema, fp)
            return name, value

        return dict(read_blocks(read_entry, fp))

    # noinspection PyMethodMayBeStatic
    def read_enum(self, schema, fp):
        index = read_long(fp)
        return schema["symbols"][index]

    # noinspection PyMethodMayBeStatic
    def read_fixed(self, schema, fp):
        return fp.read(schema["size"])

    def read_any(self, schema, fp):
        if not isinstance(schema, dict):
            schema = {"type": schema}

        field_type = schema["type"]

        # if it is a union field, read the type index first
        if isinstance(field_type, (list, tuple)):
            index = read_long(fp)
            return self.read_any(field_type[index], fp)

        if field_type not in self.reader:
            schema = self.schema.get_type_schema(field_type)
            return self.read_any(schema, fp)

        if field_type in self.reader:
            return self.reader[field_type](schema, fp)
        else:
            raise ValueError("Invalid field type: {}".format(field_type))

