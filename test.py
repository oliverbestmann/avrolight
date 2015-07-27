import json
import io
import gc
import time

import avro
import avro.io
import avro.schema
import avro.datafile
from hamcrest import *
import nose

import avrolight

SCHEMAS_TO_VALIDATE = (
    ('"null"', None),
    ('"boolean"', True),
    ('"string"', 'adsfasdf09809dsf-=adsf'),
    ('"bytes"', b'12345abcd'),
    ('"int"', 0),
    ('"int"', 1234),
    ('"long"', 1234),
    ('"float"', 1234.0),
    ('"double"', 1234.0),
    ('{"type": "fixed", "name": "Test", "size": 1}', b'B'),
    ('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', 'B'),
    ('{"type": "array", "items": "long"}', [1, 3, 2]),
    ('{"type": "map", "values": "long"}', {'a': 1, 'b': 3, 'c': 2}),
    ('["string", "null", "long"]', None),
    ("""\
   {"type": "record",
    "name": "Test",
    "fields": [{"name": "f", "type": "long"}]}
   """, {'f': 5}),
    ("""
   {
     "type": "record",
     "name": "Lisp",
     "fields": [{
       "name": "value",
       "type": [
         "null",
         "string",
         {
           "type": "record",
           "name": "Cons",
           "fields": [{"name": "car", "type": "Lisp"},
                      {"name": "cdr", "type": "Lisp"}]
         }
       ]
     }]
   }
   """, {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)


def avro_write_datum(datum, writer_schema):
    writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(writer)
    datum_writer = avro.io.DatumWriter(writer_schema)
    datum_writer.write(datum, encoder)
    return writer.getvalue()


def test_read():
    for schema, value in SCHEMAS_TO_VALIDATE:
        bytes = avro_write_datum(value, avro.schema.Parse(schema))
        schema = json.loads(schema)
        assert_that(avrolight.read(schema, bytes), equal_to(value))


def test_write():
    for schema, value in SCHEMAS_TO_VALIDATE:
        bytes = avro_write_datum(value, avro.schema.Parse(schema))
        schema = json.loads(schema)

        fp = io.BytesIO()
        avrolight.write(schema, fp, value)
        assert_that(fp.getvalue(), equal_to(bytes))


def test_read_container_file():
    for schema, value in SCHEMAS_TO_VALIDATE:
        writer = io.BytesIO()
        datum_writer = avro.io.DatumWriter(avro.schema.Parse(schema))
        container = avro.datafile.DataFileWriter(writer, datum_writer, avro.schema.Parse(schema))

        for _ in range(10):
            container.append(value)

        container.flush()

        values = list(avrolight.read_container(io.BytesIO(writer.getvalue())))
        assert_that(values, has_length(10))
        assert_that(values[0], equal_to(value))


def test_write_container_file():
    for schema, value in SCHEMAS_TO_VALIDATE:
        fp = io.BytesIO()
        with avrolight.ContainerWriter(fp, json.loads(schema)) as writer:
            for _ in range(10):
                writer.write(value)

        values = list(avrolight.read_container(io.BytesIO(fp.getvalue())))
        assert_that(values, has_length(10))
        assert_that(values[0], equal_to(value))


def timeit(name, runs=100000):
    start = time.time()
    for _ in range(runs):
        yield

    duration = time.time() - start
    print("Time {}: {:1.2f}/s".format(name, runs / duration))


def speed_avro_write():
    gc.disable()

    schema, value = SCHEMAS_TO_VALIDATE[-1]
    parsed_schema = avro.schema.Parse(schema)

    for _ in timeit("avro, writing"):
        encoder = avro.io.BinaryEncoder(io.BytesIO())
        datum_writer = avro.io.DatumWriter(parsed_schema)
        datum_writer.write(value, encoder)


def speed_avro_read():
    gc.disable()

    schema, value = SCHEMAS_TO_VALIDATE[-1]
    parsed_schema = avro.schema.Parse(schema)
    bytes = avro_write_datum(value, parsed_schema)

    for _ in timeit("avro, reading"):
        decoder = avro.io.BinaryDecoder(io.BytesIO(bytes))
        datum_reader = avro.io.DatumReader(parsed_schema)
        datum_reader.read(decoder)


def speed_avrolight_write():
    gc.disable()

    schema, value = SCHEMAS_TO_VALIDATE[-1]
    parsed_schema = json.loads(schema)

    for _ in timeit("avrolight, writing"):
        avrolight.write(parsed_schema, io.BytesIO(), value)


def speed_avrolight_read():
    gc.disable()

    schema, value = SCHEMAS_TO_VALIDATE[-1]
    parsed_schema = avrolight.Schema(json.loads(schema))

    buffer = io.BytesIO()
    avrolight.write(parsed_schema, buffer, value)

    for _ in timeit("avrolight, reading"):
        avrolight.read(parsed_schema, buffer.getvalue())


def speed():
    speed_avrolight_read()
    speed_avrolight_write()
    speed_avro_read()
    speed_avro_write()


if __name__ == '__main__':
    nose.runmodule()
