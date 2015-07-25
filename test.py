import json
import io
import gc
import time

import avro
import avro.io
import avro.schema
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

        print(schema, repr(value))
        assert_that(avrolight.write(schema, value), equal_to(bytes))

        assert_that(avrolight.read(schema, bytes), equal_to(value))


def timeit(name, runs=1000000):
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

    for _ in timeit("avro, writing"):
        avrolight.write(parsed_schema, value)


def speed_avrolight_read():
    gc.disable()

    schema, value = SCHEMAS_TO_VALIDATE[-1]
    parsed_schema = json.loads(schema)
    bytes = avrolight.write(parsed_schema, value)

    for _ in timeit("avro, reading"):
        avrolight.read(parsed_schema, bytes)


if __name__ == '__main__':
    speed_avro_read()
    speed_avro_write()
    speed_avrolight_read()
    speed_avrolight_write()

    nose.runmodule()
