__author__ = 'Oliver Bestmann'

from .io import Reader, Writer


def read(schema, bytes):
    return Reader(schema).read(bytes)


def write(schema, value):
    return Writer(schema).write(value)
