#!/usr/bin/env python3

from distutils.core import setup

setup(name="avrolight",
      version="1.0.2",
      description="A light and fast implementation of the avro message format",
      author="Oliver Bestmann",
      author_email="oliver.bestmann@googlemail.com",
      url="https://github.com/oliverbestmann/avrolight",
      requires=["cached_property"],
      packages=["avrolight"])
