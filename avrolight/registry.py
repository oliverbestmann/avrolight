import io
import hashlib
import logbook
import requests
import avrolight

from abc import ABCMeta, abstractmethod
from base64 import b64decode
from first import first

__all__ = [
    "RegistryClient", "ConsulRegistryClient", "CachingRegistryClient", "NoopRegistryClient",
    "serialize", "deserialize"
]

logger = logbook.Logger(__file__)


class RegistryClient(metaclass=ABCMeta):
    @abstractmethod
    def put(self, schema, force=False):
        """Puts the given schema into the schema registry. If force is set
        to `False`, a check will be performed to see, if the schema is already
        present it the registry, before putting it there.
        This method returns the hash of the schema.

        :param avrolight.Schema schema: The schema to put into the registry.
        :param bool force:    Always put the schema, do not check if it is already present
                              in the registry.
        :rtype: bytes
        """

    @abstractmethod
    def get(self, schema_hash):
        """Gets the schema from the registry.
        Raises `KeyError` if no schema with this hash could be found.

        :param bytes schema_hash: The schema hash to query
        :rtype: avrolight.Schema
        """

    @property
    def cached(self):
        return CachingRegistryClient(self)


class NoopRegistryClient(RegistryClient):
    def get(self, schema_hash):
        raise KeyError("Noop registry client can not resolve hash {}".format(schema_hash))

    def put(self, schema, force=False):
        pass


class CachingRegistryClient(RegistryClient):
    def __init__(self, registry):
        self.registry = registry
        self.cache = {}

    def put(self, schema, force=False):
        schema_bytes, schema_hash = serialize_schema(schema)
        if not force and schema_hash in self.cache:
            logger.debug("Not putting, schema already cached")
            return schema_hash

        # store the schema in the backend
        schema_hash = self.registry.put(schema, force)
        self.cache[schema_hash] = schema
        return schema_hash

    def get(self, schema_hash):
        try:
            return self.cache[schema_hash]
        except KeyError:
            schema = self.registry.get(schema_hash)
            self.cache[schema_hash] = schema
            return schema

    @property
    def cached(self):
        return self


class ConsulRegistryClient(RegistryClient):
    def __init__(self, endpoints, prefix="avro-schemas"):
        if isinstance(endpoints, str):
            endpoints = [endpoints]

        self.endpoints = tuple(endpoints)
        self.prefix = prefix

        if not first(self.endpoints):
            raise ValueError("Endpoints must not be empty")

    def get(self, schema_hash):
        last_error = None
        for uri in self._consul_uris(schema_hash):
            try:
                logger.debug("Retrieving schema from {}", uri)

                response = requests.get(uri)
                response.raise_for_status()
                response = response.json()
                if not response:
                    continue

                encoded_schema = first(response)["Value"]
                return avrolight.Schema(b64decode(encoded_schema.encode()).decode())
            except Exception as error:
                last_error = error

        raise KeyError(schema_hash) from last_error

    def put(self, schema, force=False):
        # serialize schema
        schema_bytes, schema_hash = serialize_schema(schema)

        if not force:
            try:
                self.get(schema_hash)
                return schema_hash
            except KeyError:
                pass

        last_error = None
        for uri in self._consul_uris(schema_hash):
            try:
                logger.debug("Putting schema to {}", uri)

                #: :type: requests.Response
                response = requests.put(uri, data=schema_bytes)
                response.raise_for_status()
                return schema_hash
            except requests.RequestException as error:
                last_error = error

        # re-raise the last error
        raise last_error

    def _consul_uris(self, schema_hash):
        """Generates the uris for the given schema hash.

        :param bytes schema_hash: The schema hash to generate uris for.
        """
        return ("/".join((endpoint, "v1/kv", self.prefix, schema_hash.decode())) for endpoint in self.endpoints)


def serialize_schema(schema):
    """Serializes the schema. Returns the serialized schema and
    the hash of that schema as a namedtuple. Both values are returned as 'bytes'.

    :rtype: (bytes, bytes)
    """
    assert isinstance(schema, avrolight.Schema)
    schema_bytes = schema.as_bytes
    schema_hash = hashlib.md5(schema_bytes).hexdigest().encode()
    return schema_bytes, schema_hash


def serialize(client, schema, message):
    schema_hash = client.put(schema)
    out = io.BytesIO()
    out.write(schema_hash)
    avrolight.write(schema, out, message)
    return out.getvalue()


def deserialize(client, message):
    fp = io.BytesIO(message)
    schema = client.get(fp.read(32))
    return avrolight.read(schema, fp)


def main():
    logbook.StderrHandler(level=logbook.DEBUG).push_application()

    schema = avrolight.Schema('{"type": "int"}')
    client = ConsulRegistryClient("http://localhost:8500").cached
    client.put(schema)
    client.put(schema)
    client.get(b"33865377dc63fad265ef74571158cb52")


if __name__ == '__main__':
    main()
