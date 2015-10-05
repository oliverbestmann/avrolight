import avrolight.json

class Schema(object):
    def __init__(self, json):
        """Parses a new schema from a json encoded string or from a map."""
        if isinstance(json, str) and json[0] in "{[":
            self._str = json
            self.json = avrolight.json.loads(json)
        else:
            self._str = None
            self.json = json

        self.types = {}
        self._register_types()

    def get_type_schema(self, name):
        """Gets the schema for a type name"""
        return self.types[name.lstrip(".")]

    @property
    def toplevel_type(self):
        """The toplevel type of this schema"""
        return self.json

    def _register_types(self):
        def _walk_list(schemata):
            for subschema in schemata:
                _walk_schema(subschema)

        def _walk_schema(schema):
            if isinstance(schema, (list, tuple)):
                _walk_list(schema)

            elif isinstance(schema, dict):
                if "name" in schema:
                    self._register_type(schema["name"], schema)

                if schema["type"] == "record":
                    _walk_list([field["type"] for field in schema["fields"]])

        _walk_schema(self.json)

    def _register_type(self, name, schema):
        self.types[name.lstrip(".")] = schema

    def __str__(self):
        if self._str is None:
            self._str = avrolight.json.dumps(self.json)

        return self._str
