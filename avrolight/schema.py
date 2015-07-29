from json import loads

class Schema(object):
    def __init__(self, json):
        if isinstance(json, str) and json[0] in "{[":
            json = loads(json)

        self.json = json
        self.types = {}

        self._register_types()

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

    def get_type_schema(self, name):
        return self.types[name.lstrip(".")]

    @property
    def start(self):
        return self.json