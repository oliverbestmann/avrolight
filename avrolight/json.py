try:
    import ujson as json
except ImportError:
    import json

dumps = json.dumps
loads = json.loads
