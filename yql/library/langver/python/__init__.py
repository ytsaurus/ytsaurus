import json
from library.python import resource

__all__ = ['get_max_released', 'get_valid']

_json = None
_max_released = None
_valid = None


def _load_json():
    global _json
    if _json is None:
        _json = json.loads(resource.find('/yql/langver.json'))


def get_max_released():
    global _max_released
    if _max_released is None:
        _load_json()
        _max_released = _json['max_released']
    return _max_released


def get_valid():
    global _valid
    if _valid is None:
        _load_json()
        _valid = _json['valid']
    return _valid
