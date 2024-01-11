import yt.wrapper as yt

import sys

##################################################################

def parse_replica_spec(yson):
    try:
        dct = yt.yson.loads(yson, yson_type="map_fragment")

        params = {
            "path": ( yt.yson.YsonString, True, None ),
            "cluster": ( yt.yson.YsonString, False, None ),
            "mode": ( yt.yson.YsonString, False, None ),
            "enable": ( yt.yson.YsonBoolean, False, True ),
        }

        for k in dct.keys():
            if k not in params:
                raise RuntimeError('Unexpected key "{}"'.format(k))

        for param, (expected_type, required, default) in params.items():
            if param not in dct:
                if required:
                    raise RuntimeError('Parameter "{}" is required'.format(param))
                else:
                    dct[param] = default
            else:
                if type(dct[param]) != expected_type:
                    raise RuntimeError('Value of "{}" has wrong type: expected {}, got {}'
                        .format(param, expected_type, type(dct[param])))

        if dct["mode"] not in ("sync", "async", None):
            raise RuntimeError('"mode" should be "sync" or "async"')

        return dct
    except Exception as e:
        print(e)
        raise TypeError()
