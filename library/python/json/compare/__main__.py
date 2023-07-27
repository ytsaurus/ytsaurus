import json
import sys

import library.python.json as lpj

data = open(sys.argv[1], 'r').read()

assert json.dumps(lpj.loads(data), indent=4, sort_keys=True) == json.dumps(json.loads(data), indent=4, sort_keys=True)
