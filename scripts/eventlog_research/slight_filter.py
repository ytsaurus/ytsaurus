import sys
import simplejson as json
from collections import defaultdict



history = defaultdict(lambda: [])

for line in sys.stdin:
    obj = json.loads(line)
    for operation in obj["operations"]:
        del obj["operations"][operation]["resource_usage"]
        del obj["operations"][operation]["resource_limits"]
        del obj["operations"][operation]["resource_demand"]
    for pool in obj["pools"]:
        del obj["pools"][pool]["resource_usage"]
        del obj["pools"][pool]["resource_limits"]
        del obj["pools"][pool]["resource_demand"]
    print json.dumps(obj)



