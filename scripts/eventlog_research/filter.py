import sys
import simplejson as json
from collections import defaultdict

history = defaultdict(lambda: [])

def filter(dict, keys):
    for key in dict.keys():
        if key not in keys:
            del dict[key]

for line in sys.stdin:
    obj = json.loads(line)
    for operation in obj["operations"]:
        filter(obj["operations"][operation], ["fair_share_ratio", "usage_ratio", "pool"])
    for pool in obj["pools"]:
        filter(obj["pools"][pool], ["fair_share_ratio", "usage_ratio"])
    print json.dumps(obj)



