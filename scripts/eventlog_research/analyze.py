import sys
import json
from collections import defaultdict

history = defaultdict(lambda: [])

for line in sys.stdin:
    obj = json.loads(line)
    for operation, info in obj["operations"].iteritems():
        history[operation].append((info["fair_share_ratio"], info["usage_ratio"]))


#for operation, hist in history.iteritems():
#    residue = 0.0
#    count = 0
#    for fair_share, usage in hist:
#        count += 1
#        if fair_share > usage:
#            residue += fair_share - usage
#    residue /= count
#    if residue > 1e-3:
#        print operation, residue


