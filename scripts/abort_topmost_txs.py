from yt.wrapper.etc_commands import execute_batch
import yt.wrapper as yt

import sys

txs = yt.list("//sys/topmost_transactions")
print >>sys.stderr, "Found", len(txs), "topmost transactions"

for i in xrange(len(txs) / 100 + 1):
    start = i * 100
    end = min(len(txs), (i + 1) * 100)
    if start >= end:
        break
    requests = []
    print >>sys.stderr, "Aborting transaction from", start, "to", end
    for j in xrange(start, end):
        requests.append({"command": "abort_tx", "parameters": {"transaction_id": txs[j]}})
    execute_batch(requests)
