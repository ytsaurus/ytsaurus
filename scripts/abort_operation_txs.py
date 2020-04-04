#!/usr/bin/env python
import argparse
import pprint
import sys

import yt.wrapper as yt

from yt.yson.yson_types import YsonEntity

for hash in yt.list("//sys/operations"):
	print "hash", hash
	for op in yt.list("//sys/operations/" + hash, attributes=["lock_count"]):
		if op.attributes["lock_count"] > 0:
			print "locked", str(op)
			for lock in yt.get("//sys/operations/" + hash + "/" + str(op) + "/@locks"):
				tx_id = lock["transaction_id"]
				print "tx", tx_id
				yt.abort_transaction(tx_id)
