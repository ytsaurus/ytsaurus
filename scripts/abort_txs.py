#!/usr/bin/env python
import argparse
import pprint
import sys

import yt.wrapper as yt

from yt.yson.yson_types import YsonEntity

for item in yt.search("//home/logfeller/staging-area", attributes=["lock_count"]):
	path = str(item)
	if item.attributes["lock_count"] > 0:
		print "locked", item
		for lock in yt.get(item + "/@locks"):
			tx_id = lock["transaction_id"]
			print "abort tx", tx_id
			yt.abort_transaction(tx_id)
