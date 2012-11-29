#!/usr/bin/env python

import yt.wrapper as yt

#from time import sleep

def extract_chunk_list_info(table, from_table=True):
    if from_table:
        chunk_list_id = table.attributes["chunk_list_id"]
    else:
        chunk_list_id = yt.get_attribute(table, "chunk_list_id")
    statistics = yt.get("#%s/@statistics" % chunk_list_id)
    return statistics["chunk_list_count"], statistics["chunk_count"]

if __name__ == "__main__":
    bad_balanced_tables = []
    for dir in ["//home", "//statbox"]:
        for table in yt.search(dir, node_type="table", attributes=["chunk_list_id"]):
            try:
                chunk_list_count, chunk_count = extract_chunk_list_info(table)
            except yt.YtError:
                continue
            if chunk_list_count > 2 and 100.0 * chunk_list_count > chunk_count:
                bad_balanced_tables.append(table)
                #print "BEFORE", chunk_list_count, chunk_count
                #yt.write_table(yt.TablePath(table, append=True), [], format=yt.DsvFormat())
                #chunk_list_count, chunk_count = extract_chunk_list_info(table, from_table=False)
                #print "AFTER", chunk_list_count, chunk_count
                #break
    yt.set("//home/ignat/bad_balanced_tables", bad_balanced_tables)
