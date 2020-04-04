#!/usr/bin/python
import yt.wrapper as yt
import argparse
import logging
import sys

logging.basicConfig(format="%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s", level=logging.INFO)

yt.config['allow_http_requests_to_yt_from_job'] = True
yt.config['token'] = "AVImjVYAAABe5eb9k7qoS_WSGomPKZxJbA"

class PrepareOverconsumptingJobs(object):
    def __init__(self, newer_than):
        self.newer_than = newer_than

    def __call__(self, row):
        if (not self.newer_than is None) and row["timestamp"] < self.newer_than:
            return
        if row["event_type"] == "job_completed":
            if not ("statistics" in row and "job_proxy" in row["statistics"]):
                return
            if "max_memory" in row["statistics"]["job_proxy"]:
                max_memory = int(row["statistics"]["job_proxy"]["max_memory"]["sum"])
            else:
                return
            memory_reserve = int(row["statistics"]["job_proxy"]["memory_reserve"]["sum"])
            if "memory_reserve_factor_x10000" in row["statistics"]["job_proxy"]:
                memory_reserve_factor = float(row["statistics"]["job_proxy"]["memory_reserve_factor_x10000"]["sum"]) / 1e4 # Fix x1000 -> x10000 in future.
            elif "memory_reserve_factor_x1000" in row["statistics"]["job_proxy"]:
                memory_reserve_factor = float(row["statistics"]["job_proxy"]["memory_reserve_factor_x1000"]["sum"]) / 1e4 # Fix x1000 -> x10000 in future.
            else:
                return

            memory_footprint = 128 * 1024 * 1024
            memory_reserve = int((memory_reserve - memory_footprint) / memory_reserve_factor + memory_footprint)            
            if max_memory > memory_reserve:
                del row["statistics"]
                row["max_memory"] = max_memory
                row["memory_reserve"] = memory_reserve
                row["overconsumption_factor"] = max_memory * 1.0 / max(1.0, memory_reserve)
                row["memory_reserve_factor"] = memory_reserve_factor
                yield row
        elif row["event_type"] == "operation_started":
            yield row

class JoinSpecsAndOutputTableInfo(object):
    def __call__(self, key, rows):
        rows = list(rows)
        operation_id = key["operation_id"]
        operation_info = None
        
        TOP = 3
        by_type = dict()
        if not any(row["event_type"] != "operation_started" for row in rows):
            return
        for row in rows:
            if row["event_type"] == "operation_started":
                output_table_compression_codecs = set()
                output_table_erasure_codecs = set()
                if "output_table_paths" in row["spec"]:
                    for table in row["spec"]["output_table_paths"]:
                        path = table 
                        tmp = path + "/@compression_codec"
                        try:
                            compression_codec = yt.get(path + "/@compression_codec")
                            output_table_compression_codecs.add(compression_codec)
                        except:
                            pass
                        try:
                            erasure_codec = yt.get(path + "/@erasure_codec")
                            output_table_erasure_codecs.add(erasure_codec)
                        except:
                            pass
                operation_info = dict()
                operation_info["output_table_compression_codecs"] = ' '.join(output_table_compression_codecs)
                operation_info["output_table_erasure_codecs"] = ' '.join(output_table_erasure_codecs)
                operation_info["operation_spec"] = row["spec"]
            else:
                job_type = row["job_type"]
                if not job_type in by_type:
                    by_type[job_type] = []
                by_type[job_type].append(row)
                by_type[job_type].sort(key=lambda x: -x["overconsumption_factor"])
                if len(by_type[job_type]) > TOP:
                    by_type[job_type] = by_type[job_type][:TOP]    

        if operation_info is None:
            return

        for _type in by_type:
            for row in by_type[_type]:
                row.update(operation_info)
                yield row

def main():
    parser = argparse.ArgumentParser(description="Parses scheduler event log in order to find jobs for " + 
                                                 "which we underestimated the jobproxy memory consumption.")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy", required=True)
    parser.add_argument("--input-table", type=str, help="Path to the scheduler event log, by default //sys/scheduler/event_log",
                        default="//sys/scheduler/event_log")
    parser.add_argument("--output-table", type=str, help="Path where output table will be created", required=True)
    parser.add_argument("--newer-than", type=str, help="Timestamp in format 2016-05-28T21:02, will be compared with real timestamp as strings")

    args = parser.parse_args()
    source_path = "<columns=[event_type;node_address;operation_id;timestamp;job_id;job_type;statistics;spec]>" + args.input_table
    output_path = args.output_table
    newer_than = None
    if "newer_than" in args:
        newer_than = args.newer_than

    yt.run_map_reduce(PrepareOverconsumptingJobs(newer_than), JoinSpecsAndOutputTableInfo(),
                      source_path, output_path, spec={"pool": "production", "min_share_ratio": 0.1, "partition_count": 10}, reduce_by=["operation_id"])

    logging.info("Sorting resulting table...")
    yt.run_sort(output_path, output_path + "_sorted", sort_by=["overconsumption_factor"])

if __name__ == "__main__":
    main()
