from yt.wrapper.operation_commands import format_operation_stderrs
import yt.wrapper as yt

from collections import defaultdict

yt.config["prefix"] = "//home/ignat/"

@yt.aggregator
def aggregate(rows):
    stat_by_date = defaultdict(dict)
    for row in rows:
        if row["event_type"] == "operation_started":
            time = row["timestamp"][:10]
            stat = stat_by_date[time]
            if "tables" not in stat:
                stat["tables"] = defaultdict(int)
            spec = row["spec"]
            if "input_table_path" in spec:
                stat["tables"][str(spec["input_table_path"])] += 1
            if "input_table_paths" in spec:
                for table in spec["input_table_paths"]:
                    stat["tables"][str(table)] += 1

    for key, value in stat_by_date.iteritems():
        for table, counter in value["tables"].iteritems():
            yield {"date": key, "table": table, "count": counter}

def summarize(key, rows):
    count = 0
    for row in rows:
        count += row["count"]
    yield {"date": key["date"], "table": key["table"], "count": count}


if __name__ == "__main__":
    try:
        yt.run_map(aggregate, "//sys/scheduler/event_log{event_type,operation_id,timestamp,spec}", "scheduler_input_stat", spec={"max_failed_job_count": 1, "data_size_per_job": 1024 ** 3})
        yt.run_map_reduce(None, summarize, "scheduler_input_stat", "scheduler_input_stat_final", spec={"max_failed_job_count": 1}, reduce_by=["date", "table"])
        sizes = {}
        for table in yt.search("/", exclude=["//home/qe", "//sys"], attributes=["compressed_data_size"], node_type="table"):
            sizes[str(table)] = table.attributes["compressed_data_size"]

        table_info_by_date = defaultdict(dict)
        for row in yt.read_table("//home/ignat/scheduler_input_stat_final"):
            table = row["table"]
            date = row["date"]
            table_info_by_date[date][table] = (sizes.get(table, 0), row["count"])

        for date in sorted(table_info_by_date):
            table_info = table_info_by_date[date]
            sum = 0
            for table in table_info:
                sum += table_info[table][0]
            print "INFO FOR DATE", date, "Size =", sum, "Count =", len(table_info)
            for table, value in sorted(table_info.items(), key=lambda item: item[1][0], reverse=True)[:100]:
                print table, value[0], value[1]
            print
    except yt.YtOperationFailedError as error:
        if "stderrs" in error.attributes:
            error.message = error.message + format_operation_stderrs(error.attributes["stderrs"])
        raise
