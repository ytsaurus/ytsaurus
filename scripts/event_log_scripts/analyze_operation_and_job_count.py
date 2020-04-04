from yt.wrapper.operation_commands import format_operation_stderrs
import yt.wrapper as yt

from collections import defaultdict

yt.config["prefix"] = "//home/ignat/"

@yt.aggregator
def aggregate(rows):
    stat_by_date = defaultdict(dict)
    for row in rows:
        if row["event_type"] == "job_started":
            time = row["timestamp"][:10]
            stat = stat_by_date[time]
            if "job_count" not in stat:
                stat["job_count"] = 0
            if "operations" not in stat:
                stat["operations"] = set()
            stat["job_count"] += 1
            stat["operations"].add(row["operation_id"])

    for key, value in stat_by_date.iteritems():
        yield {"date": key, "job_count": value["job_count"], "operations": list(value["operations"])}

def summarize(key, rows):
    job_count = 0
    operations = set()
    for row in rows:
        job_count += row["job_count"]
        operations |= set(row["operations"])
    yield {"date": key["date"], "job_count": job_count, "operation_count": len(operations)}


if __name__ == "__main__":
    try:
        yt.run_map(aggregate, "//sys/scheduler/event_log{event_type,operation_id,timestamp}", "scheduler_stat", spec={"max_failed_job_count": 1, "data_size_per_job": 1024 ** 3})
        yt.run_map_reduce(None, summarize, "scheduler_stat", "scheduler_stat_final", spec={"max_failed_job_count": 1}, reduce_by=["date"])
        results = sorted(list(yt.read_table("scheduler_stat_final")), key=lambda item: item["date"])
        for result in results:
            print result["date"], result["operation_count"], result["job_count"]
    except yt.YtOperationFailedError as error:
        if "stderrs" in error.attributes:
            error.message = error.message + format_operation_stderrs(error.attributes["stderrs"])
        raise
