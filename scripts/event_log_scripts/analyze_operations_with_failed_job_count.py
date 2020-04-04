from yt.wrapper.operation_commands import format_operation_stderrs
import yt.wrapper as yt

from collections import defaultdict
from copy import deepcopy

yt.config["prefix"] = "//home/ignat/"

@yt.aggregator
def aggregate(rows):
    stat = {}
    for row in rows:
        if row["event_type"] in ("job_failed", "operation_failed", "operation_completed"):
            op_id = row["operation_id"]
            if op_id not in stat:
                stat[op_id] = {"failed": False, "failed_job_count": 0}
            if row["event_type"] == "job_failed":
                stat[op_id]["failed_job_count"] += 1
            elif row["event_type"] == "operation_failed":
                stat[op_id]["failed"] = True
                stat[op_id]["date"] = row["timestamp"][:10]
            elif row["event_type"] == "operation_completed":
                stat[op_id]["failed"] = False
                stat[op_id]["date"] = row["timestamp"][:10]

    for key, value in stat.iteritems():
        value = deepcopy(value)
        value["operation_id"] = key
        yield value

class OperationAggregator(object):
    def __init__(self):
        self.stat_by_date = defaultdict(lambda: defaultdict(int))

    def __call__(self, key, rows):
        failed = False
        failed_job_count = 0
        date = None
        for row in rows:
            if row["failed"]:
                failed = True
            failed_job_count += row["failed_job_count"]
            if "date" in row:
                date = row["date"]

        if failed_job_count > 0 and date is not None:
            self.stat_by_date[date][(failed_job_count, failed)] += 1

    def finish(self):
        for date in self.stat_by_date:
            for key in self.stat_by_date[date]:
                failed_job_count, failed = key
                yield {"date": date, "failed_job_count": failed_job_count, "failed": failed, "count": self.stat_by_date[date][key]}

class DateAggregator(object):
    def __call__(self, key, rows):
        stat = defaultdict(int)
        for row in rows:
            stat[(row["failed_job_count"], row["failed"])] += row["count"]

        date = key["date"]
        for key in stat:
            failed_job_count, failed = key
            yield {"date": date, "failed_job_count": failed_job_count, "failed": failed, "count": stat[key]}



if __name__ == "__main__":
    try:
        yt.run_map(aggregate, "//sys/scheduler/event_log.1{event_type,operation_id,timestamp}", "scheduler_failed_stat", spec={"max_failed_job_count": 1, "data_size_per_job": 1024 ** 3})
        yt.run_map_reduce(None, OperationAggregator(), "scheduler_failed_stat", "scheduler_failed_stat_aggregated", spec={"max_failed_job_count": 1}, reduce_by=["operation_id"])
        yt.run_map_reduce(None, DateAggregator(), "scheduler_failed_stat_aggregated", "scheduler_failed_stat_final", spec={"max_failed_job_count": 1}, reduce_by=["date"])
        results = sorted(list(yt.read_table("scheduler_failed_stat_final")), key=lambda item: (item["date"], item["failed"], item["failed_job_count"]))
        for result in results:
            print result["date"], result["failed"], result["failed_job_count"], result["count"]
    except yt.YtOperationFailedError as error:
        if "stderrs" in error.attributes:
            error.message = error.message + format_operation_stderrs(error.attributes["stderrs"])
        raise
