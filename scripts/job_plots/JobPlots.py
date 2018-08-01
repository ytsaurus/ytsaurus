from __future__ import print_function

import sys
import math
import argparse
from datetime import datetime
import time
from collections import namedtuple
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
import numpy
from collections import defaultdict

import yt.wrapper as yt

JobInfo = namedtuple("JobInfo", """abs_start_time, abs_end_time, rel_start_time, rel_end_time,
                    abs_start_running, rel_start_running, length,
                    state, job_id_hi, job_id_lo, node, compressed_data_size, uncompressed_data_size""")


def get_operation_lohi(operation_id):
    id_parts = operation_id.split("-")
    lo_id = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    hi_id = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    return lo_id, hi_id

def get_operation_info(lo_id, hi_id):
    operation_info = yt.select_rows(
        """type, state, transient_state, start_time, finish_time, address, job_id_hi, job_id_lo, events,
            statistics from [//sys/operations_archive/jobs]
            where operation_id_lo = {}u and operation_id_hi = {}u""".format(lo_id, hi_id)
    )
    return list(operation_info)


def ts_to_time_str(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")

def time_str_to_ts(time_str):
    return int(time.mktime(datetime.strptime(time_str, "%Y-%m-%dT%X.%fZ").timetuple()))

def get_event_time(phase, events):
    time_str = list(event["time"] for event in events
                         if "phase" in event and event["phase"] == phase)[0]
    return time_str_to_ts(time_str)


def get_jobs(opid):
    res = JobSet()
    op_id_lo, op_id_hi = get_operation_lohi(opid)
    operation_info = get_operation_info(op_id_lo, op_id_hi)
    min_time = min(get_event_time("created", job_info["events"]) for job_info in operation_info)

    for job_info in operation_info:
        state = job_info["state"] or job_info["transient_state"]
        if state in ["completed", "failed"]:
            start_time=get_event_time("created", job_info["events"])
            start_running=get_event_time("running", job_info["events"])
            end_time=get_event_time("finished", job_info["events"])
            res.data[job_info["type"]].append(
                JobInfo(
                    state=state,
                    abs_start_time=start_time,
                    abs_start_running=start_running,
                    abs_end_time=end_time,
                    rel_start_time=start_time - min_time,
                    rel_start_running=start_running - min_time,
                    rel_end_time=end_time - min_time,
                    length=end_time-start_time,
                    job_id_hi=job_info["job_id_hi"],
                    job_id_lo=job_info["job_id_lo"],
                    node=job_info["address"].split(".")[0] if "address" in job_info else "",
                    compressed_data_size=job_info["statistics"]["data"]["input"]["compressed_data_size"]["sum"],
                    uncompressed_data_size=job_info["statistics"]["data"]["input"]["uncompressed_data_size"]["sum"]
                ))
    for job_type in res.data:
        res.data[job_type].sort()
    return res

class JobSet:
    def __init__ (self, opid = ""):
        self.data = defaultdict(list)
    
    def min_val(self, data_type):
        return min([min([getattr(job_info, data_type) for job_info in self.data[job_type]]) for job_type in self.data])
    
    def max_val(self, data_type):
        return max([max([getattr(job_info, data_type) for job_info in self.data[job_type]]) for job_type in self.data])
    
    def filter_jobs(self, job_types = [],
                    starts_after = 0, starts_before = sys.maxint,
                    ends_after = 0, ends_before = sys.maxint,
                    state = ["completed", "failed"]):
        if not job_types:
            job_types = list(self.data.keys())
        new_jobs = JobSet()
        for job_type, jobs_info in self.data.items():
            if not job_type in job_types:
                continue
            for job_info in jobs_info:
                if (starts_after <= job_info.rel_start_time <= starts_before and 
                    ends_after <= job_info.rel_end_time <= ends_before and
                    job_info.state in state):
                    new_jobs.data[job_type].append(job_info)
        return new_jobs
            
    def draw_time_gantt(self):
        for job_type, jobs_info in self.data.items():
            plt.figure(figsize=(10, 6))
            plt.title("\n{} jobs".format(job_type), fontsize=15)
            for y, job_info in enumerate(jobs_info):
                plt.hlines(y, xmin=job_info.rel_start_time, xmax=job_info.rel_start_running, colors='y')
                plt.hlines(y, xmin=job_info.rel_start_running, xmax=job_info.rel_end_time,
                           colors=('g' if job_info.state == "completed" else 'r'))
                    
            red_patch = mpatches.Patch(color='r', label='Failed jobs')
            green_patch = mpatches.Patch(color='g', label='Complited jobs')
            yellow_patch = mpatches.Patch(color='y', label='Preparation')
            plt.legend(handles=[red_patch, green_patch, yellow_patch], loc='upper center',
                       bbox_to_anchor=(0.5, -0.05), fontsize=12, ncol=3)
            plt.show()
    
    def draw_hist(self, data_type = "length", ax = None, other_jobset=None):
        plt.figure(figsize=(10, 6))
        jobsets = ([self] if other_jobset == None else [self, other_jobset])
        min_val = min(jobset.min_val(data_type) for jobset in jobsets)
        max_val = max(jobset.max_val(data_type) for jobset in jobsets)
        bin_count = min(max_val - min_val + 1, 50)
        bins = numpy.linspace(min_val, max_val, bin_count + 1)
        
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.items():
                values = list(getattr(job_info, data_type) for job_info in jobs_info)
                plt.hist(values, bins, rwidth=0.9, alpha=0.5, label=job_type + " jobs in set{}".format(i + 1))
            plt.legend(fontsize=12, ncol=len(self.data.keys()),
                       loc='upper center', bbox_to_anchor=(0.5, -0.05))
        plt.show()
        
    def draw_time_plot(self, step_count=100, other_jobset=None):
        plt.figure(figsize=(10, 6))
        jobsets = ([self] if other_jobset == None else [self, other_jobset])
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.items():
                min_time = min([job_info.rel_start_time for job_info in jobs_info])
                max_time = max([job_info.rel_end_time for job_info in jobs_info])
                time_points = numpy.linspace(min_time, max_time, min(step_count, max_time - min_time + 1))
                job_count = []
                for point in time_points:
                    job_count.append(sum(job_info.rel_start_time <= point <= job_info.rel_end_time
                                         for job_info in jobs_info))
                plt.plot(time_points, job_count, label=job_type + " jobs in set{}".format(i + 1))
        plt.legend(fontsize=12, ncol=len(self.data.keys()),
                   loc='upper center', bbox_to_anchor=(0.5, -0.05))
        plt.show()

