"""An utility for plotting job statistics."""

from __future__ import print_function

from yt.common import guid_to_parts, parts_to_guid
from yt.wrapper import YtClient
import yt.wrapper as yt

from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
import numpy

import sys
import math
import argparse
from datetime import datetime
import time
import calendar
from collections import namedtuple
from collections import defaultdict


"""Object for job representation."""
JobInfo = namedtuple(
    "JobInfo", [
        "abs_start_time", "abs_end_time", "abs_start_running", 
        "rel_start_time", "rel_end_time", "rel_start_running",
        "length", "state", "job_id_hi", "job_id_lo", "node",
        "input_compressed_data_size", "input_uncompressed_data_size", "input_data_weight", "input_row_count",
        "output_compressed_data_size", "output_uncompressed_data_size", "output_data_weight", "output_row_count"
    ]
)


def format_lohi(id_lo, id_hi, prefix='', id_as_parts=True):
    if id_as_parts:
        return "{}lo = {:20d}, {}hi = {:20d}".format(prefix, id_lo, prefix, id_hi)
    else:
        return "{:32}".format(parts_to_guid(id_hi, id_lo))


def ts_to_time_str(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def covert_to_list(elem):
    """To treat simple type arguments and list arguments in the same way."""
    if isinstance(elem, list):
        return elem
    return [elem]


def get_operation_info(op_id, client):
    """Get job statistics from the archive."""
    hi_id, lo_id = guid_to_parts(op_id)
    operation_info = client.select_rows(
        """type, state, transient_state, start_time, finish_time, address,
            job_id_hi, job_id_lo, events, statistics from [//sys/operations_archive/jobs]
            where operation_id_lo = {}u and operation_id_hi = {}u""".format(lo_id, hi_id)
    )
    return list(operation_info)


def get_event_time(phase, events):
    """Get starting of some phase for particular job."""
    time_str = next(
        event["time"] for event in events if "phase" in event and event["phase"] == phase
    )
    return yt.common.date_string_to_timestamp(time_str)


def get_data_from_output_tables(data_type, output):
    """Get particular data type from all output tabels as a list."""
    return list(output[table_num][data_type]["sum"] for table_num in output.keys())


def get_jobs(op_id, client):
    """
    Get set of all jobs in operation as a JobSet object by operation id.
    Takes YtClient object for obtaining data.
    """
    res = JobSet()
    operation_info = get_operation_info(op_id, client)
    
    # For calculation of relative time i.e. time from the operation start:
    min_time = min(get_event_time("created", job_info["events"]) for job_info in operation_info)

    for job_info in operation_info:
        state = job_info["state"] or job_info["transient_state"]
        if state in ["completed", "failed"]:
            start_time = get_event_time("created", job_info["events"])
            start_running = get_event_time("running", job_info["events"])
            end_time = get_event_time("finished", job_info["events"])
            output_compressed_data_size = get_data_from_output_tables(
                "compressed_data_size",
                job_info["statistics"]["data"]["output"]
            )
            output_uncompressed_data_size = get_data_from_output_tables(
                "uncompressed_data_size",
                job_info["statistics"]["data"]["output"]
            )
            output_data_weight = get_data_from_output_tables("data_weight", job_info["statistics"]["data"]["output"])
            output_row_count = get_data_from_output_tables("row_count", job_info["statistics"]["data"]["output"])
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
                    input_compressed_data_size=job_info["statistics"]["data"]["input"]["compressed_data_size"]["sum"],
                    input_uncompressed_data_size=job_info["statistics"]["data"]["input"]["uncompressed_data_size"]["sum"],
                    input_data_weight=job_info["statistics"]["data"]["input"]["data_weight"]["sum"],
                    input_row_count=job_info["statistics"]["data"]["input"]["row_count"]["sum"],
                    output_compressed_data_size=output_compressed_data_size,
                    output_uncompressed_data_size=output_uncompressed_data_size,
                    output_data_weight=output_data_weight,
                    output_row_count=output_data_weight
                )
            )
    for job_type in res.data:
        res.data[job_type].sort()
    return res


class JobSet(object):
    """
    Class for storing jobs and ploting them. Jobs are stored in a dictionary of lists, separated by their type.
    """
    def __init__ (self):
        self.data = defaultdict(list)
    
    def aggregate(self, data_type, func, table_num=0):
        return func(covert_to_list(getattr(job_info, data_type))[table_num]
                    for job_info_per_type in self.data.itervalues() 
                    for job_info in job_info_per_type)
    
    def filter_jobs(
        self, job_types=None,
        rel_starts_after=0, rel_starts_before=float('inf'),
        rel_ends_after=0, rel_ends_before=float('inf'),
        abs_starts_after=0, abs_starts_before=float('inf'),
        abs_ends_after=0, abs_ends_before=float('inf'),
        states=("completed", "failed"),
    ):
        """Get new set of jobs, filtered by type, state and time limits."""
        if job_types is None:
            job_types = tuple(self.data.keys())
        new_jobs = JobSet()
        for job_type, jobs_info in self.data.iteritems():
            if not job_type in job_types:
                continue
            for job_info in jobs_info:
                if (rel_starts_after <= job_info.rel_start_time <= rel_starts_before and 
                    rel_ends_after <= job_info.rel_end_time <= rel_ends_before and
                    abs_starts_after <= job_info.abs_start_time <= abs_starts_before and 
                    abs_ends_after <= job_info.abs_end_time <= abs_ends_before and
                    job_info.state in states):
                    new_jobs.data[job_type].append(job_info)
        return new_jobs
     
    def prepare_plot(self, width=10, height=6, title="", xlabel="", ylabel=""):
        """Set plot properties."""
        plt.figure(figsize=(width, height))
        plt.title(title, fontsize=15)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
    
    def draw_time_gantt(self):
        """Draw gantt chart illustrating preparation and execution periods for every job."""
        for job_type, jobs_info in self.data.iteritems():
            self.prepare_plot(title="\n{} jobs".format(job_type), xlabel="time", ylabel="jobs")
            for y, job_info in enumerate(jobs_info):
                plt.hlines(y, xmin=job_info.rel_start_time, xmax=job_info.rel_start_running, colors='y')
                plt.hlines(
                    y, xmin=job_info.rel_start_running, xmax=job_info.rel_end_time,
                    colors=('g' if job_info.state == "completed" else 'r')
                )
            
            #Creating the legend:
            red_patch = mpatches.Patch(color="r", label="Failed jobs")
            green_patch = mpatches.Patch(color="g", label="Completed jobs")
            yellow_patch = mpatches.Patch(color="y", label="Preparation")
            plt.legend(
                handles=[red_patch, green_patch, yellow_patch], loc="upper center",
                bbox_to_anchor=(0.5, -0.1), fontsize=12, ncol=3
            )
            plt.show()
    
    def draw_hist(self, data_type="length", table_num=0, other_jobset=None):
        """
        Draw histogram for selected data type.
        If data type refers to output, number of output table should be provided (default is 0).
        If other_jobset argument is passed, comparative chart for two operations will be drawn.
        """
        self.prepare_plot(title="{} histogram".format(data_type), xlabel=data_type, ylabel="jobs number")
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        
        # Find data boundaries for calculation of bins edges:
        min_val = min(jobset.aggregate(data_type, min, table_num) for jobset in jobsets)
        max_val = max(jobset.aggregate(data_type, max, table_num) for jobset in jobsets)
        bin_count = min(max_val - min_val + 1, 50)
        bins = numpy.linspace(min_val, max_val, bin_count + 1)
        
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.iteritems():
                values = [covert_to_list(getattr(job_info, data_type))[table_num] for job_info in jobs_info]
                plt.hist(values, bins, rwidth=0.9, alpha=0.5, label=job_type + " jobs in set{}".format(i + 1))
        plt.legend(
            fontsize=12, ncol=len(self.data.keys()),
            loc="upper center", bbox_to_anchor=(0.5, -0.1)
        )
        plt.show()
    
    def draw_time_plot(self, step_count=100, other_jobset=None):
        """
        Draw line graph illustrating number of running jobs during the operation.
        If other_jobset argument is passed, comparative chart for two operations will be drawn.
        """
        self.prepare_plot(xlabel="time", ylabel="jobs number")
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.iteritems():
                min_time = min(job_info.rel_start_time for job_info in jobs_info)
                max_time = max(job_info.rel_end_time for job_info in jobs_info)
                time_points = numpy.linspace(min_time, max_time, min(step_count, max_time - min_time + 1))
                job_count = []
                for point in time_points:
                    job_count.append(sum(job_info.rel_start_time <= point <= job_info.rel_end_time
                                         for job_info in jobs_info))
                plt.plot(time_points, job_count, label=job_type + " jobs in set{}".format(i + 1))
        plt.legend(
            fontsize=12, ncol=len(self.data.keys()),
            loc="upper center", bbox_to_anchor=(0.5, -0.1)
        )
        plt.show()
        
    def print_text_data(self, id_as_parts=True):
        """
        Print node id, job id and running time for every job.
        """
        for job_type, jobs_info in self.data.iteritems():
            sys.stdout.write("{} jobs:\n".format(job_type))
            for job_info in jobs_info:
                sys.stdout.write("  {}: {} [{} - {}]\n".format(
                    job_info.node,
                    format_lohi(job_info.job_id_lo, job_info.job_id_hi, id_as_parts=id_as_parts),
                    ts_to_time_str(job_info.abs_start_time),
                    ts_to_time_str(job_info.abs_end_time)
                ))

