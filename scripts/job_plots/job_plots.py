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
from collections import Iterable
from plotly import __version__
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objs as go
import numbers
from itertools import groupby


def format_lohi(id_lo, id_hi, id_as_parts=False):
    if id_as_parts:
        return "lo = {:20d}, hi = {:20d}".format(id_lo, id_hi)
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
    return list(output[table_num][data_type] for table_num in output.keys())


def trim_statistics_tree(tree):
    """Remove sum/min/max/count fields on the lower level of statistics tree"""
    if not hasattr(tree, "iteritems"):
        return
    for key, branch in tree.iteritems():
        if isinstance(branch, Iterable) and "sum" in branch:
            tree[key] = branch["sum"]
        else:
            trim_statistics_tree(branch)


class JobInfo(object):
    """Object for job representation."""
    def __init__(self, job_info, operation_start=0):
        trim_statistics_tree(job_info["statistics"])
        self.statistics = job_info["statistics"]
        
        self.abs_start_time = get_event_time("created", job_info["events"])
        self.abs_start_running = get_event_time("running", job_info["events"])
        self.abs_end_time = get_event_time("finished", job_info["events"])
        self.rel_start_time = self.abs_start_time - operation_start
        self.rel_start_running = self.abs_start_running - operation_start
        self.rel_end_time = self.abs_end_time - operation_start
        self.length = self.abs_end_time - self.abs_start_time
        
        self.state = job_info["state"] or job_info["transient_state"]
        self.type = job_info["type"]
        self.job_id_hi = job_info["job_id_hi"]
        self.job_id_lo = job_info["job_id_lo"]
        self.node = job_info["address"].split(".")[0] if "address" in job_info else ""
        
        self.input_compressed_data_size = job_info["statistics"]["data"]["input"]["compressed_data_size"]
        self.input_uncompressed_data_size = job_info["statistics"]["data"]["input"]["uncompressed_data_size"]
        self.input_data_weight = job_info["statistics"]["data"]["input"]["data_weight"]
        self.input_row_count = job_info["statistics"]["data"]["input"]["row_count"]
        self.output_compressed_data_size = get_data_from_output_tables(
            "compressed_data_size", job_info["statistics"]["data"]["output"]
        )
        self.output_uncompressed_data_size = get_data_from_output_tables(
            "uncompressed_data_size", job_info["statistics"]["data"]["output"]
        )
        self.output_data_weight = get_data_from_output_tables(
            "data_weight", job_info["statistics"]["data"]["output"]
        )
        self.output_row_count = get_data_from_output_tables(
            "row_count", job_info["statistics"]["data"]["output"]
        )
    
    def __str__(self):
        return "{}: {} [{} - {}]\n".format(
            self.node,
            format_lohi(self.job_id_lo, self.job_id_hi),
            ts_to_time_str(self.abs_start_time),
            ts_to_time_str(self.abs_end_time)
        )


def get_jobs(op_id, cluster_name):
    """
    Get set of all jobs in operation as a list of JobInfo objects by operation id and a name of the cluster.
    """
    client = yt.client.Yt(proxy = cluster_name)
    operation_info = get_operation_info(op_id, client)
    jobset = []
    
    # For calculation of relative time i.e. time from the operation start:
    min_time = min(get_event_time("created", job_info["events"]) for job_info in operation_info)

    for job_info in operation_info:
        state = job_info["state"] or job_info["transient_state"]
        if state in ["completed", "failed"]:
            jobset.append(JobInfo(job_info, min_time))
    jobset.sort(key=lambda x: (x.type, x.abs_start_time))
    return jobset


def filter_jobs(
    jobset, job_types=None,
    rel_starts_after=0, rel_starts_before=float("inf"),
    rel_ends_after=0, rel_ends_before=float("inf"),
    abs_starts_after=0, abs_starts_before=float("inf"),
    abs_ends_after=0, abs_ends_before=float("inf"),
    states=("completed", "failed"),
    filter_func=lambda x: True,
):
    """Get new set of jobs, filtered by type, state and time limits."""
    return list(filter(
        lambda job_info: (
            filter_func(job_info) and
            job_info.state in states and
            (not job_types or job_info.type in job_types) and
            rel_starts_after <= job_info.rel_start_time <= rel_starts_before and 
            rel_ends_after <= job_info.rel_end_time <= rel_ends_before and
            abs_starts_after <= job_info.abs_start_time <= abs_starts_before and 
            abs_ends_after <= job_info.abs_end_time <= abs_ends_before
        ),
        jobset,
    ))


def aggregate(jobset, func, data_type, table_num=0):
    return func([get_statistics(job_info, data_type, table_num) for job_info in jobset])


def get_statistics(job_info, data_type, table_num=0):
    path_segments = data_type.split("/")
    if (path_segments[0] != "statistics"):
        if not hasattr(job_info, data_type):
            return None
        return covert_to_list(getattr(job_info, data_type))[table_num]
    data = getattr(job_info, "statistics")
    for segment in path_segments[1:]:
        if not isinstance(data, Iterable) or not segment in data:
            return None
        data = data[segment]
    return data


def data_is_correct(jobsets, data_type=None, table_num=0):
    for jobset in jobsets:
        if not jobset:
            print("One of JobSets is empty!")
            return False
        if data_type and not aggregate(jobset, all, data_type, table_num):
            print("There is no such statistics in one of JobSets!")
            return False
        if data_type and not aggregate(
            jobset,
            lambda data: all(isinstance(x, numbers.Number) for x in data),
            data_type,
            table_num,
        ):
            print("Selected statistics is not numeric!")
            return False
    return True


def prepare_plot(title="", xlabel="", ylabel=""):
    """Set plot properties."""
    init_notebook_mode(connected=True)
    layout = dict(
        title = title,
        xaxis = dict(title = xlabel),
        yaxis = dict(title = ylabel),
        barmode = "group",
        bargap = 0.1,
    )
    return layout


def get_hline(x0=0, x1=0, y=0, color="green", name="", showlegend=False, visible=True):
    """For Gantt charts"""
    return go.Scatter(
        x=[x0, x1],
        y=[y, y],
        mode="lines",
        line=dict(color = color),
        hoverinfo="none",
        showlegend=showlegend,
        name=name,
        visible=visible,
    )


def draw_time_gantt(jobset):
    """Draw gantt chart illustrating preparation and execution periods for every job."""
    if not data_is_correct([jobset]):
        return
    for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
        lines = []
        for y, job_info in enumerate(jobs_info):
            lines.append(get_hline(job_info.rel_start_time, job_info.rel_start_running, y, "yellow"))
            lines.append(get_hline(
                job_info.rel_start_running, job_info.rel_end_time, y,
                color=("green" if job_info.state == "completed" else "red")),
            )
        #Creating the legend:
        lines.append(get_hline(color="red", name="Failed jobs", showlegend=True, visible="legendonly"))
        lines.append(get_hline(color="green", name="Completed jobs", showlegend=True, visible="legendonly"))
        lines.append(get_hline(color="yellow", name="Preparation", showlegend=True, visible="legendonly"))

        iplot(dict(
            data = lines,
            layout = prepare_plot(title="{} jobs".format(job_type), xlabel="time", ylabel="jobs"),
        ))


def draw_hist(jobset, data_type="length", table_num=0):
    """
    Draw histogram for selected data type.
    Data type can be a path in the statistics tree,
    starting with word statistics e.g. statistics/data/input/data_weight.
    If data type refers to output, number of output table should be provided (default is 0).
    """
    draw_comparative_hist([jobset], data_type, table_num)


def draw_comparative_hist(jobsets, data_type="length", table_num=0):
    """Draw histograms for several jobsets on one plot"""
    if not data_is_correct(jobsets, data_type, table_num):
        return        
    # Find data boundaries for calculation of bins edges:
    min_val = min(aggregate(jobset, min, data_type, table_num) for jobset in jobsets)
    max_val = max(aggregate(jobset, max, data_type, table_num) for jobset in jobsets)
    bin_count = min(max_val - min_val + 1, 50)
    bin_size = float(max_val - min_val + 1) / bin_count
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            values = [get_statistics(job_info, data_type, table_num) for job_info in jobs_info]
            traces.append(go.Histogram(
                x=values,
                name="{} jobs in set{}".format(job_type, i + 1),
                autobinx=False,
                xbins=dict(
                    start = min_val,
                    end = max_val + bin_size,
                    size = bin_size,
                )
            ))
    iplot(dict(
        data = traces,
        layout = prepare_plot(title="{} histogram".format(data_type), xlabel=data_type, ylabel="jobs count"),
    ))


def draw_time_plot(jobset):
    """Draw line graph illustrating number of running jobs during the operation."""
    draw_comparative_time_plot([jobset])


def draw_comparative_time_plot(jobsets):
    """Draw line graphs for several jobsets on one plot."""
    if not data_is_correct(jobsets):
        return
    step_count=200
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            min_time = min(job_info.rel_start_time for job_info in jobset if job_info.type == job_type)
            max_time = max(job_info.rel_end_time for job_info in jobset if job_info.type == job_type)
            time_points = numpy.linspace(max(0, min_time - 1), max_time + 1, min(step_count, max_time - min_time + 1))

            job_count = [0] * len(time_points)
            for job_info in jobs_info:
                for j, point in enumerate(time_points):
                    job_count[j] += job_info.rel_start_time <= point <= job_info.rel_end_time
                    
            job_count_without_zeros = job_count
            for j in range(1, len(job_count) - 1):
                if job_count[j - 1] == 0 and job_count[j] == 0 and job_count[j + 1] == 0:
                    job_count_without_zeros[j] = None

            traces.append(go.Scatter(
                x=time_points,
                y=job_count_without_zeros,
                mode="lines",
                fill="tozeroy",
                name="{} jobs in set{}".format(job_type, i + 1),
                connectgaps=False,
            ))
    iplot(dict(
        data = traces,
        layout = prepare_plot(xlabel="time", ylabel="jobs count"),
    ))


def draw_scatter_plot(
    jobset,
    x_data_type="input_data_weight", x_table_num=0,
    y_data_type="length", y_table_num=0,
):
    """
    Draw scatter plot for two statistics.
    If data type refers to output, number of output table should be provided (default is 0).
    """
    draw_comparative_scatter_plot([jobset], x_data_type, x_table_num, y_data_type, y_table_num)


def draw_comparative_scatter_plot(
    jobsets,
    x_data_type="input_data_weight", x_table_num=0,
    y_data_type="length", y_table_num=0,
    other_jobset=None,
):
    """Draw scatter charts for several jobsets on one plot."""
    if (not data_is_correct(jobsets, x_data_type, x_table_num) or
        not data_is_correct(jobsets, y_data_type, y_table_num)):
        return
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            xvalues, yvalues = zip(*[(
                get_statistics(job_info, x_data_type, x_table_num),
                get_statistics(job_info, y_data_type, y_table_num))
                for job_info in jobs_info])
            traces.append(go.Scatter(
                x=xvalues,
                y=yvalues,
                mode="markers",
                name="{} jobs in set{}".format(job_type, i + 1),
                marker = dict(
                    size = 10,
                    opacity = 0.9,
                    line = dict(width = 2),
                ),
            ))
    iplot(dict(
        data = traces,
        layout = prepare_plot(xlabel=x_data_type, ylabel=y_data_type),
    ))


def print_text_data(jobset):
    """
    Print node id, job id and running time for every job.
    """
    for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
        sys.stdout.write("{} jobs:\n".format(job_type))
        for job_info in jobs_info:
            sys.stdout.write("\t{}".format(job_info))

