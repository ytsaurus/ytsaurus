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
from plotly import __version__
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objs as go
import numbers


"""Object for job representation."""
JobInfo = namedtuple(
    "JobInfo", [
        "abs_start_time", "abs_end_time", "abs_start_running", 
        "rel_start_time", "rel_end_time", "rel_start_running",
        "length", "state", "job_id_hi", "job_id_lo", "node",
        "input_compressed_data_size", "input_uncompressed_data_size", "input_data_weight", "input_row_count",
        "output_compressed_data_size", "output_uncompressed_data_size", "output_data_weight", "output_row_count",
        "statistics",
    ]
)


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
    return list(output[table_num][data_type]["sum"] for table_num in output.keys())


def get_jobs(op_id, cluster_name):
    """
    Get set of all jobs in operation as a JobSet object by operation id and a name of the cluster.
    """
    res = JobSet()
    client = yt.client.Yt(proxy = cluster_name)
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
                    output_row_count=output_row_count,
                    statistics=job_info["statistics"],
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
        return func([self.get_statistics(job_info, data_type, table_num)
                    for job_info_per_type in self.data.itervalues() 
                    for job_info in job_info_per_type])
    
    def filter_jobs(
        self, job_types=None,
        rel_starts_after=0, rel_starts_before=float("inf"),
        rel_ends_after=0, rel_ends_before=float("inf"),
        abs_starts_after=0, abs_starts_before=float("inf"),
        abs_ends_after=0, abs_ends_before=float("inf"),
        states=("completed", "failed"),
        filter_func=lambda x: True,
    ):
        """Get new set of jobs, filtered by type, state and time limits."""
        if job_types is None:
            job_types = tuple(self.data.keys())
        new_jobs = JobSet()
        for job_type, jobs_info in self.data.iteritems():
            if not job_type in job_types:
                continue
            for job_info in jobs_info:
                if (filter_func(job_info) and
                    rel_starts_after <= job_info.rel_start_time <= rel_starts_before and 
                    rel_ends_after <= job_info.rel_end_time <= rel_ends_before and
                    abs_starts_after <= job_info.abs_start_time <= abs_starts_before and 
                    abs_ends_after <= job_info.abs_end_time <= abs_ends_before and
                    job_info.state in states):
                    new_jobs.data[job_type].append(job_info)
        return new_jobs
     
    def prepare_plot(self, title="", xlabel="", ylabel=""):
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
        #return dict(data = data, layout = layout)
    
    def data_is_correct(self, data_type=None, table_num=0, other_jobset=None):
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        for jobset in jobsets:
            if not jobset.data:
                print("One of JobSets is empty!")
                return False
            if data_type and not jobset.aggregate(data_type, all, table_num):
                print("There is no such statistics in one of JobSets!")
                return False
            if data_type and not jobset.aggregate(
                data_type,
                lambda data: all(isinstance(x, numbers.Number) for x in data),
                table_num,
            ):
                print("Selected statistics is not numeric!")
                return False
        return True
        
    def get_hline(self, x0=0, x1=0, y=0, color="green", name="", showlegend=False, visible=True):
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
    
    def get_statistics(self, job_info, data_type, table_num=0):
        path_segments = data_type.split("/")
        if (path_segments[0] != "statistics"):
            if not hasattr(job_info, data_type):
                return None
            return covert_to_list(getattr(job_info, data_type))[table_num]
        data = getattr(job_info, "statistics")
        for segment in path_segments[1:]:
            if not segment in data:
                return None
            data = data[segment]
        return data
            
    def draw_time_gantt(self):
        """Draw gantt chart illustrating preparation and execution periods for every job."""
        if not self.data_is_correct():
            return
        for job_type, jobs_info in self.data.iteritems():
            lines = []
            for y, job_info in enumerate(jobs_info):
                lines.append(self.get_hline(job_info.rel_start_time, job_info.rel_start_running, y, "yellow"))
                lines.append(self.get_hline(
                    job_info.rel_start_running, job_info.rel_end_time, y,
                    color=("green" if job_info.state == "completed" else "red")),
                )            
            #Creating the legend:
            lines.append(self.get_hline(color="red", name="Failed jobs", showlegend=True, visible="legendonly"))
            lines.append(self.get_hline(color="green", name="Completed jobs", showlegend=True, visible="legendonly"))
            lines.append(self.get_hline(color="yellow", name="Preparation", showlegend=True, visible="legendonly"))
            
            iplot(dict(
                data = lines,
                layout = self.prepare_plot(title="{} jobs".format(job_type), xlabel="time", ylabel="jobs"),
            ))
    
    def draw_hist(self, data_type="length", table_num=0, other_jobset=None):
        """
        Draw histogram for selected data type.
        Data type can be a path in the statistics tree,
        starting with word statistics e.g. statistics/data/input/data_weight/sum.
        If data type refers to output, number of output table should be provided (default is 0).
        If other_jobset argument is passed, comparative chart for two operations will be drawn.
        """ 
        if not self.data_is_correct(data_type, other_jobset=other_jobset):
            return
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        
        # Find data boundaries for calculation of bins edges:
        min_val = min(jobset.aggregate(data_type, min, table_num) for jobset in jobsets)
        max_val = max(jobset.aggregate(data_type, max, table_num) for jobset in jobsets)
        bin_count = min(max_val - min_val + 1, 50)
        traces = []
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.iteritems():
                values = [self.get_statistics(job_info, data_type, table_num) for job_info in jobs_info]
                traces.append(go.Histogram(
                    nbinsx=int(bin_count),
                    x=values,
                    name="{} jobs in set{}".format(job_type, i + 1),
                ))
        iplot(dict(
            data = traces,
            layout = self.prepare_plot(title="{} histogram".format(data_type), xlabel=data_type, ylabel="jobs count"),
        ))
    
    def draw_time_plot(self, step_count=200, other_jobset=None):
        """
        Draw line graph illustrating number of running jobs during the operation.
        If other_jobset argument is passed, comparative chart for two operations will be drawn.
        """
        if not self.data_is_correct(other_jobset=other_jobset):
            return
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        traces = []
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.iteritems():
                min_time = min(job_info.rel_start_time for job_info in jobs_info)
                max_time = max(job_info.rel_end_time for job_info in jobs_info)
                time_points = numpy.linspace(max(0, min_time - 1), max_time + 1, min(step_count, max_time - min_time + 1))
                
                job_count = []
                for point in time_points:
                    job_count.append(sum(job_info.rel_start_time <= point <= job_info.rel_end_time
                                         for job_info in jobs_info))
                    
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
            layout = self.prepare_plot(xlabel="time", ylabel="jobs count"),
        ))
        
    def draw_scatter_plot(
        self,
        x_data_type="input_data_weight", x_table_num=0,
        y_data_type="length", y_table_num=0,
        other_jobset=None,
    ):
        if (not self.data_is_correct(x_data_type, other_jobset=other_jobset) and
            not self.data_is_correct(y_data_type, other_jobset=other_jobset)):
            return
        jobsets = ([self] if other_jobset is None else [self, other_jobset])
        traces = []
        for i, jobset in enumerate(jobsets):
            for job_type, jobs_info in jobset.data.iteritems():
                xvalues = [self.get_statistics(job_info, x_data_type, x_table_num) for job_info in jobs_info]
                yvalues = [self.get_statistics(job_info, y_data_type, y_table_num) for job_info in jobs_info]
                traces.append(go.Scatter(
                    x=xvalues,
                    y=yvalues,
                    mode="markers",
                    name="{} jobs in set{}".format(job_type, i + 1),
                ))
        iplot(dict(
            data = traces,
            layout = self.prepare_plot(xlabel=x_data_type, ylabel=y_data_type),
        ))
        
        
    def print_text_data(self, id_as_parts=False):
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
