"""An utility for plotting job statistics"""
from yt.common import YtError, flatten
try:
    from yt.common import uuid_to_parts, parts_to_uuid
except ImportError:
    from yt.common import guid_to_parts as uuid_to_parts, parts_to_guid as parts_to_uuid
from yt.packages.six import iteritems, itervalues
from yt.packages.six.moves import xrange
from yt.wrapper import YtClient
import yt.wrapper as yt

import numpy
from plotly import __version__
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objs as go

from datetime import datetime
from collections import defaultdict, Iterable
from itertools import groupby
from functools import total_ordering
import numbers


def _format_lo_hi(id_lo, id_hi, id_as_parts=False):
    if id_as_parts:
        return "lo = {:20d}, hi = {:20d}".format(id_lo, id_hi)
    else:
        return "{:32}".format(parts_to_uuid(id_hi, id_lo))


def _add_line_breaks_to_uuid(uuid, job_count):
    if job_count <= 3:
        return uuid
    elif job_count <= 6:
        uuid_parts = uuid.split("-")
        return "{}-<br>{}".format("-".join(uuid_parts[:2]), "-".join(uuid_parts[2:]))
    return uuid.replace("-", "-<br>")


def _ts_to_time_str(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def _get_operation_info(op_id, client):
    """Get job statistics from the archive"""
    hi_id, lo_id = uuid_to_parts(op_id)
    start_job_id_hi, start_job_id_lo = 0, 0
    chunk_size = 5000
    operation_info = []
    while True:
        chunk = list(client.select_rows(
            """
            type, state, transient_state, start_time, finish_time, address,
            job_id_hi, job_id_lo, events, statistics from [//sys/operations_archive/jobs]
            where operation_id_lo = {}u and operation_id_hi = {}u and 
            (job_id_hi, job_id_lo) > ({}, {}) limit {}
            """.format(lo_id, hi_id, start_job_id_hi, start_job_id_lo, chunk_size)
        ))
        if not chunk:
            break
        start_job_id_hi, start_job_id_lo = chunk[-1]["job_id_hi"], chunk[-1]["job_id_lo"]
        operation_info += chunk
    return operation_info

def _get_event_time(phase, events):
    """Get starting time of some phase for particular job"""
    time_str = next(
        event["time"] for event in events if "phase" in event and event["phase"] == phase
    )
    return yt.common.date_string_to_timestamp(time_str)


def _get_statistic_from_output_tables(statistic, output):
    """Get particular statistic from all output tables as a list"""
    return sum(table[statistic] for table in itervalues(output))
            

def _nested_dict_find(data, path):
    cur_data = data
    path_segments = path.split("/")
    for segment in path_segments:
        if not isinstance(cur_data, Iterable) or segment not in cur_data:
            return 0
        cur_data = cur_data[segment]
    return cur_data


def _aggregate(jobset, func, statistic):
    return func([_get_statistics(job_info, statistic) for job_info in jobset])


def _get_statistics(job_info, statistic):
    if statistic.startswith("statistics/"):
        statistic = statistic.replace("statistics", "$", 1)
    if statistic.startswith("$/"):
        statistic_id = JobInfo.statistic_ids.get(statistic, -1)
        return job_info.statistics.get(statistic_id, None)
    else:
        return getattr(job_info, statistic, None)


def _data_is_correct(jobsets, statistic=None):
    for index, jobset in enumerate(jobsets):
        if not jobset:
            print("Job set number {} is empty!".format(index + 1))
            return False
        if statistic and not _aggregate(
            jobset, 
            lambda data: all(x is not None for x in data),
            statistic,
        ):
            print("Statistic '{}' is not known!".format(statistic))
            return False
        if statistic and not _aggregate(
            jobset,
            lambda data: all(isinstance(x, numbers.Number) for x in data),
            statistic,
        ):
            print("Statistic '{}' is not numeric!".format(statistic))
            return False
    return True


def _prepare_plot(title="", xlabel="", ylabel="", xaxis=True, yaxis=True, hovermode="x", bargap=0.1):
    """Set plot properties"""
    init_notebook_mode(connected=True)
    layout = dict(
        title=title,
        xaxis=dict(title=xlabel, visible=xaxis),
        yaxis=dict(title=ylabel, visible=yaxis),
        barmode="group",
        bargap=bargap,
        hovermode=hovermode,
    )
    return layout


def _get_hline(x0=0, x1=0, y=0, color="green", name="", showlegend=True, visible=True):
    """For Gantt charts"""
    return go.Scatter(
        x=[x0, x1],
        y=[y, y],
        mode="lines",
        line=dict(color=color),
        showlegend=showlegend,
        name=name,
        visible=visible,
        hoverinfo="name",
        hoverlabel=dict(
            namelength=-1,
        ),
    )


@total_ordering
class JobInfo(object):
    """Object for job representation"""
    statistic_ids = {}
    
    def __init__(self, job_info, operation_start=0):
        self.statistics = {}
        self._flatten_statistics_tree(job_info["statistics"], "$")
        
        self.operation_start = float(operation_start)
        self.start_time = float(_get_event_time("created", job_info["events"]) - operation_start)
        self.start_running = float(_get_event_time("running", job_info["events"]) - operation_start)
        self.end_time = float(_get_event_time("finished", job_info["events"]) - operation_start)
        self.total_time = self.end_time - self.start_time
        self.exec_time = self.end_time - self.start_running
        self.prepare_time = self.start_running - self.start_time
        
        self.state = job_info["state"] or job_info["transient_state"]
        self.type = job_info["type"]
        self.job_id_hi = job_info["job_id_hi"]
        self.job_id_lo = job_info["job_id_lo"]
        self.job_id = parts_to_uuid(self.job_id_hi, self.job_id_lo)
        self.node = job_info["address"].split(".")[0] if "address" in job_info else ""
        
        self.input_compressed_data_size = job_info["statistics"]["data"]["input"]["compressed_data_size"]
        self.input_uncompressed_data_size = job_info["statistics"]["data"]["input"]["uncompressed_data_size"]
        self.input_data_weight = job_info["statistics"]["data"]["input"]["data_weight"]
        self.input_row_count = job_info["statistics"]["data"]["input"]["row_count"]
        self.output_compressed_data_size = _get_statistic_from_output_tables(
            "compressed_data_size", job_info["statistics"]["data"]["output"]
        )
        self.output_uncompressed_data_size = _get_statistic_from_output_tables(
            "uncompressed_data_size", job_info["statistics"]["data"]["output"]
        )
        self.output_data_weight = _get_statistic_from_output_tables(
            "data_weight", job_info["statistics"]["data"]["output"]
        )
        self.output_row_count = _get_statistic_from_output_tables(
            "row_count", job_info["statistics"]["data"]["output"]
        )
        
        self.user_job_cpu = float(
            _nested_dict_find(job_info["statistics"], "user_job/cpu/sys") + 
            _nested_dict_find(job_info["statistics"], "user_job/cpu/user")
        ) / 1000
        self.job_proxy_cpu = float(
            _nested_dict_find(job_info["statistics"], "job_proxy/cpu/sys") + 
            _nested_dict_find(job_info["statistics"], "job_proxy/cpu/user")
        ) / 1000
        self.codec_decode = float(sum(
            list(itervalues(job_info["statistics"]["codec"]["cpu"]["decode"]))
        )) / 1000
        self.codec_encode = float(sum(flatten(
            list(itervalues(table)) for table in itervalues(job_info["statistics"]["codec"]["cpu"]["encode"])
        ))) / 1000
        self.input_idle_time = float(_nested_dict_find(
            job_info["statistics"], "user_job/pipes/input/idle_time"
        )) / 1000
        self.input_busy_time = float(_nested_dict_find(
            job_info["statistics"], "user_job/pipes/input/busy_time"
        )) / 1000
    
    def _insert_statistic(self, statistic, value):
        if statistic not in JobInfo.statistic_ids:
            JobInfo.statistic_ids[statistic] = len(JobInfo.statistic_ids)
        self.statistics[JobInfo.statistic_ids[statistic]] = value
    
    def _flatten_statistics_tree(self, tree, path):
        if not isinstance(tree, Iterable):
            self._insert_statistic(path, tree)
            return
        for key, branch in iteritems(tree):
            if isinstance(branch, Iterable) and "sum" in branch:
                self._insert_statistic("{}/{}".format(path, key), branch["sum"])
                tree[key] = branch["sum"]
            else:
                self._flatten_statistics_tree(branch, "{}/{}".format(path, key))
    
    def __str__(self):
        return "{}: {} [{} - {}]".format(
            self.node,
            _format_lo_hi(self.job_id_lo, self.job_id_hi),
            _ts_to_time_str(self.operation_start + self.start_time),
            _ts_to_time_str(self.operation_start + self.end_time)
        )
    
    def __eq__(self, other):
        return self.x == other.x and self.y == other.y
    
    def __lt__(self, other):
        return (self.type, self.start_time) < (other.type, other.start_time)


def get_jobs(op_id, cluster_name):
    """
    Get all jobs of operation as a list of JobInfo objects by operation id and a name of the cluster,
    e.g. 'jobs = get_jobs("7bd267f5-b8587ac7-3f403e8-2a46f9ce", "freud")'
    """
    client = yt.client.Yt(proxy = cluster_name)
    operation_info = _get_operation_info(op_id, client)
    if not operation_info:
        print("There is no operation {} in the operation archive!".format(op_id))
        return
    jobset = []
    
    operation_start = min(_get_event_time("created", job_info["events"]) for job_info in operation_info)
    for job_info in operation_info:
        state = job_info["state"] or job_info["transient_state"]
        if state in ["completed", "failed"]:
            jobset.append(JobInfo(job_info, operation_start))
    jobset.sort()
    return jobset


def get_raw_time_gantt_data(jobset):
    """
    Get data which are used in gantt chart: times of job events, job_id and state.
    Jobs are separated by their type.
    """
    if not _data_is_correct([jobset]):
        return None
    data = []
    for job_type, jobs_info in groupby(sorted(jobset), key=lambda x: x.type):
        job_events = []
        for job_info in jobs_info:
            job_events.append(dict(
                start_time=job_info.start_time,
                start_running=job_info.start_running,
                end_time=job_info.end_time,
                job_id=job_info.job_id,
                state=job_info.state,
            ))
        data.append(dict(
            job_type=job_type,
            jobs_info=job_events,
        ))
    return data


def draw_time_gantt(jobset):
    """Draw gantt chart illustrating preparation and execution periods for every job"""
    data = get_raw_time_gantt_data(jobset)
    if not data:
        return
    for jobs_per_type in data:
        lines = []
        # Creating the legend
        start = jobs_per_type["jobs_info"][0]["start_time"]
        lines.append(_get_hline(start, start, color="red", name="Failed jobs"))
        lines.append(_get_hline(start, start, color="green", name="Completed jobs"))
        lines.append(_get_hline(start, start, color="yellow", name="Preparation"))
        
        for y, job_info in enumerate(jobs_per_type["jobs_info"]):
            lines.append(_get_hline(
                job_info["start_time"], job_info["start_running"], y,
                color="yellow",
                name="Job {}".format(job_info["job_id"]),
                showlegend=False,
            ))
            lines.append(_get_hline(
                job_info["start_running"], job_info["end_time"], y,
                color=("green" if job_info["state"] == "completed" else "red"),
                name="Job {}".format(job_info["job_id"]),
                showlegend=False,
            ))
        iplot(dict(
            data=lines,
            layout=_prepare_plot(
                title="{} jobs".format(jobs_per_type["job_type"]),
                xlabel="time", ylabel="jobs", yaxis=False, hovermode="closest",
            ),
        ))
        

def get_raw_hist_data(jobset, statistic="total_time"):
    """
    Get data which are used in hist: values of selected statistic separated by job set.
    """
    data = get_raw_comparative_hist_data([jobset], statistic)
    if data:
        return data[0]
    return None
        
        
def get_raw_comparative_hist_data(jobsets, statistic="total_time"):
    """
    Get raw hist data for a list of job sets.
    """
    if not _data_is_correct(jobsets, statistic):
        return None
    data = []
    for jobset in jobsets:
        jobset_info = []
        for job_type, jobs_info in groupby(sorted(jobset), key=lambda x: x.type):
            jobset_info.append(dict(
                job_type=job_type,
                values=[_get_statistics(job_info, statistic) for job_info in jobs_info],
            ))
        data.append(jobset_info)
    return data
        
        
def draw_hist(jobset, statistic="total_time"):
    """
    Draw histogram for selected data type.
    Data type can be a path in the statistics tree, starting with word statistics or $
    e.g. statistics/data/input/data_weight or $/data/input/data_weight.
    """
    draw_comparative_hist([jobset], statistic)


def draw_comparative_hist(jobsets, statistic="total_time"):
    """Draw histograms for several jobsets on one plot"""
    data = get_raw_comparative_hist_data(jobsets, statistic)
    if not data:
        return
    # Find data boundaries for calculation of bins edges:
    min_val = min(_aggregate(jobset, min, statistic) for jobset in jobsets)
    max_val = max(_aggregate(jobset, max, statistic) for jobset in jobsets)
    bin_count = min(max_val - min_val + 1, 50)
    bin_size = float(max_val - min_val + 1) / bin_count
    traces = []
    for index, jobset in enumerate(data):
        for jobs_per_type in jobset:
            traces.append(go.Histogram(
                x=jobs_per_type["values"],
                name="{} jobs in set{}".format(jobs_per_type["job_type"], index + 1),
                autobinx=False,
                xbins=dict(
                    start=min_val,
                    end=max_val + bin_size,
                    size=bin_size,
                ),
                hoverinfo="name+y",
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
    iplot(dict(
        data=traces,
        layout=_prepare_plot(title="{} histogram".format(statistic), xlabel=statistic, ylabel="job count"),
    ))

    
def get_raw_time_plot_data(jobset):
    """
    Get data which are used in time plot:
    time points as "x_values" and job counts in these points as "y_values", separated by job type.
    """
    data = get_raw_comparative_time_plot_data([jobset])
    if data:
        return data[0]
    return None
    
    
def get_raw_comparative_time_plot_data(jobsets):
    """
    Get raw time plot data for a list of job sets.
    """
    if not _data_is_correct(jobsets):
        return None
    step_count = 200
    data = []
    for jobset in jobsets:
        jobset_info = []
        for job_type, jobs_info in groupby(sorted(jobset), key=lambda x: x.type):
            min_time = min(job_info.start_time for job_info in jobset if job_info.type == job_type)
            max_time = max(job_info.end_time for job_info in jobset if job_info.type == job_type)
            time_points = numpy.linspace(
                max(0, min_time - 1),
                max_time + 1,
                min(step_count, int(max_time - min_time + 1))
            )
            
            job_count = [0] * len(time_points)
            for job_info in jobs_info:
                for num, point in enumerate(time_points):
                    job_count[num] += job_info.start_time <= point <= job_info.end_time
            jobset_info.append(dict(
                job_type=job_type,
                x_values=list(time_points),
                y_values=job_count,
            ))
        data.append(jobset_info)
    return data
            
    
def draw_time_plot(jobset):
    """Draw line graph illustrating number of running jobs during the operation"""
    draw_comparative_time_plot([jobset])


def draw_comparative_time_plot(jobsets):
    """Draw line graphs for several jobsets on one plot"""
    data = get_raw_comparative_time_plot_data(jobsets)
    if not data:
        return
    traces = []
    for index, jobset in enumerate(data):
        for jobs_per_type in jobset:
            job_count = jobs_per_type["y_values"]
            job_count_without_zeros = job_count
            for num in xrange(1, len(job_count) - 1):
                if job_count[num - 1] == 0 and job_count[num] == 0 and job_count[num + 1] == 0:
                    job_count_without_zeros[j] = None

            traces.append(go.Scatter(
                x=jobs_per_type["x_values"],
                y=job_count_without_zeros,
                mode="lines",
                fill="tozeroy",
                name="{} jobs in set{}".format(jobs_per_type["job_type"], index + 1),
                connectgaps=False,
                hoverinfo="x+y",
            ))
    iplot(dict(
        data=traces,
        layout=_prepare_plot(xlabel="time", ylabel="job count"),
    ))
    

def get_raw_scatter_plot_data(jobset, x_statistic="input_data_weight", y_statistic="total_time"):
    """
    Get data which are used in scatter plot:
    values of selected x_statistic and y_statistic as "x_values" and "y_values" separated by job type.
    """
    data = get_raw_comparative_scatter_plot_data([jobset], x_statistic, y_statistic)
    if data:
        return data[0]
    return None
    
    
def get_raw_comparative_scatter_plot_data(jobsets, x_statistic="input_data_weight", y_statistic="total_time"):
    """
    Get raw scatter plot data for a list of job sets.
    """
    if (not _data_is_correct(jobsets, x_statistic) or
        not _data_is_correct(jobsets, y_statistic)):
        return None
    data = []
    for jobset in jobsets:
        jobset_info = []
        for job_type, jobs_info in groupby(sorted(jobset), key=lambda x: x.type):
            xvalues, yvalues = zip(*[(
                _get_statistics(job_info, x_statistic),
                _get_statistics(job_info, y_statistic)
            ) for job_info in jobs_info])
            jobset_info.append(dict(
                job_type=job_type,
                x_values=xvalues,
                y_values=yvalues,
            ))
        data.append(jobset_info)
    return data


def draw_scatter_plot(jobset, x_statistic="input_data_weight", y_statistic="total_time"):
    """
    Draw scatter plot for two statistics.
    """
    draw_comparative_scatter_plot([jobset], x_statistic, y_statistic)


def draw_comparative_scatter_plot(jobsets, x_statistic="input_data_weight", y_statistic="total_time"):
    """Draw scatter charts for several jobsets on one plot"""
    data = get_raw_comparative_scatter_plot_data(jobsets, x_statistic, y_statistic)
    if not data:
        return
    traces = []
    for index, jobset in enumerate(data):
        for jobs_per_type in jobset:
            traces.append(go.Scatter(
                x=jobs_per_type["x_values"],
                y=jobs_per_type["y_values"],
                mode="markers",
                name="{} jobs in set{}".format(jobs_per_type["job_type"], index + 1),
                marker=dict(
                    size=10,
                    opacity=0.9,
                    line=dict(width=2),
                ),
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
    iplot(dict(
        data=traces,
        layout=_prepare_plot(xlabel=x_statistic, ylabel=y_statistic, hovermode="closest"),
    ))


def get_raw_time_statistics_data(jobset):
    """
    Get data which are used in time statistics bar chart:
    values of statistics as "statistics" and list of job ids.
    Jobs are separated by their type.
    """
    if not _data_is_correct([jobset]):
        return None
    statistics = [
        "exec_time", "user_job_cpu", "job_proxy_cpu",
        "codec_decode", "codec_encode", "input_idle_time", "input_busy_time",
    ]
    data = []
    for job_type, job_info_per_type in groupby(sorted(jobset), key=lambda x: x.type):
        jobs_info = list(job_info_per_type)
        job_ids = [
            _add_line_breaks_to_uuid(job_info.job_id, len(jobs_info))
            for job_info in jobs_info
        ]
        statistics_values = {}
        for statistic in statistics:
            values = [getattr(job_info, statistic) for job_info in jobs_info]
            statistics_values[statistic] = values
        data.append(dict(
            job_type=job_type,
            statistics=statistics_values,
            job_ids=job_ids,
        ))
    return data
        
    
def draw_time_statistics_bar_chart(jobset):
    """
    Draw duration of different job stages as a bar chart for every job
    """
    data = get_raw_time_statistics_data(jobset)
    if not data:
        return
    statistics = [
        "exec_time", "user_job_cpu", "job_proxy_cpu",
        "codec_decode", "codec_encode", "input_idle_time", "input_busy_time",
    ]
    for jobs_per_type in data:
        traces = []
        for statistic in statistics:
            traces.append(go.Bar(
                x=jobs_per_type["job_ids"],
                y=jobs_per_type["statistics"][statistic],
                name=statistic,
                hoverinfo="name+y",
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
        iplot(dict(
            data=traces,
            layout=_prepare_plot(
                title="{} jobs".format(jobs_per_type["job_type"]), xlabel="jobs", ylabel="statistics values",
                hovermode="closest", bargap=0.3,
            ),
        ))
        

def print_jobset(jobset, sort_by="start_time", reverse=False, additional_fields=()):
    """
    Print node id, job id, running time and maybe some additional fields for every job
    """
    for job_type, jobs_info in groupby(
        sorted(jobset, key=lambda x: (x.type, _get_statistics(x, sort_by)), reverse=reverse),
        key=lambda x: x.type,
    ):
        print("{} jobs:".format(job_type))
        for job_info in jobs_info:
            print("\t{}".format(job_info))
            
            if additional_fields:
                print("\t{}:".format("additional_fields"))
                for field in additional_fields:
                    print("\t\t{}: {}".format(field, _get_statistics(job_info, field)))

