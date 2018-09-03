"""An utility for plotting job statistics"""

from yt.common import guid_to_parts, parts_to_guid
from yt.wrapper import YtClient
import yt.wrapper as yt

import numpy
from plotly import __version__
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objs as go

from datetime import datetime
from collections import defaultdict, Iterable
from itertools import groupby
import numbers


def _format_lohi(id_lo, id_hi, id_as_parts=False):
    if id_as_parts:
        return "lo = {:20d}, hi = {:20d}".format(id_lo, id_hi)
    else:
        return "{:32}".format(parts_to_guid(id_hi, id_lo))


def _add_line_breaks_to_guid(guid, job_count):
    if job_count <= 3:
        return guid
    elif job_count <= 6:
        guid_parts = guid.split("-")
        return "{}-<br>{}".format("-".join(guid_parts[:2]), "-".join(guid_parts[2:]))
    return guid.replace("-", "-<br>")


def _ts_to_time_str(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def _convert_to_list(elem):
    """To treat simple type arguments and list arguments in the same way"""
    if isinstance(elem, list):
        return elem
    return [elem]


def _get_operation_info(op_id, client):
    """Get job statistics from the archive"""
    hi_id, lo_id = guid_to_parts(op_id)
    operation_info = client.select_rows(
        """type, state, transient_state, start_time, finish_time, address,
            job_id_hi, job_id_lo, events, statistics from [//sys/operations_archive/jobs]
            where operation_id_lo = {}u and operation_id_hi = {}u""".format(lo_id, hi_id)
    )
    return list(operation_info)


def _get_event_time(phase, events):
    """Get starting time of some phase for particular job"""
    time_str = next(
        event["time"] for event in events if "phase" in event and event["phase"] == phase
    )
    return yt.common.date_string_to_timestamp(time_str)


def _get_statistic_from_output_tables(statistic, output):
    """Get particular statistic from all output tables as a list"""
    return sum(table[statistic] for table in output.itervalues())


def _trim_statistics_tree(tree):
    """Remove sum/min/max/count fields on the lower level of statistics tree"""
    if not hasattr(tree, "iteritems"):
        return
    for key, branch in tree.iteritems():
        if isinstance(branch, Iterable) and "sum" in branch:
            tree[key] = branch["sum"]
        else:
            _trim_statistics_tree(branch)
            

def _nested_dict_find(data, path):
    cur_data = data
    path_segments = path.split("/")
    for segment in path_segments:
        if not isinstance(cur_data, Iterable) or not segment in cur_data:
            return 0
        cur_data = cur_data[segment]
    return cur_data


def _aggregate(jobset, func, statistic):
    return func([_get_statistics(job_info, statistic) for job_info in jobset])


def _get_statistics(job_info, statistic):
    path_segments = statistic.split("/")
    if (path_segments[0] != "statistics" and path_segments[0] != "$"):
        if not hasattr(job_info, statistic):
            return None
        return getattr(job_info, statistic)
    data = getattr(job_info, "statistics")
    for segment in path_segments[1:]:
        if not isinstance(data, Iterable) or not segment in data:
            return None
        data = data[segment]
    return data


def _data_is_correct(jobsets, statistic=None):
    for i, jobset in enumerate(jobsets):
        if not jobset:
            print("Job set number {} is empty!".format(i + 1))
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
        title = title,
        xaxis = dict(title = xlabel, visible = xaxis),
        yaxis = dict(title = ylabel, visible = yaxis),
        barmode = "group",
        bargap = bargap,
        hovermode = hovermode,
    )
    return layout


def _get_hline(x0=0, x1=0, y=0, color="green", name="", showlegend=False, visible=True):
    """For Gantt charts"""
    return go.Scatter(
        x=[x0, x1],
        y=[y, y],
        mode="lines",
        line=dict(color = color),
        showlegend=showlegend,
        name=name,
        visible=visible,
        hoverinfo="name",
        hoverlabel=dict(
            namelength=-1,
        ),
    )


class JobInfo(object):
    """Object for job representation"""
    def __init__(self, job_info, operation_start=0):
        _trim_statistics_tree(job_info["statistics"])
        self.statistics = job_info["statistics"]
        
        self.operation_start = operation_start
        self.start_time = _get_event_time("created", job_info["events"]) - operation_start
        self.start_running = _get_event_time("running", job_info["events"]) - operation_start
        self.end_time = _get_event_time("finished", job_info["events"]) - operation_start
        self.total_time = self.end_time - self.start_time
        self.exec_time = self.end_time - self.start_running
        self.prepare_time = self.start_running - self.start_time
        
        self.state = job_info["state"] or job_info["transient_state"]
        self.type = job_info["type"]
        self.job_id_hi = job_info["job_id_hi"]
        self.job_id_lo = job_info["job_id_lo"]
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
            alg for alg in job_info["statistics"]["codec"]["cpu"]["decode"].itervalues()
        )) / 1000
        self.codec_encode = float(sum(
            alg for table in job_info["statistics"]["codec"]["cpu"]["encode"].itervalues()
                for alg in table.itervalues() 
        )) / 1000
        self.input_idle_time = float(_nested_dict_find(
            job_info["statistics"], "user_job/pipes/input/idle_time"
        )) / 1000
        self.busy_time = float(_nested_dict_find(
            job_info["statistics"], "user_job/pipes/input/busy_time"
        )) / 1000
        
    
    def __str__(self):
        return "{}: {} [{} - {}]\n".format(
            self.node,
            _format_lohi(self.job_id_lo, self.job_id_hi),
            _ts_to_time_str(self.operation_start + self.start_time),
            _ts_to_time_str(self.operation_start + self.end_time)
        )


def get_jobs(op_id, cluster_name):
    """
    Get set of all jobs in operation as a list of JobInfo objects by operation id and a name of the cluster,
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
    jobset.sort(key=lambda x: (x.type, x.start_time))
    return jobset


def draw_time_gantt(jobset):
    """Draw gantt chart illustrating preparation and execution periods for every job"""
    if not _data_is_correct([jobset]):
        return
    for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
        lines = []
        for y, job_info in enumerate(jobs_info):
            lines.append(_get_hline(
                job_info.start_time, job_info.start_running, y,
                color="yellow",
                name="Job {}".format(_format_lohi(job_info.job_id_lo, job_info.job_id_hi)),
            ))
            lines.append(_get_hline(
                job_info.start_running, job_info.end_time, y,
                color=("green" if job_info.state == "completed" else "red"),
                name="Job {}".format(_format_lohi(job_info.job_id_lo, job_info.job_id_hi)),
            ))
        #Creating the legend:
        lines.append(_get_hline(color="red", name="Failed jobs", showlegend=True, visible="legendonly"))
        lines.append(_get_hline(color="green", name="Completed jobs", showlegend=True, visible="legendonly"))
        lines.append(_get_hline(color="yellow", name="Preparation", showlegend=True, visible="legendonly"))

        iplot(dict(
            data = lines,
            layout = _prepare_plot(
                title="{} jobs".format(job_type),
                xlabel="time", ylabel="jobs", yaxis=False, hovermode="closest",
            ),
        ))


def draw_hist(jobset, statistic="total_time"):
    """
    Draw histogram for selected data type.
    Data type can be a path in the statistics tree, starting with word statistics or $
    e.g. statistics/data/input/data_weight or $/data/input/data_weight.
    """
    draw_comparative_hist([jobset], statistic)


def draw_comparative_hist(jobsets, statistic="total_time"):
    """Draw histograms for several jobsets on one plot"""
    if not _data_is_correct(jobsets, statistic):
        return        
    # Find data boundaries for calculation of bins edges:
    min_val = min(_aggregate(jobset, min, statistic) for jobset in jobsets)
    max_val = max(_aggregate(jobset, max, statistic) for jobset in jobsets)
    bin_count = min(max_val - min_val + 1, 50)
    bin_size = float(max_val - min_val + 1) / bin_count
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            values = [_get_statistics(job_info, statistic) for job_info in jobs_info]
            traces.append(go.Histogram(
                x=values,
                name="{} jobs in set{}".format(job_type, i + 1),
                autobinx=False,
                xbins=dict(
                    start = min_val,
                    end = max_val + bin_size,
                    size = bin_size,
                ),
                hoverinfo="name+y",
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
    iplot(dict(
        data = traces,
        layout = _prepare_plot(title="{} histogram".format(statistic), xlabel=statistic, ylabel="jobs count"),
    ))


def draw_time_plot(jobset):
    """Draw line graph illustrating number of running jobs during the operation"""
    draw_comparative_time_plot([jobset])


def draw_comparative_time_plot(jobsets):
    """Draw line graphs for several jobsets on one plot"""
    if not _data_is_correct(jobsets):
        return
    step_count=200
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            min_time = min(job_info.start_time for job_info in jobset if job_info.type == job_type)
            max_time = max(job_info.end_time for job_info in jobset if job_info.type == job_type)
            time_points = numpy.linspace(max(0, min_time - 1), max_time + 1, min(step_count, max_time - min_time + 1))

            job_count = [0] * len(time_points)
            for job_info in jobs_info:
                for j, point in enumerate(time_points):
                    job_count[j] += job_info.start_time <= point <= job_info.end_time
                    
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
                hoverinfo="x+y",
            ))
    iplot(dict(
        data = traces,
        layout = _prepare_plot(xlabel="time", ylabel="jobs count"),
    ))


def draw_scatter_plot(jobset, x_statistic="input_data_weight", y_statistic="total_time"):
    """
    Draw scatter plot for two statistics.
    """
    draw_comparative_scatter_plot([jobset], x_statistic, y_statistic)


def draw_comparative_scatter_plot(jobsets, x_statistic="input_data_weight", y_statistic="total_time"):
    """Draw scatter charts for several jobsets on one plot"""
    if (not _data_is_correct(jobsets, x_statistic) or
        not _data_is_correct(jobsets, y_statistic)):
        return
    traces = []
    for i, jobset in enumerate(jobsets):
        for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
            xvalues, yvalues = zip(*[(
                _get_statistics(job_info, x_statistic),
                _get_statistics(job_info, y_statistic))
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
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
    iplot(dict(
        data = traces,
        layout = _prepare_plot(xlabel=x_statistic, ylabel=y_statistic, hovermode="closest"),
    ))


def draw_time_statistics_bar_chart(jobset):
    """
    Draw duration of different job stages as a bar chart for every job
    """
    if not _data_is_correct([jobset]):
        return
    statistics = [
        "exec_time", "user_job_cpu", "job_proxy_cpu",
        "codec_decode", "codec_encode", "input_idle_time", "busy_time",
    ]
    for job_type, job_info_per_type in groupby(jobset, key=lambda x: x.type):
        jobs_info = list(job_info_per_type)
        job_ids = [_add_line_breaks_to_guid(
            parts_to_guid(job_info.job_id_hi, job_info.job_id_lo),
            len(jobs_info),
        ) for job_info in jobs_info]
        
        traces = []
        for statistic in statistics:
            values = [getattr(job_info, statistic) for job_info in jobs_info]
            traces.append(go.Bar(
                x=job_ids,
                y=values,
                name=statistic,
                hoverinfo="name+y",
                hoverlabel=dict(
                    namelength=-1,
                ),
            ))
        iplot(dict(
            data = traces,
            layout = _prepare_plot(
                title="{} jobs".format(job_type), xlabel="jobs", ylabel="statistics values",
                hovermode="closest", bargap=0.3,
            ),
        ))
    
def print_text_data(jobset):
    """
    Print node id, job id and running time for every job
    """
    for job_type, jobs_info in groupby(jobset, key=lambda x: x.type):
        print("{} jobs:\n".format(job_type))
        for job_info in jobs_info:
            print("\t{}".format(job_info))

