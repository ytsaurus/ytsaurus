from matplotlib import use
use('Agg')

from . import fields

from collections import defaultdict
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

METRICS = [
    (lambda x, mean: (max(x) - min(x)) / mean if mean else None, '(max-min) / mean'),
    (lambda x, mean: (max(x) * min(x)) / (mean * mean) if mean else None, 'max*min / mean*mean'),
    (lambda x, mean: sum(map(lambda y: abs(y - mean), x)) / (mean * len(x)) if mean else None,
     'sum_k(abs(v - mean)) / k*mean'),
    (lambda x, mean: max(x) / mean if mean else None, 'max / mean'),
    (lambda x, mean: min(x) / mean if mean else None, 'min / mean'),
]


def get_timestamp_label(timestamps):
    format_time = lambda ts : datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    if len(timestamps) == 0:
        return 'timestamp'
    if len(timestamps) == 1:
        return 'timestamp {}'.format(format_time(timestamps[0]))
    return 'timestamp from {} to {}'.format(format_time(timestamps[0]),
                                            format_time(timestamps[-1]))


def params_to_str(params):
    if params is None:
        return ''
    if isinstance(params, str):
        return params
    return '__'.join(params)


def make_plot(stat, params, save_to, boxes, plot_type, legend, percentiles):
    metric = params_to_str(params)
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(20, 10))
    fig.suptitle('{} statistics of {}'.format(plot_type, metric))

    timestamps = [item[fields.TS_FIELD] for item in stat]
    ax.set_xlabel(get_timestamp_label(timestamps))
    ax.set_ylabel(metric)

    if percentiles:
        for percentile in percentiles:
            y = [np.percentile(np.array(list(item[fields.CELLS_FIELD].values())), percentile)
                 for item in stat]
            ax.plot(timestamps, y, label='{} percentile'.format(percentile))
    else:
        ys = defaultdict(list)
        for item in stat:
            for box_id in boxes:
                ys[box_id].append(item[fields.CELLS_FIELD].get(box_id, 0))

        for box_id, y in ys.items():
            ax.plot(timestamps, y, label=box_id)

    if legend:
        ax.legend(loc='upper right')
    ax.set_ylim(bottom=0, top=None)
    fig.savefig(save_to)
    plt.close(fig)


def make_metric_plot(stat, params, save_to):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(20, 10))
    fig.suptitle('Metrics of {}'.format(params_to_str(params)))
    timestamps = [item[fields.TS_FIELD] for item in stat]
    ax.set_xlabel(get_timestamp_label(timestamps))

    for metric, name in METRICS:
        y, timestamps = [], []

        for item in stat:
            if len(item[fields.CELLS_FIELD]) == 0:
                continue

            x = list(item[fields.CELLS_FIELD].values())
            y.append(metric(x, np.mean(np.array(x))))
            timestamps.append(item[fields.TS_FIELD])

        ax.plot(timestamps, y, label=name)

    ax.legend(loc='upper right')
    ax.set_ylim(bottom=0, top=None)
    fig.savefig(save_to)
    plt.close(fig)


def make_count_plot(timestamps, counts, save_to, boxes, plot_type, metric):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(20, 10))
    fig.suptitle('Count metric of {}'.format(plot_type))
    ax.set_xlabel(get_timestamp_label(timestamps))
    ax.set_ylabel(metric)

    ys = defaultdict(list)
    for count in counts:
        for box_id in boxes:
            ys[box_id].append(count.get(box_id, 0))

    for box_id, y in ys.items():
        ax.plot(timestamps, y, label=box_id)

    ax.set_ylim(bottom=0, top=None)
    fig.savefig(save_to)
    plt.close(fig)
