import argparse
import logging
import os
import signal
import sys
import time
from yt import yson

from yt.yt.tools.dynamic_tables.tablet_workload_balancer.lib import (
    algorithms, common, fields, load, plot)


INTERRUPT_CAUGHT = False
CELLS_PLOT = 'cells.png'
NODES_PLOT = 'nodes.png'
CELL_METRICS_PLOT = 'metrics_by_cells.png'
NODE_METRICS_PLOT = 'metrics_by_nodes.png'
TABLET_COUNT_BY_CELL_PLOT = 'tablet_count_by_cells.png'
CELL_COUNT_BY_NODE_PLOT = 'cell_count_by_nodes.png'
TABLET_COUNT_BY_NODE_PLOT = 'tablet_count_by_nodes.png'


'''
configuration structure
[
    {
        'params': param or YsonList [param], required;
        'plot_dir': dirname to save plots, required;
        'plot_label_name': label to the plots, params by default;
        'legend': False by default;
        'percentiles': YsonList[percentile], plot only percentlies, disabled by default;
    };
    ...
]
'''


def parse():
    parser = argparse.ArgumentParser(description='Statistics plot.')
    common.add_common_arguments(parser)

    parser.add_argument('-p', '--period', type=int, required=True, help='delay betweet two runs')
    parser.add_argument('--config', type=str, help='for build plots by many params')
    parser.add_argument('--params', nargs="+", help='params (returns sum of params)')
    parser.add_argument('--plot-label-name', type=str, default=None,
                        help='set label to the plot')
    parser.add_argument('--by-nodes', action='store_true', help='make plot by nodes')
    parser.add_argument('--by-cells', action='store_true', help='make plot by cells')
    parser.add_argument('--plot-counts', action='store_true', help='make count metric plots')

    return parser.parse_args()


def save(data, filename):
    with open(filename, 'a') as f:
        yson.dump(data, f)


def make_plot_struct(ts, cells):
    return {fields.TS_FIELD: ts,
            fields.CELLS_FIELD: cells}


def calc_cells_metric(tablets_info, params, arrangement):
    return algorithms.utils.sum_by(algorithms.utils.get_tablets_params(
        tablets_info[fields.TABLES_FIELD], params), arrangement)


def load_config(config_str):
    config = yson.loads(str.encode(config_str))
    for plot_config in config:
        assert plot_config['params']
        assert plot_config['plot_dir']
        assert os.path.isdir(plot_config['plot_dir'])
        plot_config['legend'] = plot_config.setdefault("legend", False)
        plot_config['plot_label_name'] = plot_config.get('plot_label_name', None)
        plot_config['percentiles'] = plot_config.get('percentiles', None)
        assert len(plot_config) == 5
    return config


def make_config_from_args(args):
    return {
        'params': args.params,
        'plot_dir': args.plot_directory,
        'legend': args.legend,
        'plot_label_name': args.plot_label_name,
        'percentiles': yson.loads(str.encode(args.percentiles)) if args.percentiles else None,
    }


def make_plots_by_config(config, cells_plot_data, nodes_plot_data, total_cells, total_nodes,
                         tablets_info, arrangement, nodes, by_nodes, by_cells):
    cell_id_to_magnitude = calc_cells_metric(tablets_info, config['params'], arrangement)

    if by_cells:
        cells_data = make_plot_struct(tablets_info[fields.TS_FIELD], cell_id_to_magnitude)
        cells_plot_data.append(cells_data)
        plot.make_plot(cells_plot_data, config['plot_label_name'],
                       os.path.join(config['plot_dir'], CELLS_PLOT), list(total_cells), '',
                       config['legend'], config['percentiles'])
        plot.make_metric_plot(cells_plot_data, config['plot_label_name'],
                              os.path.join(config['plot_dir'], CELL_METRICS_PLOT))

    if by_nodes:
        node_id_to_magnitude = algorithms.utils.sum_by(cell_id_to_magnitude, nodes)
        nodes_data = make_plot_struct(tablets_info[fields.TS_FIELD], node_id_to_magnitude)
        nodes_plot_data.append(nodes_data)
        plot.make_plot(nodes_plot_data, config['plot_label_name'],
                       os.path.join(config['plot_dir'], NODES_PLOT), list(total_nodes), '',
                       config['legend'], config['percentiles'])
        plot.make_metric_plot(nodes_plot_data, config['plot_label_name'],
                              os.path.join(config['plot_dir'], NODE_METRICS_PLOT))


def plot_main():
    args = parse()

    if not args.by_nodes and not args.by_cells and not args.plot_counts:
        print('At least one of --by-nodes, --by-cells and --plot-counts must be specified')
        return

    if args.tables:
        tables = args.tables
    else:
        tables = list(load.get_tables_list_by_directory(args.directory))
    multi_plot_config = load_config(args.config) if args.config else [make_config_from_args(args)]

    logging.basicConfig(
        stream=sys.stderr,
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG if args.debug_log else logging.INFO)
    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.info('Plotter configuration: %s', multi_plot_config)

    sleep_time = 0
    cells_plot_data = [[] for _ in multi_plot_config]
    nodes_plot_data = [[] for _ in multi_plot_config]
    total_cells, total_nodes = set(), set()
    if args.plot_counts:
        timestamps = []
        cell_id_to_tablet_count_list = []
        node_id_to_cell_count_list = []
        node_id_to_tablet_count_list = []

    while not INTERRUPT_CAUGHT:
        time.sleep(sleep_time)
        ts, tablets_info, cells, cell_id_to_node = load.load_statistics(tables)
        if not cells:
            sleep_time = max(args.period - (int(time.time()) - ts), 0)
            continue

        nodes = algorithms.utils.invert_dict(cell_id_to_node)
        arrangement = algorithms.utils.init_arrangement(tablets_info, cell_id_to_node.keys())

        total_cells |= set(cells)
        total_nodes |= set(nodes.keys())

        if args.debug_mode:
            save(tablets_info, args.tablets_info_file)
            save(cell_id_to_node, args.cells_info_file)

        if args.by_nodes or args.by_cells:
            for index, plot_config in enumerate(multi_plot_config):
                make_plots_by_config(plot_config, cells_plot_data[index], nodes_plot_data[index],
                                     total_cells, total_nodes, tablets_info, arrangement, nodes,
                                     args.by_nodes, args.by_cells)

        if args.plot_counts:
            timestamps.append(ts)
            node_id_to_cell_count_list.append({
                node_id : len(cells) for node_id, cells in nodes.items()})
            cell_id_to_tablet_count_list.append({
                cell_id : len(tablets) for cell_id, tablets in arrangement.items()})
            node_id_to_tablet_count_list.append({
                node_id : sum([len(arrangement[cell_id]) for cell_id in cells])
                for node_id, cells in nodes.items()})

            plot.make_count_plot(timestamps, cell_id_to_tablet_count_list,
                                 os.path.join(args.plot_directory, TABLET_COUNT_BY_CELL_PLOT),
                                 list(total_cells), 'cells', 'tablet_count')
            plot.make_count_plot(timestamps, node_id_to_cell_count_list,
                                 os.path.join(args.plot_directory, CELL_COUNT_BY_NODE_PLOT),
                                 list(total_nodes), 'nodes', 'cell_count')
            plot.make_count_plot(timestamps, node_id_to_tablet_count_list,
                                 os.path.join(args.plot_directory, TABLET_COUNT_BY_NODE_PLOT),
                                 list(total_nodes), 'nodes', 'tablet_count')

        sleep_time = max(args.period - (int(time.time()) - ts), 0)


def interrupt_handler(sig, frame):
    global INTERRUPT_CAUGHT
    INTERRUPT_CAUGHT = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, interrupt_handler)
    plot_main()
