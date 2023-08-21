import argparse
import os

from yt import yson
from yt.yt.tools.dynamic_tables.tablet_workload_balancer.lib import algorithms, fields, load, plot


PROCESSED_PLOT_FILENAME = '{}processed.png'
INITIAL_PLOT_FILENAME = '{}initial.png'


def parse():
    parser = argparse.ArgumentParser(description='Predict arrangements.')
    parser.add_argument('--stat', type=str, default='stat.json', help='statistics file')
    parser.add_argument('--cells', type=str, default='cells.json', help='cell arrangements file')
    parser.add_argument('--offset', type=int, default=0,
                        help='history statistics offset (skip n predicts)')
    parser.add_argument('--algorithm', type=str, default=next(iter(algorithms.ALGORITHMS.keys())),
                        choices=list(algorithms.ALGORITHMS.keys()), help='balancing type')
    parser.add_argument('--config', type=str, default='', help='algorithm configuration')
    parser.add_argument('--plot-directory', type=str, default='./', help='dirname to save plot')
    parser.add_argument('--legend', action='store_true', help='add legend to plots')
    parser.add_argument('--plot-names-prefix', type=str, default='',
                        help='add prefix to plots (end with _)')
    parser.add_argument('--percentiles', type=str, default=None,
                        help='plot only percentlies, a list of percentlies is expected')
    parser.add_argument('--by-all-fields', action='store_true', help='make all plots by all opt_fields')
    parser.add_argument('--list-fragment-format', action='store_true', help='yson files type')
    return parser.parse_args()


def simulate(algorithm, statistics, cell_addresses, algo_config, plot_path,
             plot_names_prefix, legend, percentiles):
    processed_actions, processed_plot, initial_plot, cells = \
        algorithms.utils.process_snapshot_series(algorithm, statistics, cell_addresses, algo_config)

    plot.make_plot(processed_plot, algo_config.get(fields.PLOT_FIELD, algo_config[fields.OPT_FIELD]),
                   os.path.join(plot_path, PROCESSED_PLOT_FILENAME.format(plot_names_prefix)),
                   list(cells), 'Processed', legend, percentiles)

    plot.make_plot(initial_plot, algo_config.get(fields.PLOT_FIELD, algo_config[fields.OPT_FIELD]),
                   os.path.join(plot_path, INITIAL_PLOT_FILENAME.format(plot_names_prefix)),
                   list(cells), 'Initial', legend, percentiles)

    return processed_actions


def yson_load(fin, list_fragment_format=False):
    if list_fragment_format:
        return yson.load(fin, yson_type="list_fragment")
    return yson.load(fin)


def main():
    args = parse()

    percentiles = yson.loads(str.encode(args.percentiles)) if args.percentiles else None

    if not args.by_all_fields:
        algo_config = yson.loads(str.encode(args.config))
        with open(args.stat, 'rb') as fstat, open(args.cells, 'rb') as fcells:
            statistics = yson_load(fstat, args.list_fragment_format)
            cell_addresses = yson_load(fcells, args.list_fragment_format)
            processed_actions = simulate(algorithms.ALGORITHMS[args.algorithm], statistics,
                                         cell_addresses, algo_config, args.plot_directory,
                                         args.plot_names_prefix, args.legend, percentiles)

        for actions in processed_actions:
            print(actions)
        return

    for field in load.perf_keys():
        for plot_field in load.perf_keys():
            algo_config = {
                fields.OPT_FIELD : field,
            }

            plot_names_prefix = field + '__'
            if plot_field != field:
                algo_config[fields.PLOT_FIELD] = plot_field
                plot_names_prefix += 'plot__' + plot_field + '__'

            with open(args.stat, 'rb') as fstat, open(args.cells, 'rb') as fcells:
                statistics = yson_load(fstat, args.list_fragment_format)
                cell_addresses = yson_load(fcells, args.list_fragment_format)
                processed_actions = simulate(algorithms.ALGORITHMS[args.algorithm], statistics,
                                             cell_addresses, algo_config, args.plot_directory,
                                             plot_names_prefix, args.legend, percentiles)

            print()
            for actions in processed_actions:
                print(actions)


if __name__ == '__main__':
    main()
