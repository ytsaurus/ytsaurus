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
PROCESSED_PLOT_FILENAME = '{}processed.png'
INITIAL_PLOT_FILENAME = '{}initial.png'


def parse():
    parser = argparse.ArgumentParser(description='Tablet balancer.')
    common.add_common_arguments(parser)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--period', type=int, help='delay betweet two runs')
    group.add_argument('--one-shot', action='store_true', help='one-time balancing')

    parser.add_argument('--algorithm', type=str, default=next(iter(algorithms.ALGORITHMS.keys())),
                        choices=list(algorithms.ALGORITHMS.keys()), help='balancing type')
    parser.add_argument('--config', type=str, default='', help='algorithm configuration')
    parser.add_argument('--dry-run', action='store_true',
                        help="don't execute actions, just simulate them")
    parser.add_argument('--plot-names-prefix', type=str, default='',
                        help='add prefix to plots (should end with _)')

    return parser.parse_args()


def balance_main():
    args = parse()
    algo_config = yson.loads(str.encode(args.config))
    percentiles = yson.loads(str.encode(args.percentiles)) if args.percentiles else None
    algorithm = algorithms.ALGORITHMS[args.algorithm]

    if args.tables:
        tables = args.tables
    else:
        tables = list(load.get_tables_list_by_directory(args.directory))

    logging.basicConfig(
        stream=sys.stderr,
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG if args.debug_log else logging.INFO)
    logging.getLogger('matplotlib.font_manager').disabled = True

    if args.one_shot:
        common.balancing_step(tables, args, algo_config, algorithm)
        return

    sleep_time = 0
    all_cells = set()
    initial_plot_data = []
    processed_plot_data = []

    while not INTERRUPT_CAUGHT:
        time.sleep(sleep_time)

        ts, result = common.balancing_step(tables, args, algo_config, algorithm)
        if not result or not args.plot_directory:
            sleep_time = max(args.period - (int(time.time()) - ts), 0)
            continue

        processed_plot, initial_plot, cells = result

        all_cells |= set(cells)
        processed_plot_data.append(processed_plot)
        initial_plot_data.append(initial_plot)

        plot.make_plot(processed_plot_data,
                       algo_config.get(fields.PLOT_FIELD, algo_config[fields.OPT_FIELD]),
                       os.path.join(args.plot_directory,
                                    PROCESSED_PLOT_FILENAME.format(args.plot_names_prefix)),
                       list(all_cells), 'Processed', args.legend, percentiles)

        plot.make_plot(initial_plot_data,
                       algo_config.get(fields.PLOT_FIELD, algo_config[fields.OPT_FIELD]),
                       os.path.join(args.plot_directory,
                                    INITIAL_PLOT_FILENAME.format(args.plot_names_prefix)),
                       list(all_cells), 'Initial', args.legend, percentiles)
        sleep_time = max(args.period - (int(time.time()) - ts), 0)


def interrupt_handler(sig, frame):
    global INTERRUPT_CAUGHT
    INTERRUPT_CAUGHT = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, interrupt_handler)
    balance_main()
