import logging
from yt import yson
import yt.wrapper as yt

from . import algorithms, load


def add_common_arguments(parser):
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-t', '--tables', nargs="+", help='table paths')
    group.add_argument('--directory', type=str, help='directory with tables')

    parser.add_argument('--proxy', type=yt.config.set_proxy, help='yt proxy')
    parser.add_argument('--plot-directory', type=str, help='dirname to save plot')
    parser.add_argument('--debug-mode', action='store_true',
                        help='save tablets_info and cells_info')
    parser.add_argument('--tablets-info-file', type=str, default='tablets.ysons',
                        help='only with debug-mode')
    parser.add_argument('--cells-info-file', type=str, default='cells.ysons',
                        help='only with debug-mode')
    parser.add_argument('--legend', action='store_true', help='add legend to plots')
    parser.add_argument('--percentiles', type=str, default=None,
                        help='plot only percentlies, a list of percentlies is expected')
    parser.add_argument('--debug-log', action='store_true', help='set logging level to DEBUG')


def save_yson_list_item(data, filename):
    with open(filename, 'a') as f:
        yson.dump(data, f)
        print(';', file=f)


def balancing_step(tables, args, algo_config, algorithm):
    ts, tablets_info, cells, cell_id_to_node = load.load_statistics(tables)
    if not cells:
        return ts, None

    logging.debug('load_statistics start timestamp %s', ts)

    nodes = algorithms.utils.invert_dict(cell_id_to_node)
    arrangement = algorithms.utils.init_arrangement(tablets_info, cell_id_to_node.keys())

    actions, state, processed_plot, initial_plot = algorithm(
        tablets_info, arrangement, nodes, **algo_config).process()

    logging.debug('List of generated actions:\n%s', actions)

    if actions and not args.dry_run:
        result = actions.execute()
        if result.unknown:
            logging.error("Unknown execution status of move actions")
            return ts, None
        if not result.ok:
            if result.text:
                logging.error("Actions execute error: %s", result.text)
            logging.error("Failed to execute move actions")
            if result.states is not None:
                logging.debug("States by tables: %s",
                              ("tablet_action for table {} {}".format(table, state)
                               for table, state in result.states.items()))
            return ts, None
        logging.info("Actions completed successfully")
    elif not actions:
        logging.info("No actions found")

    if args.debug_mode:
        save_yson_list_item(tablets_info, args.tablets_info_file)
        save_yson_list_item(cell_id_to_node, args.cells_info_file)

    return ts, (processed_plot, initial_plot, cells)
