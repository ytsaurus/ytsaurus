from ..fields import (TABLET_ID_FIELD, TABLES_FIELD, CELL_ID_FIELD,
                                        PERF_COUNTERS_FIELD, STATISTICS_FIELD)


def init_arrangement(snapshot, cell_ids=None):
    if cell_ids is None:
        cell_ids = []
    inverted_arrangement = get_tablets_param(snapshot[TABLES_FIELD], CELL_ID_FIELD)
    arrangement = invert_dict(inverted_arrangement)
    for cell_id in cell_ids:
        if cell_id not in arrangement:
            arrangement[cell_id] = set()
    return arrangement


def process_snapshot_series(algorithm, statistics, cell_addresses, args):
    actions, processed_plot_data, initial_plot_data, cells = [], [], [], set()
    arrangement = None

    for snapshot, cell_id_to_node in zip(statistics, cell_addresses):
        nodes = invert_dict(cell_id_to_node)

        curr_cells = set.union(*nodes.values())
        cells |= curr_cells
        if arrangement is None:
            arrangement = init_arrangement(snapshot)
        arrangement = {cell: arrangement.get(cell, set()) for cell in curr_cells}

        curr_tablets = set(
            item[TABLET_ID_FIELD] for tablets in snapshot[TABLES_FIELD].values()
            for item in tablets)
        arrangement = {cell: tablets.intersection(curr_tablets)
                       for cell, tablets in arrangement.items()}

        arrangement_tablets = set.union(*arrangement.values())
        tablet_to_cell = invert_dict(init_arrangement(snapshot))
        for tablet in curr_tablets - arrangement_tablets:
            arrangement[tablet_to_cell[tablet]].add(tablet)

        item_actions, state, processed_plot, initial_plot = algorithm(
            snapshot, arrangement, nodes, **args).process()
        arrangement = state.cell_to_tablets
        actions.append(item_actions)
        processed_plot_data.append(processed_plot)
        initial_plot_data.append(initial_plot)

    return actions, processed_plot_data, initial_plot_data, cells


'''
expected func from {min, max}
'''


def find_top_tablet_in_cell(cell_id, by_tablet, arrangement, func):
    tablet_ids = arrangement[cell_id]
    tablet_id = func(filter(lambda k: k in tablet_ids, by_tablet), key=by_tablet.get)
    return tablet_id


def sum_by(to_what, arrangement):
    return {key_id: sum(to_what.get(what_id, 0) for what_id in key_info)
            for key_id, key_info in arrangement.items()}


def get_tablets_param(snapshot, field):
    result = dict()
    for tablets in snapshot.values():
        for elem in tablets:
            if field in elem.keys():
                result[elem[TABLET_ID_FIELD]] = elem[field]
            elif field in elem[PERF_COUNTERS_FIELD].keys():
                result[elem[TABLET_ID_FIELD]] = elem[PERF_COUNTERS_FIELD][field]
            else:
                result[elem[TABLET_ID_FIELD]] = elem[STATISTICS_FIELD].get(field)
    return result


'''
snapshot: dict (statistic item)
fields: str or not empty List[str]

returns: dict { tablet_id : aggregate }
where aggregate = statistics[fields] if fields: str
otherwise aggregate = sum of statistics[field] for each field in fields
'''


def get_tablets_params(snapshot, fields):
    if isinstance(fields, str):
        return get_tablets_param(snapshot, fields)
    result = get_tablets_param(snapshot, fields[0])
    for field in fields[1:]:
        add = get_tablets_param(snapshot, field)
        result = {key : result[key] + add[key] for key in result}
    return result


def invert_dict(data):
    v = {}
    for key, value in data.items():
        if not isinstance(value, str) and is_iterable(value):
            for elem in value:
                v[elem] = key
        else:
            v.setdefault(value, set()).add(key)
    return v


def is_iterable(obj):
    try:
        _ = iter(obj)
        return True
    except TypeError:
        return False
    except:
        raise
