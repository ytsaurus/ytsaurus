import logging
import time

from yt.wrapper.http_helpers import get_retriable_errors
import yt.wrapper as yt

from . import fields

GENERAL_KEYS = [
    'cell_id',
    'index',
    fields.PERF_COUNTERS_FIELD,
    'tablet_id',
    fields.STATISTICS_FIELD,
]
STATISTICS_KEYS = ['compressed_data_size']


def perf_keys():
    for perf_key_base in ("dynamic_row_lookup", "static_chunk_row_lookup",
                          "dynamic_row_write", "static_chunk_row_read"):
        for statistics in ("10m_rate", "1h_rate", "rate", "count"):
            yield '_'.join([perf_key_base, statistics])
            yield '_'.join([perf_key_base, "data_weight", statistics])


def filter_tablet_attrs(data):
    for tablet in data:
        result = {key : tablet[key] for key in GENERAL_KEYS}
        result[fields.PERF_COUNTERS_FIELD] = {key : tablet[fields.PERF_COUNTERS_FIELD][key]
                                              for key in perf_keys()}
        result[fields.STATISTICS_FIELD] = {key : tablet[fields.STATISTICS_FIELD][key]
                                           for key in STATISTICS_KEYS}
        yield result


def load_tablets_info(tables, ts):
    result = {
        fields.TS_FIELD : ts,
        fields.TABLES_FIELD : {}}
    for table in tables:
        if yt.get("{}/@tablet_state".format(table)) != 'mounted':
            logging.debug("Table %s is not mounted", table)
            continue
        result[fields.TABLES_FIELD][table] = list(filter_tablet_attrs(
            yt.get(table + "/@tablets")))
    return result


def get_cells(tables):
    common_bundle = None
    cells = {}
    for table in tables:
        bundle = yt.get('{}/@tablet_cell_bundle'.format(table))
        if common_bundle is not None and common_bundle != bundle:
            raise Exception("Table {} is in bundle {} when previous tables are in "
                            "bundle {}".format(table, bundle, common_bundle))
        if common_bundle is None:
            common_bundle = bundle
            cells = yt.get('//sys/tablet_cell_bundles/{}/@tablet_cell_ids'.format(bundle))
    return cells


def get_leading_peer(cell_id, peers):
    for peer in peers:
        if peer['state'] == 'leading':
            return peer['address']
    logging.debug("Cell %s has no leading peer", cell_id)


def get_leading_peer_for_cells(cell_ids):
    batch_client = yt.create_batch_client()
    responses = []
    for cell_id in cell_ids:
        responses.append(batch_client.get("#{}/@peers".format(cell_id)))
    batch_client.commit_batch()
    for cell_id, response in zip(cell_ids, responses):
        if response.is_ok():
            peer = get_leading_peer(cell_id, response.get_result())
            if peer is not None:
                yield cell_id, peer
        else:
            logging.warning("Cannot get peers for cell %s", cell_id)


def get_tables_list_by_directory(path):
    type_filters = [
        lambda table_attrs: table_attrs["type"] == "table",
        lambda table_attrs: bool(table_attrs["dynamic"]),
        lambda table_attrs: table_attrs["in_memory_mode"] == "none",
    ]

    tables = yt.list(path, attributes=["type", "dynamic", "in_memory_mode"])
    for table in tables:
        if all(predicate(table.attributes) for predicate in type_filters):
            yield "{}/{}".format(path, table)


def load_statistics(tables):
    ts = int(time.time())
    try:
        tablets_info = load_tablets_info(tables, ts)

        cell_ids = get_cells(tablets_info[fields.TABLES_FIELD])
        cell_id_to_node = {cell_id : peer for cell_id, peer in
                           get_leading_peer_for_cells(cell_ids)}

        return ts, tablets_info, cell_ids, cell_id_to_node
    except get_retriable_errors() as err:
        logging.debug("Failed to load statistics %s", err)
        return ts, None, None, None
