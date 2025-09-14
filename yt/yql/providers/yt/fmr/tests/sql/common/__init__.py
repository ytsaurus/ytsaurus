from yt import yson
import cyson
from yql_utils import get_table_clusters, replace_vals


def sort_yson(yson):
    for res in yson:
        for data in res[b'Write']:
            if b'Unordered' in res and b'Data' in data:
                data[b'Data'] = sorted(data[b'Data'])
    return yson


def add_table_clusters(suite, config, data_path):
    clusters = get_table_clusters(suite, config, data_path)
    if not clusters:
        return None

    def patch(cfg_message):
        for c in sorted(clusters):
            mapping = cfg_message.Yt.ClusterMapping.add()
            mapping.Name = c
    return patch


def get_yson_from_file_sql_query_result(query_result):
    res_yson = query_result.results
    res_yson = cyson.loads(res_yson) if res_yson else cyson.loads("[]")
    res_yson = replace_vals(res_yson)
    return sort_yson(res_yson)


def get_yson_from_yt_sql_query_result(query_result):
    yt_res_yson = query_result.results.get('data', [])
    return replace_vals(cyson.loads(yson.dumps(yt_res_yson)))
