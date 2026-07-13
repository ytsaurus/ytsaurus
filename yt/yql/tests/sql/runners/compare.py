import os

from yql_utils import dump_table_yson, is_sorted_table, is_unordered_result, \
    normalize_result, stable_result_file, stable_table_file


def compare_results(primary_res, reference_res, primary_name, reference_name):
    if not os.path.exists(reference_res.results_file):
        return

    assert os.path.exists(primary_res.results_file)
    primary_res_yson = normalize_result(stable_result_file(primary_res), is_unordered_result(primary_res))
    reference_res_yson = normalize_result(stable_result_file(reference_res), is_unordered_result(reference_res))

    assert primary_res_yson == reference_res_yson, 'RESULTS_DIFFER\n' \
        '%(primary_name)s result:\n %(primary_res_yson)s\n\n' \
        '%(reference_name)s result:\n %(reference_res_yson)s\n' % locals()


def compare_output_tables(primary_tables_res, reference_tables_res, primary_name, reference_name):
    for table in reference_tables_res:
        assert table in primary_tables_res

        if not os.path.exists(reference_tables_res[table].file):
            continue

        assert os.path.exists(primary_tables_res[table].file)
        reference_table_yson = dump_table_yson(
            stable_table_file(reference_tables_res[table]),
            sort=not is_sorted_table(reference_tables_res[table]),
        )
        primary_table_yson = dump_table_yson(
            stable_table_file(primary_tables_res[table]),
            sort=not is_sorted_table(primary_tables_res[table]),
        )

        assert reference_table_yson == primary_table_yson, \
            'OUT_TABLE_DIFFER: %(table)s\n' \
            '%(primary_name)s table:\n %(primary_table_yson)s\n\n' \
            '%(reference_name)s table:\n %(reference_table_yson)s\n' % locals()

        reference_table_attr = reference_tables_res[table].attr
        primary_table_attr = primary_tables_res[table].attr
        assert reference_table_attr == primary_table_attr, \
            'OUT_TABLE_ATTR_DIFFER: %(table)s\n' \
            '%(primary_name)s table attrs:\n %(primary_table_attr)s\n\n' \
            '%(reference_name)s table attrs:\n %(reference_table_attr)s\n' % locals()


def compare_file_run_with_reference(primary_res, primary_tables_res, reference_res, reference_tables_res,
                                    primary_name, reference_name):
    compare_results(primary_res, reference_res, primary_name, reference_name)
    compare_output_tables(primary_tables_res, reference_tables_res, primary_name, reference_name)
