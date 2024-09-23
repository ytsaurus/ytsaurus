import os

import yt.wrapper
import yt.yson


SCHEMA_COLUMNS = [
    # -------------COLUMN NAME-----------------|---TYPE---|---REQUIRED---|--ATTRIBUTE--#
    ("_read_schema_attributes",                 "any",          False,      False),
    ("_read_schema",                            "any",          False,      False),
    ("_yql_read_udf",                           "string",       False,      False),
    ("_yql_row_spec_attributes",                "any",          False,      False),
    ("_yql_row_spec",                           "any",          False,      False),
    ("access_time",                             "string",       True,       True),
    ("account",                                 "string",       True,       True),
    ("acl",                                     "any",          False,      True),
    ("children_held",                           "any",          False,      False),
    ("chunk_count",                             "int64",        False,      True),
    ("chunk_format",                            "string",       False,      True),
    ("chunk_row_count",                         "int64",        False,      True),
    ("compressed_data_size",                    "int64",        False,      True),
    ("compression_codec",                       "string",       False,      True),
    ("creation_time",                           "string",       True,       True),
    ("cypress_transaction_id",                  "string",       False,      False),
    ("cypress_transaction_timestamp",           "any",          False,      False),
    ("cypress_transaction_title",               "string",       False,      False),
    ("data_weight",                             "int64",        False,      True),
    ("dynamic",                                 "any",          False,      True),
    ("erasure_codec",                           "string",       False,      True),
    ("external",                                "boolean",      True,       True),
    ("id",                                      "string",       True,       True),
    ("in_memory_mode",                          "string",       False,      True),
    ("inherit_acl",                             "boolean",      True,       True),
    ("media",                                   "any",          False,      True),
    ("modification_time",                       "string",       True,       True),
    ("monitor_table_statistics",                "any",          False,      True),
    ("nightly_compression_settings",            "any",          False,      True),
    ("optimize_for",                            "string",       False,      True),
    ("original_owner",                          "string",       False,      True),
    ("originator_cypress_transaction_id",       "string",       False,      False),
    ("orphaned",                                "boolean",      False,      False),
    ("owner",                                   "string",       False,      True),
    ("parent_id",                               "string",       False,      True),
    ("path",                                    "string",       True,       False),
    ("primary_medium",                          "string",       False,      True),
    ("ref_counter",                             "int64",        True,       True),
    ("replication_factor",                      "int64",        False,      True),
    ("resource_usage",                          "any",          False,      True),
    ("revision",                                "uint64",       False,      True),
    ("row_count",                               "int64",        False,      True),
    ("schema_attributes",                       "any",          False,      True),
    ("schema_mode",                             "string",       False,      True),
    # ("schema",                                  "any",          False,      True),
    ("sorted",                                  "any",          False,      True),
    ("security_tags",                           "any",          False,      True),
    ("tablet_cell_bundle",                      "string",       False,      True),
    ("target_path",                             "string",       False,      True),
    ("type",                                    "string",       True,       True),
    ("uncompressed_data_size",                  "int64",        False,      True),
    ("user_attribute_keys",                     "any",          False,      True),
    ("versioned_resource_usage",                "any",          False,      True),
    ("last_compression_time",                   "string",       False,      True),
    ("force_nightly_compress",                  "any",          False,      True),
    ("nightly_compressed",                      "boolean",      False,      True),
    ("last_compression_desired_chunk_size",     "int64",        False,      True),
    ("nightly_compression_select_timestamp",    "int64",        False,      True),
    ("nightly_compression_user_time",           "string",       False,      True),
    ("tablet_state",                            "string",       False,      True),
    ("expiration_time",                         "string",       False,      True),
]

DEFAULT_VALUES = {
    "account": "default",
    "creation_time": "2011-02-13T12:13:14.151617Z",
    "modification_time": "2011-02-13T12:13:14.151618Z",
    "access_time": "2011-02-13T12:13:14.151619Z",
    "external": False,
}

EXPORT_DESTINATION = "//sys/admin/snapshots/snapshot_exports/latest"


class FakeSnapshotRunner:
    def check_required_attributes(self, attributes: dict):
        for attribute in DEFAULT_VALUES:
            if attribute not in attributes:
                attributes[attribute] = DEFAULT_VALUES[attribute]
        return attributes

    def _create_schema(self):
        schema = yt.yson.YsonList()
        schema.attributes["unique_keys"] = False
        schema.attributes["strict"] = True
        for column_name, type, required, _ in SCHEMA_COLUMNS:
            schema.append({"name": column_name, "type": type, "required": required})
        return schema

    def _get_attributes(self):
        return [column[0] for column in SCHEMA_COLUMNS]

    def build_and_export_master_snapshot(self, yt_client: yt.wrapper.YtClient):
        yt_client.remove(os.path.dirname(EXPORT_DESTINATION), recursive=True, force=True)
        yt_client.create("map_node", os.path.dirname(EXPORT_DESTINATION), recursive=True)
        yt_client.create("table", EXPORT_DESTINATION, recursive=True, ignore_existing=True, attributes={"schema": self._create_schema()})
        snapshot = yt_client.search("/", attributes=self._get_attributes())
        yt_client.write_table(EXPORT_DESTINATION, map(lambda x: self.check_required_attributes(x.attributes), [row for row in snapshot if "path" in row.attributes]))
