import yt.wrapper as yt

import os
import sys

def prepare_udfs(yt_client, yp_path):
    udf_descriptions = [
        {
            "source_path": "enums.bc",
            "arcadia_name": "yp_enums",
            "functions": [
                "format_pod_current_state",
                "format_pod_target_state",
                "format_hfsm_state",
                "format_resource_kind"
            ],
        }
    ]
    for udf_description in udf_descriptions:
        udf_path = os.path.join(os.path.dirname(__file__), "../python/yp/", udf_description["source_path"])
        udf_data = open(udf_path).read()

        names = udf_description["functions"]

        first_udf_path = yt.ypath_join(yp_path, "udfs", names[0])
        yt_client.write_file(first_udf_path, udf_data)
        for name in names[1:]:
            yt_client.copy(first_udf_path, yt.ypath_join(yp_path, "udfs", name), force=True)

    yt_client.set(yt.ypath_join(yp_path, "udfs/format_pod_current_state/@function_descriptor"),
        {
            "name": "format_pod_current_state",
            "calling_convention": "unversioned_value",
            "result_type": {"tag": "concrete_type", "value": "string"},
            "argument_types": [
                {"tag": "concrete_type", "value": "int64"},
            ],
        })

    yt_client.set(yt.ypath_join(yp_path, "udfs/format_pod_target_state/@function_descriptor"),
        {
            "name": "format_pod_target_state",
            "calling_convention": "unversioned_value",
            "result_type": {"tag" : "concrete_type", "value" : "string"},
            "argument_types": [
                {"tag": "concrete_type", "value": "int64"},
            ],
        })

    yt_client.set(yt.ypath_join(yp_path, "udfs/format_hfsm_state/@function_descriptor"),
        {
            "name": "format_hfsm_state",
            "calling_convention": "unversioned_value",
            "result_type": {"tag" : "concrete_type", "value" : "string"},
            "argument_types": [
                {"tag": "concrete_type", "value": "int64"},
            ],
        })

    yt_client.set(yt.ypath_join(yp_path, "udfs/format_resource_kind/@function_descriptor"),
        {
            "name": "format_resource_kind",
            "calling_convention": "unversioned_value",
            "result_type": {"tag" : "concrete_type", "value" : "string"},
            "argument_types": [
                {"tag": "concrete_type", "value": "int64"},
            ],
        })

def main():
    proxy, yp_path = sys.argv[1:3]
    client = yt.YtClient(proxy)
    prepare_udfs(client, yp_path)

if __name__ == "__main__":
    main()
