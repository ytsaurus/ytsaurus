# Pipelines describes the pipelines to create in YT.
#
# Each key is the pipeline entity name used by yt_sync as a path to create Cypress objects.
#
# Config for pipeline under stages:
#   "$merge_presets" pulls in the standard pipeline preset.
#   "monitoring_project" / "monitoring_cluster" are required coordinates for metrics.
PIPELINES = {
    "pipeline": {
        "default": {
            "$merge_presets": ["builtin:pipeline_preset"],
            "monitoring_project": "",
            "monitoring_cluster": "",
        },
    },
}
