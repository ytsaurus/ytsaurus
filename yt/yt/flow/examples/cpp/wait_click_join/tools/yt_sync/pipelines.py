from .stages import Stage

MAIN_PIPELINE = {
    "default": {
        "$merge_presets": ["builtin:pipeline_preset"],
    },
    Stage.STABLE: {
        "monitoring_project": "wait-click-join",
        "monitoring_cluster": "wait-click-join",
    },
    Stage.PRESTABLE: {
        "monitoring_project": "wait-click-join",
        "monitoring_cluster": "wait-click-join-pre",
    },
    Stage.DEV: {
        "monitoring_project": "wait-click-join",
        "monitoring_cluster": "wait-click-join-dev",
    },
    Stage.TEST: {
        "monitoring_project": "",
        "monitoring_cluster": "",
    },
}

PIPELINES = {
    "pipeline": MAIN_PIPELINE,
}
