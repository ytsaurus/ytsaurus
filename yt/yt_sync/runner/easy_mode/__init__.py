from yt.yt_sync.core.spec_merger import StagesSpec

from yt.yt_sync.runner.core import run_yt_sync

from .make_description import make_runner_description


def run_yt_sync_easy_mode(name: str, stages_spec: StagesSpec, **kwargs):
    return run_yt_sync(name, make_runner_description(stages_spec), **kwargs)
