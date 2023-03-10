import vh3
from typing import Literal, Optional


@vh3.decorator.operation(deterministic=True, owner='robot-yt-sch-usage')
@vh3.decorator.autorelease_to_nirvana_on_trunk_commit(
    version="https://nirvana.yandex-team.ru/alias/operation/prepare-scheduling-usage-log/1.23",
    ya_make_folder_path="yt/yt/tools/prepare_scheduling_usage/vh3",
)
@vh3.decorator.update_defaults(
    # In MB.
    max_ram=1024,
)
@vh3.decorator.resources(vh3.YaMakeResource("yt/yt/tools/prepare_scheduling_usage", name="prepare_scheduling_usage_binary"))
@vh3.decorator.job_command_from_str(
    """bash -c 'export YT_TOKEN=${param["yt_token"]}; ${resource["prepare_scheduling_usage_binary"]} --cluster ${param["yt_cluster"]} """
    """--input-path ${param["input_path"]} --output-path ${param["output_dir"]} --pool ${param["pool"]} --mode table """
    """[#if param["set_expiration_timeout"]??]--set-expiration-timeout ${param["set_expiration_timeout"]}[/#if]'"""
)
def prepare_scheduling_usage_log(
    *,
    yt_token: vh3.Secret,
    pool: vh3.String,
    input_path: vh3.String = "",
    output_dir: vh3.String = "",
    yt_cluster: vh3.String = "hahn",
    set_expiration_timeout: Optional[vh3.Integer] = None,
    network: Optional[vh3.Enum[Literal["none"]]] = None
) -> vh3.NoOutput:
    """
    Prepare scheduling usage log

    :param input_path
      Input path to scheduler structured log
    :param output_dir
    :param yt_cluster
    :param yt_token
    :param pool
    :param set_expiration_timeout
    :param network
      Network for operation container, see https://nda.ya.ru/t/GPQyfuIh5KovVk for more information
    """

    raise NotImplementedError()
