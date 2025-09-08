from yt_commands import exists, create

from yt.common import wait, YtError


def get_scheduling_options(user_slots):
    return {"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": user_slots}}}}


# TODO(max42): rework this.
def get_async_expiring_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time, expiration_period):
    return {
        "expire_after_access_time": expire_after_access_time,
        "expire_after_successful_update_time": expire_after_successful_update_time,
        "refresh_time": refresh_time,
        "expiration_period": expiration_period,
    }


def get_object_attribute_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time, expiration_period):
    return {
        "yt": {
            "table_attribute_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time, expiration_period
            ),
            "permission_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time, expiration_period
            ),
        },
    }


def get_disabled_cache_config():
    disabled_cache_config = {
        "read_from": "follower",
        "expire_after_successful_update_time": 0,
        "expire_after_failed_update_time": 0,
        "refresh_time": 0,
        "expire_after_access_time": 0,
    }
    return {
        "yt": {
            "table_attribute_cache": disabled_cache_config,
            "permission_cache": disabled_cache_config,
        }
    }


def get_schema_from_description(describe_info):
    schema = []
    for info in describe_info:
        schema.append(
            {
                "name": info["name"],
                "type": info["type"],
            }
        )
    return schema


def get_breakpoint_node(breakpoint_name):
    return "//sys/clickhouse/breakpoints/{}".format(breakpoint_name)


def wait_breakpoint(breakpoint_name):
    wait(lambda: exists(get_breakpoint_node(breakpoint_name)))


def release_breakpoint(breakpoint_name):
    if not exists(get_breakpoint_node(breakpoint_name)):
        raise YtError("Breakpoint doesn't exist")
    create("document", get_breakpoint_node(breakpoint_name) + "/release")
