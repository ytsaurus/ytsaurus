def get_scheduling_options(user_slots):
    return {"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": user_slots}}}}


# TODO(max42): rework this.
def get_async_expiring_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time):
    return {
        "expire_after_access_time": expire_after_access_time,
        "expire_after_successful_update_time": expire_after_successful_update_time,
        "refresh_time": refresh_time,
    }


def get_object_attribute_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time):
    return {
        "yt": {
            "table_attribute_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time
            ),
            "permission_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time
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
