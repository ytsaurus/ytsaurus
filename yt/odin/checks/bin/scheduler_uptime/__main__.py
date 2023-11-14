from yt_odin_checks.lib.check_runner import main

from yt.common import date_string_to_datetime
import yt.wrapper as yt


def is_zero_time_string(time_string):
    return date_string_to_datetime(time_string).year == 1970


def run_check_impl(yt_client, logger, options, states):
    scheduler_uptime_path = "//sys/admin/odin/scheduler_uptime"
    try:
        new_scheduler_service_info = yt_client.get("//sys/scheduler/orchid/scheduler/service")

        if is_zero_time_string(new_scheduler_service_info["last_connection_time"]):
            logger.info("Scheduler has disconnected and is not connected now")
            return states.UNAVAILABLE_STATE, "Scheduler has disconnected and is not connected now"

    except yt.YtError as err:
        if err.is_resolve_error():
            new_scheduler_service_info = None
        else:
            return states.PARTIALLY_AVAILABLE_STATE, "Failed to get scheduler service information"

    if new_scheduler_service_info is None:
        version = yt_client.get("//sys/scheduler/orchid/service/version")
        hostname = yt_client.get("//sys/scheduler/orchid/service/hostname")
        connection_time = yt_client.get("//sys/scheduler/@connection_time")
    else:
        version = new_scheduler_service_info["build_version"]
        connection_time = new_scheduler_service_info["last_connection_time"]
        hostname = new_scheduler_service_info["hostname"]

    logger.info("Scheduler info (connection_time: %s, version: %s, hostname: %s)", connection_time, version, hostname)
    if yt_client.exists(scheduler_uptime_path):
        scheduler_update_info = yt_client.get(scheduler_uptime_path)
        logger.info("Previous scheduler info from %s (%s)", scheduler_uptime_path, scheduler_update_info)
        previous_is_under_mainenance_path = "//sys/scheduler/instances/{}/@maintenance".format(scheduler_update_info.get("hostname", None))
        previous_is_under_maintenance = yt_client.exists(previous_is_under_mainenance_path) and yt_client.get(previous_is_under_mainenance_path)
        previous_connection_time_is_zero = date_string_to_datetime(scheduler_update_info["connection_time"]).year == 1970
        if (previous_connection_time_is_zero
                or scheduler_update_info["connection_time"] == connection_time
                or scheduler_update_info["version"] != version
                or previous_is_under_maintenance):
            result = states.FULLY_AVAILABLE_STATE
        else:
            message = "Scheduler reconnected at {}".format(connection_time)
            logger.info(message)
            result = (states.UNAVAILABLE_STATE, message)
    else:
        logger.info("Last scheduler info not found in {}".format(scheduler_uptime_path))
        yt_client.create("document", scheduler_uptime_path, attributes={"value": {}})
        result = (states.PARTIALLY_AVAILABLE_STATE, "Last scheduler info not found in {}".format(scheduler_uptime_path))

    yt_client.set(scheduler_uptime_path, {"connection_time": connection_time, "version": version, "hostname": hostname})

    return result


def run_check(yt_client, logger, options, states):
    exceptions_list = yt.get_retriable_errors() + (yt.YtError,)
    try:
        return run_check_impl(yt_client, logger, options, states)
    except exceptions_list:
        logger.warning("Communication error")
        return states.PARTIALLY_AVAILABLE_STATE, "Cypress communication error"


if __name__ == "__main__":
    main(run_check)
