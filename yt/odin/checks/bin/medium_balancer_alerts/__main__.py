from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    alert_message = ""

    if not yt_client.exists("//sys/cron/medium_balancer/enabled") or not yt_client.get("//sys/cron/medium_balancer/enabled"):
        alert_message += "Medium balancer disabled://sys/cron/medium_balancer/enabled is %false; "

    failed = False
    if yt_client.exists("//sys/cron/medium_balancer/errors"):
        errors = yt_client.get("//sys/cron/medium_balancer/errors")
        if errors:
            failed = True
            logger.error("Medium balancer errors:")
            for error in errors:
                logger.error(error)
                alert_message += error + "; "

    if failed:
        logger.error(alert_message)
        return states.UNAVAILABLE_STATE, alert_message
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
