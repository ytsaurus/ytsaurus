from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    path = "//sys/@master_alerts"
    alerts = []
    if yt_client.exists(path):
        for alert in yt_client.get(path):
            alerts.append(alert)
    if alerts:
        logger.info(f"Path {path} contains the following alerts: {alerts}")
        return states.UNAVAILABLE_STATE, str(alerts)
    logger.info(f"No alerts matching config found at path {path}")
    return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
