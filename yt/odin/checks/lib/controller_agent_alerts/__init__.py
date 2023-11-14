def run_check_impl(yt_client, logger, options, states):
    instances = yt_client.list("//sys/controller_agents/instances", attributes=["alerts", "tags"])
    logger.debug("Collected %d controller agents", len(instances))

    no_alerts = True

    for instance in instances:
        address = str(instance)
        if "default" not in instance.attributes.get("tags", []):
            logger.info("%s does not have the 'default' tag", address)
            continue
        alerts = instance.attributes.get("alerts")
        if alerts:
            logger.info("%s: %s", address, alerts)
            no_alerts = False

    return states.FULLY_AVAILABLE_STATE if no_alerts else states.UNAVAILABLE_STATE
