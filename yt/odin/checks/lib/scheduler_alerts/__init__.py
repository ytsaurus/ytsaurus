import re


def get_pool_trees(alert):
    result = []
    for error in alert["inner_errors"]:
        if "pool_tree" in error["attributes"]:
            result.append(error["attributes"]["pool_tree"])
    return result


def match_pool_tree(pool_tree, pool_tree_matchers):
    for matcher in pool_tree_matchers:
        if isinstance(matcher, str):
            if pool_tree == matcher:
                return True
        elif isinstance(matcher, re.Pattern):
            if matcher.match(pool_tree):
                return True
        else:
            raise RuntimeError("Unsupported matcher {}".format(matcher))
    return False


def run_check_impl(yt_client,
                   logger,
                   options,
                   states,
                   skip_alert_types=(),
                   include_alert_types=(),
                   skip_pool_trees=(),
                   include_pool_trees=()):
    path = "//sys/scheduler/@alerts"
    logger.debug("Config: skip_alert_types: {}, include_alert_types: {}, skip_pool_trees: {}, include_pool_trees: {}"
                 .format(skip_alert_types, include_alert_types, skip_pool_trees, include_pool_trees))

    if yt_client.exists(path):
        alerts = []
        raw_alerts = yt_client.get(path)
        for alert in raw_alerts:
            try:
                alert_type = alert["attributes"]["alert_type"]
            except KeyError:
                # Fail only main check on alert type missing.
                if include_alert_types or include_pool_trees:
                    continue
                raise

            if alert_type in skip_alert_types or (include_alert_types and alert_type not in include_alert_types):
                continue
            if alert_type == "update_fair_share":
                pool_trees = get_pool_trees(alert)
                if any(  # noqa
                       map(lambda pool_tree:  # noqa
                               not match_pool_tree(pool_tree, skip_pool_trees) and  # noqa
                               (not include_pool_trees or match_pool_tree(pool_tree, include_pool_trees)),  # noqa
                           pool_trees)  # noqa
                ):   # noqa
                    alerts.append(alert)
            else:
                alerts.append(alert)
        if len(alerts) > 0:
            logger.info("Path {} contains the following alerts: {}".format(path, alerts))
            return states.UNAVAILABLE_STATE, str(alerts)
        logger.info("No alerts matching config found at path {} ".format(path))
    else:
        logger.info("{} path does not exist".format(path))

    return states.FULLY_AVAILABLE_STATE
