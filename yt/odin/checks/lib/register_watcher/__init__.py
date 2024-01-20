from yt.common import datetime_to_string, date_string_to_timestamp

from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta


STATE_TABLE_PATH = "//sys/admin/odin/register_watcher"
STATE_TABLE_SCHEMA = [
    {
        "name": "fqdn",
        "type": "string",
        "sort_order": "ascending",
    },
    {
        "name": "register_time",
        "type": "int64",
        "sort_order": "ascending",
    },
    {
        "name": "reason",
        "type": "string",
    },
]

WATCH_PERIOD_DAYS = 3
WATCH_THRESHOLD = int((datetime.utcnow() - timedelta(days=WATCH_PERIOD_DAYS)).strftime('%s'))


def run_check_impl(yt_client, logger, options, states, node_tag_filter=["internal", "nirvana"]):
    if not yt_client.exists(STATE_TABLE_PATH):
        yt_client.create("table", STATE_TABLE_PATH, attributes={
            "schema": STATE_TABLE_SCHEMA,
            "dynamic": True,
        })
        yt_client.mount_table(STATE_TABLE_PATH, sync=True)
    tablet_state = yt_client.get("{}/@tablet_state".format(STATE_TABLE_PATH))
    if tablet_state != "mounted":
        message = "{}/@tablet_state is {} but should be mounted".format(STATE_TABLE_PATH, tablet_state)
        logger.error(message)
        return states.UNAVAILABLE_STATE, message

    def is_timestamp_fresh_enough(timestamp):
        return True if timestamp >= WATCH_THRESHOLD else False

    def load_state():
        state = defaultdict(list)
        if yt_client.exists(STATE_TABLE_PATH):
            for k in yt_client.select_rows("fqdn, register_time, reason FROM [{}]".format(STATE_TABLE_PATH)):
                if is_timestamp_fresh_enough(k["register_time"]):
                    if k["reason"] is None:
                        state[k["fqdn"]].append(k["register_time"])
        return state

    def save_state(state):
        rows = []
        for node, register_times_unix in state.items():
            for register_time_unix in register_times_unix:
                if node in saved_state and register_time_unix in saved_state[node]:
                    continue
                rows.append({"fqdn": node, "register_time": register_time_unix})
        if rows:
            yt_client.insert_rows(STATE_TABLE_PATH, rows)

    state = load_state()
    saved_state = deepcopy(state)
    attributes = ["tags", "register_time"]
    cluster_nodes = yt_client.list("//sys/cluster_nodes", attributes=attributes, read_from="cache")
    nodes = {str(n): n.attributes for n in cluster_nodes
             if any(set(node_tag_filter) & set(n.attributes.get('tags', [])))}
    raw_alerts = defaultdict(list)

    for node, attr in nodes.items():
        timestamp = attr.get("register_time", None)
        if not timestamp:
            continue
        unixstamp = date_string_to_timestamp(timestamp)

        if is_timestamp_fresh_enough(unixstamp):
            fqdn = node.split(":")[0]
            # Registrations with the same timestamps will not be detected. Sorry, superfast registrations.
            if unixstamp not in state[fqdn]:
                state[fqdn].append(unixstamp)

            if len(state[fqdn]) >= options["crit_threshold"]:
                raw_alerts["CRIT"].append((fqdn, len(state[fqdn])))
            elif len(state[fqdn]) >= options["warn_threshold"]:
                raw_alerts["WARN"].append((fqdn, len(state[fqdn])))

    save_state(state)

    if len(raw_alerts["CRIT"]) > 0:
        return_state = states.UNAVAILABLE_STATE
    elif len(raw_alerts["WARN"]) > 0:
        return_state = states.PARTIALLY_AVAILABLE_STATE
    else:
        return states.FULLY_AVAILABLE_STATE, "OK"

    alerts = {}
    message = ""
    top = None
    for t in ["CRIT", "WARN"]:
        if t not in raw_alerts or not raw_alerts[t]:
            continue
        alerts[t] = {}
        for fqdn, count in raw_alerts[t]:
            if count not in alerts[t]:
                alerts[t][count] = []
            alerts[t][count].append(fqdn)

        line = "{}: ".format(t)
        for k in sorted(alerts[t].keys(), reverse=True):
            if top is None:
                top = alerts[t][k][0]
            line += "{}: {} ".format(k, alerts[t][k])
        if message:
            message += " "
        message += line.strip()

    logger.info(message[:1024])
    logger.info("Sample of top by register rate node: {} register times {}".format(
        top, [datetime_to_string(datetime.utcfromtimestamp(k)) for k in sorted(state[top], reverse=True)][:42])
    )
    logger.info("Review or clean register timestamps at {}@{} via yt-admin/register_watcher_helper.py"
                .format(STATE_TABLE_PATH, options["cluster_name"]))
    logger.info("Timestamps older than {} days are skipped from computing CRIT state".format(WATCH_PERIOD_DAYS))
    return return_state, message[:1024]
