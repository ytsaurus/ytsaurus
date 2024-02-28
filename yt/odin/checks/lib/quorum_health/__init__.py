# coding=utf-8
import typing  # noqa
import logging
import requests
import concurrent.futures

from datetime import datetime  # noqa

from yt.wrapper import YtClient  # noqa
import yt.yson as yson

OK = 0
WARN = 1
CRIT = 2

DEFAULT_CONCURRENCY = 10


class Hydra(object):
    FOLLOWING_STATE = 'following'
    LEADING_STATE = 'leading'
    OFFLINE_STATE = 'offline'
    MAINTENANCE_STATE = 'maintenance'
    ACTIVE_STATES = [FOLLOWING_STATE, LEADING_STATE]

    hostname = None
    port = None
    _state = None

    @classmethod
    def from_hostname_port_string(cls, hostname_port):
        # type: (str) -> Hydra

        instance = cls()
        instance.hostname, instance.port = hostname_port.split(':')
        return instance

    def state(self):
        return self._state

    def set_state(self, state, logger):
        if state == Hydra.MAINTENANCE_STATE:
            logger.info('Maintenance found for {}:{}'.format(self.hostname, self.port))

        if self._state is None:
            self._state = state
            return

        # offline < maintenance < (leading or following)

        if state in self.ACTIVE_STATES:
            self._state = state
            return

        if state == self.MAINTENANCE_STATE and self._state not in self.ACTIVE_STATES:
            self._state = state
            return

        # else do nothing

    def __str__(self):
        return '{}:{}:{}'.format(self.hostname, self.port, self._state)

    def __repr__(self):
        return '{}:{}:{}'.format(self.hostname, self.port, self._state)


class Cell(object):
    def __init__(self, is_primary, cell_tag, masters):
        # type: (bool, str, typing.List[Master]) -> typing.NoReturn

        self.cell_tag = cell_tag
        self.is_primary = is_primary
        self.masters = masters

    def __str__(self):
        masters = [str(m) for m in self.masters]
        return 'cell_tag: {}, is_primary: {}, masters: {}'.format(self.cell_tag, self.is_primary, masters)


def configure_loggers():
    logging.getLogger("Yt").setLevel(logging.DEBUG)


def configure_timeout_and_retries(yt_client):
    yt_client.config['proxy']['request_timeout'] = 15000
    yt_client.config['proxy']['connect_timeout'] = 15000
    yt_client.config['proxy']['retries']['count'] = 2
    yt_client.config['proxy']['retries']['backoff']['policy'] = 'rounded_up_to_request_timeout'


def get_hydra_state(hydra, monitoring_port, logger):
    try:
        url = "http://{}:{}/orchid/monitoring/hydra/state".format(hydra.hostname, monitoring_port)
        result = requests.get(url, timeout=(5, 15))
        if result.status_code == 200:
            hydra.set_state(str(yson.loads(result.content)), logger)
            logger.info("Successfully read hydra state from {}:{} (state: {})".format(hydra.hostname, monitoring_port, hydra.state()))
        else:
            hydra.set_state(Hydra.OFFLINE_STATE, logger)
            logger.info("Failed to read hydra state from {}:{} (status code: {})".format(hydra.hostname, monitoring_port, result.status_code))
    except Exception as e:
        hydra.set_state(Hydra.OFFLINE_STATE, logger)
        logger.info("Failed to read hydra state from {}:{} ({})".format(hydra.hostname, monitoring_port, e))


def build_juggler_message(instances):
    # type: (typing.List[Hydra]) -> str
    msg = ''
    for i in instances:
        if i.state() not in [Hydra.LEADING_STATE, Hydra.FOLLOWING_STATE]:
            msg = '{} {}:{}'.format(msg, i.hostname, i.state())
    return msg


def build_cell_juggler_message(cell):
    # type: (Cell) -> str

    msg = 'Cell {} is unhealthy state:'.format(cell.cell_tag)
    return '{} {}'.format(msg, build_juggler_message(cell.masters))


def discover_clock(yt_client, options, logger):
    # type: (YtClient, logging.Logger) -> typing.List[Hydra] or None
    monitoring_port = options["monitoring_port"]
    concurrency = options.get("concurrency", DEFAULT_CONCURRENCY)
    path = '//sys/timestamp_providers'
    clock = []
    for clock_hostname_port in yt_client.list(path, attributes=["maintenance"]):
        clock_instance = Hydra.from_hostname_port_string(clock_hostname_port)
        if clock_hostname_port.attributes.get("maintenance", False):
            clock_instance.set_state(Hydra.MAINTENANCE_STATE, logger)
        clock.append(clock_instance)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = []
        for clock_instance in clock:
            futures.append(pool.submit(get_hydra_state, clock_instance, monitoring_port, logger))
        concurrent.futures.wait(futures)

    return clock


def discover_masters(yt_client, options, logger):
    # type: (YtClient, logging.Logger) -> typing.List[Cell] or None

    monitoring_port = options["monitoring_port"]
    concurrency = options.get("concurrency", DEFAULT_CONCURRENCY)

    discovery_requests = [
        {'parameters': {'path': '//sys/primary_masters', 'attributes': ['maintenance']}, 'command': 'get'},
        {'parameters': {'path': '//sys/@primary_cell_tag'}, 'command': 'get'},
        {'parameters': {'path': '//sys/secondary_masters', 'attributes': ['maintenance']}, 'command': 'get'},
    ]

    try:
        data = yt_client.execute_batch(discovery_requests)["results"]
    except Exception as error:
        logger.info('Can\'t discover masters with error: {}'.format(str(error)))
        return None

    primary_masters = data[0]['output']['value']
    primary_cell_tag = str(data[1]['output']['value'])
    secondary_masters = data[2]['output']['value']

    def cell_from_peers(cell_tag, peers):
        hydra_peers = []
        for item in peers.items():
            hostname_port = item[0]
            hydra_instance = Hydra.from_hostname_port_string(hostname_port)

            if item[1].attributes.get("maintenance", False):
                hydra_instance.set_state(Hydra.MAINTENANCE_STATE, logger)
            hydra_peers.append(hydra_instance)

        return Cell(cell_tag == primary_cell_tag, cell_tag, hydra_peers)

    cells = [cell_from_peers(primary_cell_tag, primary_masters)]
    for cell_tag in secondary_masters:
        cells.append(cell_from_peers(cell_tag, secondary_masters[cell_tag]))

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = []
        for cell in cells:
            for master in cell.masters:
                futures.append(pool.submit(get_hydra_state, master, monitoring_port, logger))
        concurrent.futures.wait(futures)

    return cells


def check_cell_health(instances, current_datetime, logger):
    # type: (Cell, datetime) -> int

    leading = 0
    following = 0
    in_maintenance = 0

    total = len(instances)

    for i in instances:
        if i.state() == Hydra.LEADING_STATE:
            leading += 1
        elif i.state() == Hydra.FOLLOWING_STATE:
            following += 1
        elif i.state() == Hydra.MAINTENANCE_STATE:
            in_maintenance += 1

    # Если есть leading, и все остальные following - возвращаем OK
    # Если есть leading, один in_maintenance и все остальные following - возвращаем WARN
    # Если нет leading - возвращаем CRIT
    # Если есть leading, и всего три мастера - возвращаем CRIT
    # Если есть leading, мастеров больше 3, но не работает following больше одного - возвращаем CRIT
    # Если есть leading, не работает один following и время между одиннадцатью вечера и десятью утра - возвращаем WARN
    # Во всех остальных случаях - возвращаем CRIT

    if leading == 1 and following == total - 1:
        return OK

    if leading == 1 and in_maintenance == 1 and following == total - 2:
        logger.info("One master is in maintenance for cell {}".format(instances))
        return WARN

    if leading != 1:
        logger.info("Number of leading masters is not equal to 1 for cell {}".format(instances))
        return CRIT

    if total == 3:
        logger.info("One master is missing out of three for cell {}".format(instances))
        return CRIT

    if total - leading - following > 1:
        logger.info("Multiple masters are missing for cell {}".format(instances))
        return CRIT

    logging.info("Unknown bad state: leading={}, following={}, in_maintenance={} for cell {}".format(
        leading, following, in_maintenance, instances))
    if current_datetime.hour >= 23 or current_datetime.hour <= 9:
        return WARN
    return CRIT


def get_cluster_cells_health(cells, current_date, logger):
    health = []
    juggler_message = None

    for cell in cells:
        health_state = check_cell_health(cell.masters, current_date, logger)
        health.append(health_state)
        if health_state != OK:
            if juggler_message is None:
                juggler_message = build_cell_juggler_message(cell)
            else:
                juggler_message = '{}, {}'.format(juggler_message, build_cell_juggler_message(cell))

    return health, juggler_message
