def create_solomon_duration_sensors(records):
    sensors = []
    for record in records:
        if record.duration is not None:
            sensors.append(dict(
                labels=dict(
                    sensor="duration",
                    check=record.service,
                    proxy=record.cluster,
                ),
                ts=record.timestamp,
                kind="IGAUGE",
                value=record.duration,
            ))
    return sensors


def create_solomon_timed_out_sensors(records):
    sensors = []
    for record in records:
        value = 1 if record.duration is None else 0
        sensors.append(dict(
            labels=dict(
                sensor="timed_out",
                check=record.service,
                proxy=record.cluster,
            ),
            ts=record.timestamp,
            kind="IGAUGE",
            value=value,
        ))
    return sensors


def create_solomon_availability_sensors(records, availability_services):
    sensors = []
    for record in records:
        if record.service not in availability_services:
            continue
        # NB: emit the raw check state (0, 0.5, 1, ...); monium interprets it.
        # DGAUGE (not IGAUGE) because the state is fractional.
        sensors.append(dict(
            labels=dict(
                sensor="availability",
                check=record.service,
                proxy=record.cluster,
            ),
            ts=record.timestamp,
            kind="DGAUGE",
            value=record.state,
        ))
    return sensors
