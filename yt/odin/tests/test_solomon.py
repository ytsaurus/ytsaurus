from yt_odin.storage.storage import OdinDBRecord
from yt_odin.webservice.solomon import (
    create_solomon_availability_sensors,
    create_solomon_duration_sensors,
    create_solomon_timed_out_sensors,
)


def _record(service="sort_result", state=1, duration=42, timestamp=1000, cluster="hume"):
    return OdinDBRecord(
        cluster=cluster,
        service=service,
        timestamp=timestamp,
        state=state,
        duration=duration,
        messages=None,
    )


def test_create_solomon_availability_sensors_emits_raw_state():
    records = [
        _record(service="sort_result", state=1),
        _record(service="map_result", state=0.5),
        _record(service="master", state=1),  # not opted in
    ]
    sensors = create_solomon_availability_sensors(records, {"sort_result", "map_result"})
    assert len(sensors) == 2
    by_check = {s["labels"]["check"]: s for s in sensors}
    assert set(by_check) == {"sort_result", "map_result"}
    # raw state is passed through unchanged, including fractions
    assert by_check["sort_result"]["value"] == 1
    assert by_check["map_result"]["value"] == 0.5
    for sensor in sensors:
        assert sensor["labels"]["sensor"] == "availability"
        assert sensor["labels"]["proxy"] == "hume"
        assert sensor["kind"] == "DGAUGE"
        assert sensor["ts"] == 1000


def test_create_solomon_availability_sensors_empty_selection():
    records = [_record(service="sort_result", state=1)]
    assert create_solomon_availability_sensors(records, set()) == []


def test_duration_sensors_still_work():
    sensors = create_solomon_duration_sensors([_record(duration=7)])
    assert len(sensors) == 1
    assert sensors[0]["labels"]["sensor"] == "duration"
    assert sensors[0]["value"] == 7
    # timed-out records (duration is None) produce no duration sensor
    assert create_solomon_duration_sensors([_record(duration=None)]) == []


def test_timed_out_sensors_still_work():
    assert create_solomon_timed_out_sensors([_record(duration=None)])[0]["value"] == 1
    assert create_solomon_timed_out_sensors([_record(duration=5)])[0]["value"] == 0
