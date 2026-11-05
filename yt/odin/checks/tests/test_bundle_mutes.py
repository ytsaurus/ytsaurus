from yt.common import datetime_to_string
from yt_odin_checks.lib.bundle_mutes import (
    BUNDLES_PATH,
    MUTE_REASON_HINT,
    STATE_PATH,
    run_check_impl,
)

from mock import Mock

from datetime import datetime, timedelta


class _States:
    FULLY_AVAILABLE_STATE = 1.0
    PARTIALLY_AVAILABLE_STATE = 0.5
    UNAVAILABLE_STATE = 0.0


class _Bundle:
    def __init__(self, attributes):
        self.attributes = attributes


class _YtClient:
    def __init__(self, bundles, state=None):
        self.bundles = {
            bundle: _Bundle(attributes)
            for bundle, attributes in bundles.items()
        }
        self.state = state
        self.set_paths = []

    def exists(self, path):
        assert path == STATE_PATH
        return self.state is not None

    def get(self, path, attributes=None):
        if path == BUNDLES_PATH:
            assert attributes is not None
            return self.bundles
        if path == STATE_PATH:
            return self.state
        raise AssertionError(f"Unexpected get path {path}")

    def create(self, node_type, path, ignore_existing=False):
        assert node_type == "document"
        assert path == STATE_PATH
        assert ignore_existing
        if self.state is None:
            self.state = {}

    def set(self, path, value):
        self.set_paths.append(path)
        if path == STATE_PATH:
            self.state = value
            return

        prefix = BUNDLES_PATH + "/"
        assert path.startswith(prefix)
        bundle, attribute = path[len(prefix):].split("/@", 1)
        self.bundles[bundle].attributes[attribute] = value


NOW = datetime(2026, 8, 26, 10, 0)
DEFAULT_OPTIONS = {
    "reasonless_grace_period_seconds": 30 * 60,
    "expiry_warning_period_seconds": 0,
    "max_mute_duration_seconds": 30 * 24 * 60 * 60,
}


def _run_check(yt_client, now=NOW, options=None, logger=None):
    return run_check_impl(
        yt_client,
        logger or Mock(),
        options or DEFAULT_OPTIONS,
        _States,
        now=now,
    )


def _reason(**kwargs):
    reason = {
        "attribute": "mute_tablet_cells_check",
        "ticket": "YT-12345",
        "author": "ifsmirnov",
        "finish_time": datetime_to_string(NOW + timedelta(days=1)),
    }
    reason.update(kwargs)
    return reason


def test_reasonless_mute_uses_grace_period():
    yt_client = _YtClient({
        "default": {
            "mute_tablet_cells_check": True,
        },
    })

    assert _run_check(yt_client)[0] == _States.FULLY_AVAILABLE_STATE
    assert yt_client.state == {
        "bundles": {
            "default": {
                "mute_tablet_cells_check": {
                    "first_seen": datetime_to_string(NOW),
                    "last_seen": datetime_to_string(NOW),
                },
            },
        },
    }

    result = _run_check(yt_client, now=NOW + timedelta(minutes=31))
    assert result[0] == _States.UNAVAILABLE_STATE
    assert "valid reason is missing" in result[1]


def test_violations_are_logged_separately_and_joined_in_description():
    first_seen = NOW - timedelta(hours=1)
    yt_client = _YtClient(
        {
            "default": {
                "mute_tablet_cells_check": True,
                "mute_tablet_cell_snapshots_check": True,
            },
        },
        state={
            "bundles": {
                "default": {
                    "mute_tablet_cells_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                    "mute_tablet_cell_snapshots_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                },
            },
        },
    )
    logger = Mock()

    result = _run_check(yt_client, logger=logger)

    assert result[0] == _States.UNAVAILABLE_STATE
    assert result[1].split("; ") == [
        (
            "Unjustified bundle mutes: "
            "default/mute_tablet_cell_snapshots_check: active since "
            f"{datetime_to_string(first_seen)}, valid reason is missing"
        ),
        (
            "default/mute_tablet_cells_check: active since "
            f"{datetime_to_string(first_seen)}, valid reason is missing"
        ),
    ]
    assert logger.error.call_count == 3
    assert logger.error.call_args_list[-1].args == (MUTE_REASON_HINT,)


def test_duration_is_normalized_only_once():
    reason = _reason(duration="2h")
    reason.pop("finish_time")
    yt_client = _YtClient({
        "default": {
            "mute_tablet_cells_check": True,
            "mute_reasons": [reason],
        },
    })

    assert _run_check(yt_client)[0] == _States.FULLY_AVAILABLE_STATE
    normalized_reason = yt_client.bundles["default"].attributes["mute_reasons"][0]
    assert normalized_reason["start_time"] == datetime_to_string(NOW)
    assert normalized_reason["finish_time"] == datetime_to_string(NOW + timedelta(hours=2))
    assert "duration" not in normalized_reason

    assert _run_check(yt_client, now=NOW + timedelta(minutes=1))[0] == _States.FULLY_AVAILABLE_STATE
    reason_write_paths = [
        path
        for path in yt_client.set_paths
        if path.endswith("/@mute_reasons")
    ]
    assert len(reason_write_paths) == 1


def test_duration_renews_maximum_mute_duration():
    first_seen = NOW - timedelta(days=10)
    reason = _reason(duration="2h")
    reason.pop("finish_time")
    yt_client = _YtClient(
        {
            "default": {
                "mute_tablet_cells_check": True,
                "mute_reasons": [reason],
            },
        },
        state={
            "bundles": {
                "default": {
                    "mute_tablet_cells_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                },
            },
        },
    )
    options = dict(DEFAULT_OPTIONS, max_mute_duration_seconds=2 * 60 * 60)

    assert _run_check(yt_client, options=options)[0] == _States.FULLY_AVAILABLE_STATE
    normalized_reason = yt_client.bundles["default"].attributes["mute_reasons"][0]
    assert normalized_reason["start_time"] == datetime_to_string(NOW)
    assert normalized_reason["finish_time"] == datetime_to_string(NOW + timedelta(hours=2))

    renewed_at = NOW + timedelta(hours=1)
    renewed_reason = dict(normalized_reason, duration="2h")
    renewed_reason.pop("finish_time")
    yt_client.bundles["default"].attributes["mute_reasons"] = [renewed_reason]

    assert (
        _run_check(yt_client, now=renewed_at, options=options)[0]
        == _States.FULLY_AVAILABLE_STATE
    )
    normalized_reason = yt_client.bundles["default"].attributes["mute_reasons"][0]
    assert normalized_reason["start_time"] == datetime_to_string(renewed_at)
    assert normalized_reason["finish_time"] == datetime_to_string(
        renewed_at + timedelta(hours=2)
    )
    assert yt_client.state["bundles"]["default"]["mute_tablet_cells_check"][
        "first_seen"
    ] == datetime_to_string(first_seen)


def test_dry_run_logs_sets_without_mutating_cypress():
    reason = _reason(duration="2h")
    reason.pop("finish_time")
    yt_client = _YtClient({
        "default": {
            "mute_tablet_cells_check": True,
            "mute_reasons": [reason],
        },
    })
    logger = Mock()
    options = dict(DEFAULT_OPTIONS, dry_run=True)

    assert (
        _run_check(yt_client, options=options, logger=logger)[0]
        == _States.FULLY_AVAILABLE_STATE
    )
    assert yt_client.set_paths == []
    assert yt_client.state is None

    logged_sets = [
        call.args
        for call in logger.info.call_args_list
        if call.args and call.args[0].startswith("Dry run: would set path")
    ]
    assert logged_sets == [
        (
            "Dry run: would set path %s to value %r",
            f"{BUNDLES_PATH}/default/@mute_reasons",
            [{
                "attribute": "mute_tablet_cells_check",
                "ticket": "YT-12345",
                "author": "ifsmirnov",
                "start_time": datetime_to_string(NOW),
                "finish_time": datetime_to_string(NOW + timedelta(hours=2)),
            }],
        ),
        (
            "Dry run: would set path %s to value %r",
            STATE_PATH,
            {
                "bundles": {
                    "default": {
                        "mute_tablet_cells_check": {
                            "first_seen": datetime_to_string(NOW),
                            "last_seen": datetime_to_string(NOW),
                        },
                    },
                },
            },
        ),
    ]


def test_date_finish_time_expires_at_2359_msk():
    first_seen = datetime(2026, 8, 26, 19, 0)
    yt_client = _YtClient(
        {
            "default": {
                "mute_tablet_cells_check": True,
                "mute_reasons": [_reason(finish_time="2026-08-26")],
            },
        },
        state={
            "bundles": {
                "default": {
                    "mute_tablet_cells_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                },
            },
        },
    )

    assert (
        _run_check(yt_client, now=datetime(2026, 8, 26, 20, 58))[0]
        == _States.FULLY_AVAILABLE_STATE
    )
    assert (
        _run_check(yt_client, now=datetime(2026, 8, 26, 20, 59))[0]
        == _States.UNAVAILABLE_STATE
    )


def test_timestamp_without_timezone_is_interpreted_as_msk():
    first_seen = datetime(2026, 8, 26, 19, 0)
    yt_client = _YtClient(
        {
            "default": {
                "mute_tablet_cells_check": True,
                "mute_reasons": [_reason(finish_time="2026-08-26T23:59:00")],
            },
        },
        state={
            "bundles": {
                "default": {
                    "mute_tablet_cells_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                },
            },
        },
    )

    assert (
        _run_check(yt_client, now=datetime(2026, 8, 26, 20, 58))[0]
        == _States.FULLY_AVAILABLE_STATE
    )
    assert (
        _run_check(yt_client, now=datetime(2026, 8, 26, 20, 59))[0]
        == _States.UNAVAILABLE_STATE
    )


def test_wildcard_reason_covers_disabled_bundle_controller_option():
    wildcard_reason = _reason(attribute=None)
    wildcard_reason.pop("attribute")
    yt_client = _YtClient({
        "default": {
            "enable_bundle_controller": True,
            "enable_system_account_management": False,
            "mute_reasons": [wildcard_reason],
        },
    })

    assert _run_check(yt_client)[0] == _States.FULLY_AVAILABLE_STATE


def test_disabled_option_is_ignored_when_bundle_controller_is_disabled():
    yt_client = _YtClient({
        "default": {
            "enable_bundle_controller": False,
            "enable_system_account_management": False,
        },
    })

    assert _run_check(yt_client)[0] == _States.FULLY_AVAILABLE_STATE
    assert yt_client.state == {"bundles": {}}


def test_unknown_reason_field_does_not_turn_reason_into_wildcard():
    reason = _reason()
    reason["atribute"] = reason.pop("attribute")
    first_seen = NOW - timedelta(hours=1)
    yt_client = _YtClient(
        {
            "default": {
                "mute_tablet_cells_check": True,
                "mute_reasons": [reason],
            },
        },
        state={
            "bundles": {
                "default": {
                    "mute_tablet_cells_check": {
                        "first_seen": datetime_to_string(first_seen),
                        "last_seen": datetime_to_string(first_seen),
                    },
                },
            },
        },
    )

    result = _run_check(yt_client)
    assert result[0] == _States.UNAVAILABLE_STATE
    assert (
        "default/@mute_reasons: entry 0 has unknown fields ['atribute']"
        in result[1]
    )


def test_validation_error_logs_hint_as_last_warning():
    logger = Mock()
    yt_client = _YtClient({
        "default": {
            "mute_reasons": [{
                "ticket": "YT-12345",
                "author": "ifsmirnov",
            }],
        },
    })

    result = _run_check(yt_client, logger=logger)

    assert result[0] == _States.PARTIALLY_AVAILABLE_STATE
    assert logger.warning.call_args_list[0].args == (
        "default/@mute_reasons: entry 0 must contain exactly one of finish_time and duration",
    )
    assert logger.warning.call_args_list[-1].args == (MUTE_REASON_HINT,)


def test_unmuted_attribute_is_removed_from_state():
    yt_client = _YtClient({
        "default": {
            "mute_tablet_cells_check": True,
        },
    })
    assert _run_check(yt_client)[0] == _States.FULLY_AVAILABLE_STATE

    yt_client.bundles["default"].attributes["mute_tablet_cells_check"] = False
    assert (
        _run_check(yt_client, now=NOW + timedelta(minutes=1))[0]
        == _States.FULLY_AVAILABLE_STATE
    )
    assert yt_client.state == {"bundles": {}}
