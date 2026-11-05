from yt.common import datetime_to_string, utcnow

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple


BUNDLES_PATH: str = "//sys/tablet_cell_bundles"
STATE_PATH: str = "//sys/admin/odin/bundle_mutes"
MUTE_REASONS_ATTRIBUTE: str = "mute_reasons"

MUTE_ATTRIBUTES: Tuple[str, ...] = (
    "mute_tablet_cells_check",
    "mute_tablet_cell_snapshots_check",
    "mute_tablet_cell_gossip_check",
)

BUNDLE_CONTROLLER_ATTRIBUTE: str = "enable_bundle_controller"
ENABLE_ATTRIBUTES: Tuple[str, ...] = (
    "enable_instance_allocation",
    "enable_tablet_cell_management",
    "enable_node_tag_filter_management",
    "enable_tablet_node_dynamic_config",
    "enable_rpc_proxy_management",
    "enable_system_account_management",
    "enable_resource_limits_management",
)

CONTROLLED_ATTRIBUTES: Set[str] = set(MUTE_ATTRIBUTES) | set(ENABLE_ATTRIBUTES)

DEFAULT_REASONLESS_GRACE_PERIOD_SECONDS: int = 30 * 60
DEFAULT_EXPIRY_WARNING_PERIOD_SECONDS: int = 24 * 60 * 60
DEFAULT_MAX_MUTE_DURATION_SECONDS: int = 30 * 24 * 60 * 60

MAX_ISSUES_IN_DESCRIPTION: int = 20
MUTE_REASON_HINT: str = (
    "Set //sys/tablet_cell_bundles/<bundle>/@mute_reasons, for example: "
    "[{attribute=\"mute_tablet_cells_check\";ticket=\"YT-12345\";"
    "author=\"<login>\";duration=\"2h\";}]"
)

_ALLOWED_REASON_FIELDS = {
    "attribute",
    "ticket",
    "start_time",
    "finish_time",
    "duration",
    "author",
    "reason",
}
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DURATION_COMPONENT_PATTERN = re.compile(r"(\d+)([wdhms])")
_MSK_TIMEZONE = timezone(timedelta(hours=3))
_DURATION_UNIT_SECONDS = {
    "w": 7 * 24 * 60 * 60,
    "d": 24 * 60 * 60,
    "h": 60 * 60,
    "m": 60,
    "s": 1,
}


class _InvalidStateError(ValueError):
    pass


@dataclass(frozen=True)
class _MuteReason:
    attribute: Optional[str]
    start_time: Optional[datetime]
    finish_time: datetime


def _parse_time(value: Any, allow_date: bool) -> datetime:
    if not isinstance(value, str):
        raise ValueError("expected a string")

    if _DATE_PATTERN.fullmatch(value):
        if not allow_date:
            raise ValueError("expected a full timestamp")
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(
            hour=23,
            minute=59,
            tzinfo=_MSK_TIMEZONE,
        )
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    iso_value = value
    if value.endswith("Z"):
        iso_value = value[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(iso_value)
    except ValueError as error:
        raise ValueError("expected an ISO 8601 timestamp or YYYY-MM-DD") from error

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_MSK_TIMEZONE)

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_duration(value: Any) -> timedelta:
    if not isinstance(value, str) or not value:
        raise ValueError("expected a non-empty duration string")

    total_seconds = 0
    position = 0
    for match in _DURATION_COMPONENT_PATTERN.finditer(value):
        if match.start() != position:
            raise ValueError("expected a duration like 30m, 2h or 1d12h")
        total_seconds += int(match.group(1)) * _DURATION_UNIT_SECONDS[match.group(2)]
        position = match.end()

    if position != len(value) or total_seconds <= 0:
        raise ValueError("expected a positive duration like 30m, 2h or 1d12h")

    return timedelta(seconds=total_seconds)


def _normalize_and_validate_reasons(
    raw_reasons: Any,
    now: datetime,
) -> Tuple[List[Any], List[_MuteReason], List[str], bool]:
    if raw_reasons is None:
        return [], [], [], False
    if not isinstance(raw_reasons, list):
        return [], [], ["must be a list"], False

    normalized_reasons: List[Any] = list(raw_reasons)
    valid_reasons: List[_MuteReason] = []
    errors: List[str] = []
    changed = False

    for index, raw_reason in enumerate(raw_reasons):
        prefix = f"entry {index}"
        if not isinstance(raw_reason, dict):
            errors.append(f"{prefix} must be a map")
            continue

        unknown_fields = sorted(set(raw_reason) - _ALLOWED_REASON_FIELDS)
        if unknown_fields:
            errors.append(f"{prefix} has unknown fields {unknown_fields}")
            continue

        invalid_required_fields = [
            field
            for field in ("ticket", "author")
            if not isinstance(raw_reason.get(field), str) or not raw_reason[field].strip()
        ]
        if invalid_required_fields:
            errors.append(f"{prefix} has invalid required fields {invalid_required_fields}")
            continue

        if "reason" in raw_reason and not isinstance(raw_reason["reason"], str):
            errors.append(f"{prefix} field 'reason' must be a string")
            continue

        attribute = None
        if "attribute" in raw_reason:
            attribute = raw_reason["attribute"]
            if not isinstance(attribute, str) or attribute not in CONTROLLED_ATTRIBUTES:
                errors.append(f"{prefix} has unknown attribute {attribute!r}")
                continue

        time_field_count = int("finish_time" in raw_reason) + int("duration" in raw_reason)
        if time_field_count != 1:
            errors.append(f"{prefix} must contain exactly one of finish_time and duration")
            continue

        start_time = None
        if "duration" in raw_reason:
            try:
                finish_time = now + _parse_duration(raw_reason["duration"])
            except (OverflowError, ValueError) as error:
                errors.append(f"{prefix} has invalid duration: {error}")
                continue

            start_time = now
            normalized_reason = dict(raw_reason)
            normalized_reason.pop("duration")
            normalized_reason["start_time"] = datetime_to_string(start_time)
            normalized_reason["finish_time"] = datetime_to_string(finish_time)
            normalized_reasons[index] = normalized_reason
            changed = True
        else:
            try:
                finish_time = _parse_time(raw_reason["finish_time"], allow_date=True)
            except ValueError as error:
                errors.append(f"{prefix} has invalid finish_time: {error}")
                continue

            if "start_time" in raw_reason:
                try:
                    start_time = _parse_time(raw_reason["start_time"], allow_date=False)
                except ValueError as error:
                    errors.append(f"{prefix} has invalid start_time: {error}")
                    continue
                if start_time > now:
                    errors.append(f"{prefix} has start_time in the future")
                    continue

        valid_reasons.append(_MuteReason(
            attribute=attribute,
            start_time=start_time,
            finish_time=finish_time,
        ))

    return normalized_reasons, valid_reasons, errors, changed


def _get_active_mutes(attributes: Mapping[str, Any]) -> Set[str]:
    active_mutes = {
        attribute
        for attribute in MUTE_ATTRIBUTES
        if bool(attributes.get(attribute, False))
    }

    if bool(attributes.get(BUNDLE_CONTROLLER_ATTRIBUTE, False)):
        active_mutes.update(
            attribute
            for attribute in ENABLE_ATTRIBUTES
            if not bool(attributes.get(attribute, True))
        )

    return active_mutes


def _load_previous_state(yt_client: Any) -> Mapping[str, Any]:
    if not yt_client.exists(STATE_PATH):
        return {}

    state = yt_client.get(STATE_PATH)
    if not isinstance(state, dict) or not isinstance(state.get("bundles"), dict):
        raise _InvalidStateError(f"Invalid state document {STATE_PATH}")

    bundles = state["bundles"]
    for bundle, bundle_state in bundles.items():
        if not isinstance(bundle_state, dict):
            raise _InvalidStateError(f"Invalid state for bundle {bundle!r}")
        for attribute, attribute_state in bundle_state.items():
            if not isinstance(attribute_state, dict) or "first_seen" not in attribute_state:
                raise _InvalidStateError(
                    f"Invalid state for bundle {bundle!r}, attribute {attribute!r}"
                )
            try:
                _parse_time(attribute_state["first_seen"], allow_date=False)
            except ValueError as error:
                raise _InvalidStateError(
                    f"Invalid first_seen for bundle {bundle!r}, attribute {attribute!r}: {error}"
                ) from error

    return bundles


def _reconcile_state(
    active_mutes: Mapping[str, Set[str]],
    previous_state: Mapping[str, Any],
    now: datetime,
) -> Tuple[Dict[str, Any], Dict[Tuple[str, str], datetime]]:
    bundles_state: Dict[str, Any] = {}
    first_seen_by_mute: Dict[Tuple[str, str], datetime] = {}
    now_string = datetime_to_string(now)

    for bundle in sorted(active_mutes):
        bundle_state: Dict[str, Any] = {}
        previous_bundle_state = previous_state.get(bundle, {})

        for attribute in sorted(active_mutes[bundle]):
            previous_attribute_state = previous_bundle_state.get(attribute)
            if previous_attribute_state is None:
                first_seen = now
                first_seen_string = now_string
            else:
                first_seen_string = previous_attribute_state["first_seen"]
                first_seen = _parse_time(first_seen_string, allow_date=False)

            bundle_state[attribute] = {
                "first_seen": first_seen_string,
                "last_seen": now_string,
            }
            first_seen_by_mute[(bundle, attribute)] = first_seen

        if bundle_state:
            bundles_state[bundle] = bundle_state

    return {"bundles": bundles_state}, first_seen_by_mute


def _set_value(
    yt_client: Any,
    logger: Any,
    path: str,
    value: Any,
    dry_run: bool,
) -> None:
    if dry_run:
        logger.info("Dry run: would set path %s to value %r", path, value)
        return

    yt_client.set(path, value)


def _save_state(
    yt_client: Any,
    logger: Any,
    state: Mapping[str, Any],
    dry_run: bool,
) -> None:
    if not dry_run and not yt_client.exists(STATE_PATH):
        yt_client.create("document", STATE_PATH, ignore_existing=True)
    _set_value(yt_client, logger, STATE_PATH, state, dry_run)


def _get_option_seconds(options: Mapping[str, Any], name: str, default: int) -> int:
    value = options.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"Option {name!r} must be a non-negative number")
    return int(value)


def _get_bool_option(options: Mapping[str, Any], name: str, default: bool) -> bool:
    value = options.get(name, default)
    if not isinstance(value, bool):
        raise ValueError(f"Option {name!r} must be a boolean")
    return value


def _format_duration(value: timedelta) -> str:
    total_seconds = int(value.total_seconds())
    for suffix, unit_seconds in (
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
    ):
        if total_seconds % unit_seconds == 0:
            return f"{total_seconds // unit_seconds}{suffix}"
    return f"{total_seconds}s"


def _find_covering_reason(
    reasons: Sequence[_MuteReason],
    attribute: str,
    first_seen: datetime,
    now: datetime,
    max_mute_duration: timedelta,
) -> Tuple[Optional[datetime], str]:
    matching_reasons = [
        reason
        for reason in reasons
        if reason.attribute is None or reason.attribute == attribute
    ]
    if not matching_reasons:
        return None, "valid reason is missing"

    unexpired_reasons = [reason for reason in matching_reasons if reason.finish_time > now]
    if not unexpired_reasons:
        last_finish_time = max(reason.finish_time for reason in matching_reasons)
        return None, f"reason expired at {datetime_to_string(last_finish_time)}"

    if max_mute_duration:
        unexpired_reasons = [
            reason
            for reason in unexpired_reasons
            if reason.finish_time <= (reason.start_time or first_seen) + max_mute_duration
        ]
        if not unexpired_reasons:
            return None, (
                "finish_time exceeds the maximum mute duration "
                f"{_format_duration(max_mute_duration)}"
            )

    return max(reason.finish_time for reason in unexpired_reasons), ""


def _build_description(prefix: str, issues: Sequence[str]) -> str:
    sampled_issues = list(issues[:MAX_ISSUES_IN_DESCRIPTION])
    description = f"{prefix}: " + "; ".join(sampled_issues)
    if len(issues) > len(sampled_issues):
        description += f"; and {len(issues) - len(sampled_issues)} more"
    return description


def run_check_impl(
    yt_client: Any,
    logger: Any,
    options: Mapping[str, Any],
    states: Any,
    now: Optional[datetime] = None,
) -> Tuple[float, str]:
    """Check that every active tablet cell bundle mute has a valid reason."""

    now = now or utcnow()
    reasonless_grace_period = timedelta(seconds=_get_option_seconds(
        options,
        "reasonless_grace_period_seconds",
        DEFAULT_REASONLESS_GRACE_PERIOD_SECONDS,
    ))
    expiry_warning_period = timedelta(seconds=_get_option_seconds(
        options,
        "expiry_warning_period_seconds",
        DEFAULT_EXPIRY_WARNING_PERIOD_SECONDS,
    ))
    max_mute_duration = timedelta(seconds=_get_option_seconds(
        options,
        "max_mute_duration_seconds",
        DEFAULT_MAX_MUTE_DURATION_SECONDS,
    ))
    dry_run = _get_bool_option(options, "dry_run", False)

    attributes_to_fetch = list(MUTE_ATTRIBUTES) + list(ENABLE_ATTRIBUTES) + [
        BUNDLE_CONTROLLER_ATTRIBUTE,
        MUTE_REASONS_ATTRIBUTE,
    ]
    bundles = yt_client.get(BUNDLES_PATH, attributes=attributes_to_fetch)

    active_mutes: Dict[str, Set[str]] = {}
    reasons_by_bundle: Dict[str, List[_MuteReason]] = {}
    metadata_errors: List[str] = []

    for bundle in sorted(bundles):
        attributes = bundles[bundle].attributes
        bundle_active_mutes = _get_active_mutes(attributes)
        if bundle_active_mutes:
            active_mutes[bundle] = bundle_active_mutes

        raw_reasons = attributes.get(MUTE_REASONS_ATTRIBUTE)
        normalized_reasons, valid_reasons, errors, changed = _normalize_and_validate_reasons(
            raw_reasons,
            now,
        )
        reasons_by_bundle[bundle] = valid_reasons
        metadata_errors.extend(
            f"{bundle}/@{MUTE_REASONS_ATTRIBUTE}: {error}"
            for error in errors
        )

        if changed:
            reasons_path = f"{BUNDLES_PATH}/{bundle}/@{MUTE_REASONS_ATTRIBUTE}"
            _set_value(yt_client, logger, reasons_path, normalized_reasons, dry_run)
            if not dry_run:
                logger.info("Normalized durations in %s", reasons_path)

    try:
        previous_state = _load_previous_state(yt_client)
        state, first_seen_by_mute = _reconcile_state(active_mutes, previous_state, now)
    except _InvalidStateError as error:
        logger.error(str(error))
        return states.UNAVAILABLE_STATE, str(error)

    _save_state(yt_client, logger, state, dry_run)

    violations: List[str] = []
    warnings: List[str] = list(metadata_errors)

    for bundle in sorted(active_mutes):
        for attribute in sorted(active_mutes[bundle]):
            first_seen = first_seen_by_mute[(bundle, attribute)]
            finish_time, failure_reason = _find_covering_reason(
                reasons_by_bundle[bundle],
                attribute,
                first_seen,
                now,
                max_mute_duration,
            )

            if finish_time is not None:
                if expiry_warning_period and finish_time - now <= expiry_warning_period:
                    warnings.append(
                        f"{bundle}/{attribute}: reason expires at "
                        f"{datetime_to_string(finish_time)}"
                    )
                continue

            mute_age = now - first_seen
            if mute_age < reasonless_grace_period:
                logger.warning(
                    "Bundle %s attribute %s has no covering reason, but is within grace period",
                    bundle,
                    attribute,
                )
                continue

            violations.append(
                f"{bundle}/{attribute}: active since {datetime_to_string(first_seen)}, "
                f"{failure_reason}"
            )

    if violations:
        errors = violations + metadata_errors
        description = _build_description("Unjustified bundle mutes", errors)
        for error in errors:
            logger.error(error)
        logger.error(MUTE_REASON_HINT)
        return states.UNAVAILABLE_STATE, description

    if warnings:
        description = _build_description("Bundle mute warnings", warnings)
        for warning in warnings:
            logger.warning(warning)
        if metadata_errors:
            logger.warning(MUTE_REASON_HINT)
        return states.PARTIALLY_AVAILABLE_STATE, description

    return states.FULLY_AVAILABLE_STATE, "OK"
