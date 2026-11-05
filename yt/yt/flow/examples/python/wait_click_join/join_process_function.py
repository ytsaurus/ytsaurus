"""JoinProcessFunction: RowFunction that joins hit and action streams."""

import logging
import re

from yt.yt.flow.library.python.companion.computation import RowFunction

log = logging.getLogger(__name__)


def _parse_duration_seconds(duration_str):
    """Parse duration string like '10s', '1m', '1h' into seconds."""
    match = re.match(r"^(\d+)([smh])$", duration_str.lower())
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == "s":
            return value
        elif unit == "m":
            return value * 60
        elif unit == "h":
            return value * 3600
    raise ValueError(f"Cannot parse duration: {duration_str}")


class JoinProcessFunction(RowFunction):
    """Joins hit and action streams using external state and timers."""

    # [BEGIN on_message]
    def on_message(self, message, output, ctx):
        stream_id = message.stream_id

        if stream_id == "hit":
            hit_id = message.payload["hit_id"]
            hit_time = message.payload["hit_time"]
            hit_payload = message.payload["hit_payload"]
        elif stream_id == "action":
            hit_id = message.payload["hit_id"]
            hit_time = message.payload["hit_time"]
        else:
            raise ValueError(f"Unknown streamId: {stream_id}")

        log.debug("Process message (Key: %s, HitId: %s, HitTime: %s)", message.key, hit_id, hit_time)

        wait_time = _parse_duration_seconds(ctx.parameters["wait_for_actions"])
        max_time = hit_time + wait_time

        # All messages out of wait range would be dropped.
        if message.event_timestamp >= max_time:
            log.warning(
                "Message is out of wait range, skip (HitId: %s, HitTime: %s, MaxTime: %s)",
                hit_id,
                hit_time,
                max_time,
            )
            return

        # Drop late data.
        if message.event_timestamp < ctx.min_watermark:
            log.warning(
                "Late message, skip (HitId: %s, EventTimestamp: %s, InputEventWatermark: %s)",
                hit_id,
                message.event_timestamp,
                ctx.min_watermark,
            )
            return

        state = ctx.external_state("/join-state", message)
        builder = state.to_builder()

        if stream_id == "hit":
            log.debug("Process hit message (HitId: %s, HitPayload: %s)", hit_id, hit_payload)
            builder.set("hit_payload", hit_payload)
        else:
            action_time = message.payload["action_time"]
            is_click = message.payload["is_click"]
            log.debug("Process action message (HitId: %s, ActionTime: %s)", hit_id, action_time)
            if is_click:
                builder.set("click_time", action_time)
            else:
                builder.set("show_time", action_time)

        state.set(builder.finish())
        log.debug("Add timer for Key (MaxTime: %s, HitTime: %s)", max_time, hit_time)
        output.add_timer(max_time, hit_time)

    # [END on_message]

    # [BEGIN on_timer]
    def on_timer(self, timer, output, ctx):
        hit_id = timer.key["hit_id"]
        log.debug("Process timer (KeyHitId: %s, Timer: %s)", hit_id, timer)

        state = ctx.external_state("/join-state", timer)

        log.debug("Process state (KeyHitId: %s, State: %s)", hit_id, state)

        if timer.stream_id == "timer":
            show_time = state.get("show_time")
            hit_payload = state.get("hit_payload")

            if show_time is not None and hit_payload is not None and show_time != 0:
                log.debug("Click join: %s", state)
                hit_time = timer.key["hit_time"]
                click_time = state.get("click_time")

                builder = ctx.message_builder("joined_action")
                builder.set("hit_id", hit_id)
                builder.set("hit_time", hit_time)
                builder.set("show_time", show_time)
                builder.set("hit_payload", hit_payload)

                if click_time is not None and click_time != 0:
                    builder.set("is_click", True)
                    builder.set("click_time", click_time)
                else:
                    builder.set("is_click", False)
                    builder.set("click_time", 0)

                output.add_message(builder.finish())

            state.clear()
        else:
            raise ValueError(f"Unknown streamId: {timer.stream_id}")

    # [END on_timer]
