WM = {"hit": 0, "action": 0}


def test_hit_stores_payload_and_schedules_timer(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)
    msg = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=1000,
    )

    with harness.processing([msg], watermarks=WM) as r:
        assert r.external_state("/join-state", key)["hit_payload"] == "p1"
        assert len(r.timers) == 1
        assert r.timers[0].trigger_timestamp == 1010  # hit_time + 10s


def test_action_show_stores_show_time(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)
    msg = harness.build_message(
        "action",
        key=key,
        hit_id="h1",
        hit_time=1000,
        action_time=1002,
        is_click=False,
        event_timestamp=1002,
    )

    with harness.processing([msg], watermarks=WM) as r:
        assert r.external_state("/join-state", key)["show_time"] == 1002
        assert len(r.timers) == 1


def test_action_click_stores_click_time(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)
    msg = harness.build_message(
        "action",
        key=key,
        hit_id="h1",
        hit_time=1000,
        action_time=1005,
        is_click=True,
        event_timestamp=1005,
    )

    with harness.processing([msg], watermarks=WM) as r:
        assert r.external_state("/join-state", key)["click_time"] == 1005


def test_late_message_dropped(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)
    msg = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=500,
    )

    with harness.processing([msg], watermarks={"hit": 600, "action": 600}) as r:
        assert r.external_state_size("/join-state") == 0
        assert len(r.timers) == 0


def test_message_out_of_wait_range_dropped(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)
    # event_timestamp >= hit_time + 10s → dropped
    msg = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=1010,
    )

    with harness.processing([msg], watermarks=WM) as r:
        assert r.external_state_size("/join-state") == 0


def test_timer_emits_joined_action_when_show_present(harness):
    key = harness.build_key(hit_id="h1", hit_time=1000)

    # Step 1: Hit
    hit = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=1000,
    )
    with harness.processing([hit], watermarks=WM) as r1:
        pass

    # Step 2: Show action
    show = harness.build_message(
        "action",
        key=key,
        hit_id="h1",
        hit_time=1000,
        action_time=1002,
        is_click=False,
        event_timestamp=1002,
    )
    with harness.processing(
        [show],
        watermarks=WM,
        external_states={"/join-state": {key: r1.external_state_raw("/join-state", key)}},
    ) as r2:
        pass

    # Step 3: Timer fires
    timer = harness.build_timer(key=key, trigger_timestamp=1010, stream_id="timer", event_timestamp=1000)
    with harness.processing(
        timers=[timer],
        external_states={"/join-state": {key: r2.external_state_raw("/join-state", key)}},
    ) as r3:
        assert len(r3.messages) == 1
        out = r3.messages[0]
        assert out.payload["hit_id"] == "h1"
        assert out.payload["hit_payload"] == "p1"
        assert out.payload["show_time"] == 1002
        assert out.payload["is_click"] is False
        assert out.payload["click_time"] == 0
        assert r3.external_state_is_reset("/join-state", key)


def test_full_join_flow_with_click(harness):
    """Hit -> Show -> Click -> Timer -> output with is_click=True"""
    key = harness.build_key(hit_id="h1", hit_time=1000)

    # Step 1: Hit
    hit = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=1000,
    )
    with harness.processing([hit], watermarks=WM) as r1:
        pass

    # Step 2: Show action
    show = harness.build_message(
        "action",
        key=key,
        hit_id="h1",
        hit_time=1000,
        action_time=1002,
        is_click=False,
        event_timestamp=1002,
    )
    with harness.processing(
        [show],
        watermarks=WM,
        external_states={"/join-state": {key: r1.external_state_raw("/join-state", key)}},
    ) as r2:
        pass

    # Step 3: Click action
    click = harness.build_message(
        "action",
        key=key,
        hit_id="h1",
        hit_time=1000,
        action_time=1005,
        is_click=True,
        event_timestamp=1005,
    )
    with harness.processing(
        [click],
        watermarks=WM,
        external_states={"/join-state": {key: r2.external_state_raw("/join-state", key)}},
    ) as r3:
        pass

    # Step 4: Timer fires -> output
    timer = harness.build_timer(key=key, trigger_timestamp=1010, stream_id="timer", event_timestamp=1000)
    with harness.processing(
        timers=[timer],
        external_states={"/join-state": {key: r3.external_state_raw("/join-state", key)}},
    ) as r4:
        assert len(r4.messages) == 1
        out = r4.messages[0]
        assert out.payload["hit_id"] == "h1"
        assert out.payload["is_click"] is True
        assert out.payload["click_time"] == 1005
        assert r4.external_state_is_reset("/join-state", key)


def test_timer_without_show_emits_nothing(harness):
    """If only a hit was received but no show, timer should emit nothing."""
    key = harness.build_key(hit_id="h1", hit_time=1000)

    # Only hit, no show
    hit = harness.build_message(
        "hit",
        key=key,
        hit_id="h1",
        hit_time=1000,
        hit_payload="p1",
        event_timestamp=1000,
    )
    with harness.processing([hit], watermarks=WM) as r1:
        pass

    # Timer fires but show_time is 0 → no output
    timer = harness.build_timer(key=key, trigger_timestamp=1010, stream_id="timer", event_timestamp=1000)
    with harness.processing(
        timers=[timer],
        external_states={"/join-state": {key: r1.external_state_raw("/join-state", key)}},
    ) as r2:
        assert len(r2.messages) == 0
        assert r2.external_state_is_reset("/join-state", key)
