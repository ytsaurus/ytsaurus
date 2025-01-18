from yt.yt_sync.core.model.tablet_state import YtTabletState


class TestYtTabletState:
    def test_default(self):
        state = YtTabletState()
        assert state.is_not_set
        assert not state.is_mounted
        assert not state.is_unmounted
        assert not state.is_frozen

    def test_set_mounted(self):
        state = YtTabletState()
        state.set(YtTabletState.MOUNTED)
        assert not state.is_not_set
        assert state.is_mounted
        assert not state.is_unmounted
        assert not state.is_frozen

    def test_set_unmounted(self):
        state = YtTabletState()
        state.set(YtTabletState.UNMOUNTED)
        assert not state.is_not_set
        assert not state.is_mounted
        assert state.is_unmounted
        assert not state.is_frozen

    def test_set_frozen(self):
        state = YtTabletState()
        state.set(YtTabletState.FROZEN)
        assert not state.is_not_set
        assert not state.is_mounted
        assert not state.is_unmounted
        assert state.is_frozen

    def test_set_unknown(self):
        state = YtTabletState()
        state.set("_unknown_")
        assert state.is_not_set
        assert not state.is_mounted
        assert not state.is_unmounted
        assert not state.is_frozen
