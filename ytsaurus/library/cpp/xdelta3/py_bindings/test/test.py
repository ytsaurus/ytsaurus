# -*- coding: utf-8 -*-
import unittest

from yt.xdelta_aggregate_column.bindings import StateEncoder, State, XDeltaCodec


class StatesTestCases(unittest.TestCase):
    def test_error_state(self):
        en = StateEncoder(None)
        st = State(en.create_error_state(2))
        self.assertTrue(st.type == st.ERROR_TYPE)
        self.assertTrue(st.has_error_code)
        self.assertTrue(st.error_code == 2)

    def test_base_state(self):
        en = StateEncoder(None)
        base = b"1234"
        st = State(en.create_base_state(base))
        self.assertTrue(st.type == st.BASE_TYPE)
        self.assertTrue(st.payload_size == len(base))

    def test_patch_state(self):
        en = StateEncoder(None)
        base = b"1234"
        state = b"12356"
        st = State(en.create_patch_state((base, state)))
        self.assertTrue(st.type == st.PATCH_TYPE)
        self.assertTrue(st.payload_size > 0)

    def test_codec(self):
        xd = XDeltaCodec(None)
        base = b"123456"
        state = b"654321"
        patch = xd.compute_patch((base, state))
        self.assertTrue(len(patch))
        state1 = xd.apply_patch((base, patch, len(state)))
        self.assertTrue(len(state1))
        self.assertTrue(state1 == state)
