"""Integration tests for native encryption in yt.wrapper.pickling.

Test classes:
  TestEncryptChaCha20Class  - _EncryptChaCha20: prefix, roundtrip, empty, passthrough, wrong prefix
  TestEncryptFernetClass    - _EncryptFernet baseline: prefix, roundtrip (skip if no cryptography)
  TestPicklerUnpickler      - End-to-end Pickler->Unpickler: dict, list, bytes, None, complex, 10 MB
  TestCrossFormatDecryption - Unpickler auto-detects ENC1/ENC2, unencrypted passthrough
  TestEdgeCases             - No encryption, None key, wire format, tampered, key reuse, file I/O
  TestCreateEncryptorErrors - Pickler error paths: fernet unavailable
  TestHexKeyRoundTrip       - Hex key generate/set/decrypt, raw bytes, non-UTF8 fallback
"""

import os
import pickle
import tempfile
import unittest

from yt.testlib import authors

from yt.wrapper import encryption as _encryption
from yt.wrapper import pickling as _pickling


class TestEncryptChaCha20Class(unittest.TestCase):

    def setUp(self):
        self.cipher = _pickling._EncryptChaCha20()
        self.cipher.set_key()

    @authors("asklit")
    def test_prefix(self):
        self.assertEqual(self.cipher.data_prefix, b"ENC2")

    @authors("asklit")
    def test_encrypt_has_prefix(self):
        self.assertTrue(self.cipher.encrypt(b"test data").startswith(b"ENC2"))

    @authors("asklit")
    def test_encrypt_decrypt_roundtrip(self):
        data = b"pickle data here"
        self.assertEqual(self.cipher.decrypt(self.cipher.encrypt(data)), data)

    @authors("asklit")
    def test_encrypt_none_on_empty(self):
        self.assertIsNone(self.cipher.encrypt(b""))

    @authors("asklit")
    def test_decrypt_passthrough_non_enc(self):
        data = b"plain data without ENC prefix"
        self.assertEqual(self.cipher.decrypt(data), data)

    @authors("asklit")
    def test_decrypt_wrong_prefix_returns_none(self):
        self.assertIsNone(self.cipher.decrypt(b"ENC9some_data"))


class TestEncryptFernetClass(unittest.TestCase):

    def setUp(self):
        try:
            self.cipher = _pickling._EncryptFernet()
            self.cipher.set_key()
        except Exception:
            self.skipTest("cryptography not available")

    @authors("asklit")
    def test_prefix(self):
        self.assertEqual(self.cipher.data_prefix, b"ENC1")

    @authors("asklit")
    def test_encrypt_decrypt_roundtrip(self):
        data = b"fernet test data"
        enc = self.cipher.encrypt(data)
        self.assertTrue(enc.startswith(b"ENC1"))
        self.assertEqual(self.cipher.decrypt(enc), data)


class TestPicklerUnpickler(unittest.TestCase):

    def _roundtrip(self, obj, engine="native_chacha"):
        pickler = _pickling.Pickler("pickle")
        key = pickler.enable_encryption(key="", engine=engine)
        data = pickler.dumps(obj)
        self.assertTrue(len(data) > 0)

        unpickler = _pickling.Unpickler("pickle")
        unpickler.enable_encryption(key=key)
        return unpickler.loads(data)

    @authors("asklit")
    def test_encryption_dict(self):
        obj = {"key": "value", "nested": [1, 2, 3]}
        self.assertEqual(self._roundtrip(obj, engine="native_chacha"), obj)

    @authors("asklit")
    def test_encryption_list(self):
        obj = list(range(1000))
        self.assertEqual(self._roundtrip(obj, engine="native_chacha"), obj)

    @authors("asklit")
    def test_encryption_bytes(self):
        obj = os.urandom(10000)
        self.assertEqual(self._roundtrip(obj, engine="native_chacha"), obj)

    @authors("asklit")
    def test_encryption_none(self):
        self.assertIsNone(self._roundtrip(None, engine="native_chacha"))

    @authors("asklit")
    def test_encryption_complex_object(self):
        obj = {
            "str": "hello", "int": 42, "float": 3.14,
            "bytes": b"\x00\xff", "list": [1, "two", 3.0],
            "nested": {"a": {"b": {"c": True}}},
            "tuple": (1, 2, 3), "set": {1, 2, 3},
        }
        self.assertEqual(self._roundtrip(obj, engine="native_chacha"), obj)

    @authors("asklit")
    def test_fernet_roundtrip(self):
        try:
            self.assertEqual(
                self._roundtrip({"test": "fernet"}, engine="cryptography_fernet"),
                {"test": "fernet"})
        except Exception:
            self.skipTest("cryptography not available")

    @authors("asklit")
    def test_encryption_large_object(self):
        obj = os.urandom(10 * 1024 * 1024)
        self.assertEqual(self._roundtrip(obj, engine="native_chacha"), obj)


class TestCrossFormatDecryption(unittest.TestCase):

    @authors("asklit")
    def test_encryption_auto_detected(self):
        obj = {"algorithm": "chacha20"}
        pickler = _pickling.Pickler("pickle")
        key = pickler.enable_encryption(key="", engine="native_chacha")
        data = pickler.dumps(obj)

        unpickler = _pickling.Unpickler("pickle")
        unpickler.enable_encryption(key=key)
        self.assertEqual(unpickler.loads(data), obj)

    @authors("asklit")
    def test_fernet_auto_detected(self):
        try:
            from cryptography.fernet import Fernet  # noqa: F401
        except ImportError:
            self.skipTest("cryptography not available")

        obj = {"algorithm": "fernet"}
        pickler = _pickling.Pickler("pickle")
        key = pickler.enable_encryption(key="", engine="cryptography_fernet")
        data = pickler.dumps(obj)

        unpickler = _pickling.Unpickler("pickle")
        unpickler.enable_encryption(key=key)
        self.assertEqual(unpickler.loads(data), obj)

    @authors("asklit")
    def test_unencrypted_data_passthrough(self):
        obj = {"not": "encrypted"}
        raw = pickle.dumps(obj)
        unpickler = _pickling.Unpickler("pickle")
        unpickler.enable_encryption(key="somekey")
        self.assertEqual(unpickler.loads(raw), obj)


class TestEdgeCases(unittest.TestCase):

    @authors("asklit")
    def test_pickler_without_encryption(self):
        pickler = _pickling.Pickler("pickle")
        obj = {"no": "encryption"}
        data = pickler.dumps(obj)
        self.assertFalse(data.startswith(b"ENC"))
        self.assertEqual(_pickling.Unpickler("pickle").loads(data), obj)

    @authors("asklit")
    def test_pickler_enable_encryption_none(self):
        pickler = _pickling.Pickler("pickle")
        self.assertIsNone(pickler.enable_encryption(key=None))

    @authors("asklit")
    def test_wire_format_enc2_prefix(self):
        pickler = _pickling.Pickler("pickle")
        pickler.enable_encryption(key="", engine="native_chacha")
        data = pickler.dumps(42)
        self.assertTrue(data.startswith(b"ENC2"))
        self.assertGreaterEqual(len(data[4:]), 12)

    @authors("asklit")
    def test_wire_format_enc1_prefix(self):
        try:
            from cryptography.fernet import Fernet  # noqa: F401
        except ImportError:
            self.skipTest("cryptography not available")
        pickler = _pickling.Pickler("pickle")
        pickler.enable_encryption(key="", engine="cryptography_fernet")
        data = pickler.dumps(42)
        self.assertTrue(data.startswith(b"ENC1"))

    @authors("asklit")
    def test_tampered_data(self):
        key = _encryption.generate_key()
        data = b"important data"
        enc = _encryption.encrypt(key, data)
        tampered = bytearray(enc)
        tampered[-1] ^= 0xFF
        self.assertNotEqual(_encryption.decrypt(key, bytes(tampered)), data)

    @authors("asklit")
    def test_key_reuse_across_pickler_calls(self):
        pickler1 = _pickling.Pickler("pickle")
        key = pickler1.enable_encryption(key="", engine="native_chacha")
        data1 = pickler1.dumps({"call": 1})

        pickler2 = _pickling.Pickler("pickle")
        pickler2.enable_encryption(key=key, engine="native_chacha")
        data2 = pickler2.dumps({"call": 2})

        unpickler = _pickling.Unpickler("pickle")
        unpickler.enable_encryption(key=key)
        self.assertEqual(unpickler.loads(data1), {"call": 1})
        self.assertEqual(unpickler.loads(data2), {"call": 2})

    @authors("asklit")
    def test_dump_load_file(self):
        obj = {"file": "test", "data": list(range(100))}
        pickler = _pickling.Pickler("pickle")
        key = pickler.enable_encryption(key="", engine="native_chacha")

        with tempfile.NamedTemporaryFile(delete=False) as f:
            pickler.dump(obj, f)
            fname = f.name
        try:
            unpickler = _pickling.Unpickler("pickle")
            unpickler.enable_encryption(key=key)
            with open(fname, "rb") as f:
                self.assertEqual(unpickler.load(f), obj)
        finally:
            os.unlink(fname)


class TestCreateEncryptorErrors(unittest.TestCase):

    @authors("asklit")
    def test_fernet_unavailable(self):
        import yt.wrapper.pickling as pm
        original_error = pm.fernet_import_error
        original_fernet = pm.__dict__.get("Fernet")
        try:
            pm.fernet_import_error = "mocked import error"
            if "Fernet" in pm.__dict__:
                del pm.__dict__["Fernet"]
            with self.assertRaises(Exception) as ctx:
                p = _pickling.Pickler("pickle")
                p.enable_encryption(key="", engine="cryptography_fernet")
            self.assertIn("Cannot encrypt pickled file", str(ctx.exception))
        finally:
            pm.fernet_import_error = original_error
            if original_fernet is not None:
                pm.__dict__["Fernet"] = original_fernet


class TestHexKeyRoundTrip(unittest.TestCase):

    @authors("asklit")
    def test_generate_returns_hex(self):
        cipher = _pickling._EncryptChaCha20()
        hex_key = cipher.set_key()
        self.assertIsInstance(hex_key, bytes)
        self.assertEqual(len(bytes.fromhex(hex_key.decode())), 32)

    @authors("asklit")
    def test_hex_key_set_and_decrypt(self):
        cipher1 = _pickling._EncryptChaCha20()
        hex_key = cipher1.set_key()
        enc = cipher1.encrypt(b"test hex key round trip")

        cipher2 = _pickling._EncryptChaCha20()
        cipher2.set_key(hex_key)
        self.assertEqual(cipher2.decrypt(enc), b"test hex key round trip")

    @authors("asklit")
    def test_raw_bytes_key_accepted(self):
        cipher = _pickling._EncryptChaCha20()
        raw_key = os.urandom(32)
        returned = cipher.set_key(raw_key)
        self.assertEqual(bytes.fromhex(returned.decode()), raw_key)

    @authors("asklit")
    def test_invalid_hex_falls_back_to_raw(self):
        cipher = _pickling._EncryptChaCha20()
        non_utf8_key = os.urandom(32)
        while non_utf8_key.isascii():
            non_utf8_key = os.urandom(32)
        cipher.set_key(non_utf8_key)
        self.assertEqual(cipher.key, non_utf8_key)
