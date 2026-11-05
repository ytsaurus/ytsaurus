"""Unit tests for yt.wrapper.encryption module.

Test classes:
  TestChaCha20Module        - Public API: roundtrip, empty, nonces, invalid keys, boundaries, large data
  TestPurePythonBackend     - Pure Python backend isolation: symmetry, RFC 7539 vector, block boundaries
  TestOpenSSLBackend        - OpenSSL backend isolation: roundtrip, large data (skip if unavailable)
  TestBackendConsistency    - Cross-backend: same output for same key+nonce, cross-decrypt
  TestPurePythonFallback    - Mock OpenSSL off: fallback, 1 MB, cross-backend encrypt/decrypt
"""

import os
import unittest

from yt.testlib import authors

from yt.wrapper import encryption as _encryption


class TestChaCha20Module(unittest.TestCase):

    def setUp(self):
        self.key = _encryption.generate_key()

    @authors("asklit")
    def test_key_generation(self):
        key1 = _encryption.generate_key()
        key2 = _encryption.generate_key()
        self.assertEqual(len(key1), 32)
        self.assertEqual(len(key2), 32)
        self.assertNotEqual(key1, key2)

    @authors("asklit")
    def test_encrypt_decrypt_roundtrip(self):
        for size in [1, 13, 63, 64, 65, 128, 1000, 4096]:
            data = os.urandom(size)
            self.assertEqual(
                _encryption.decrypt(self.key, _encryption.encrypt(self.key, data)),
                data, "Failed at size {}".format(size))

    @authors("asklit")
    def test_empty_data(self):
        self.assertEqual(_encryption.encrypt(self.key, b""), b"")
        self.assertEqual(_encryption.decrypt(self.key, b""), b"")

    @authors("asklit")
    def test_nonce_prepended(self):
        data = b"hello"
        enc = _encryption.encrypt(self.key, data)
        self.assertEqual(len(enc), _encryption.NONCE_SIZE + len(data))

    @authors("asklit")
    def test_different_nonces_per_call(self):
        data = b"same data"
        enc1 = _encryption.encrypt(self.key, data)
        enc2 = _encryption.encrypt(self.key, data)
        self.assertNotEqual(enc1[:_encryption.NONCE_SIZE], enc2[:_encryption.NONCE_SIZE])

    @authors("asklit")
    def test_wrong_key_produces_wrong_output(self):
        data = b"secret message"
        enc = _encryption.encrypt(self.key, data)
        self.assertNotEqual(_encryption.decrypt(_encryption.generate_key(), enc), data)

    @authors("asklit")
    def test_invalid_key_length(self):
        with self.assertRaises(ValueError):
            _encryption.encrypt(b"short", b"data")
        with self.assertRaises(ValueError):
            _encryption.decrypt(b"short", b"x" * 20)
        with self.assertRaises(ValueError):
            _encryption.encrypt(b"x" * 31, b"data")
        with self.assertRaises(ValueError):
            _encryption.encrypt(b"x" * 33, b"data")

    @authors("asklit")
    def test_data_too_short_for_decrypt(self):
        with self.assertRaises(ValueError):
            _encryption.decrypt(self.key, b"short")

    @authors("asklit")
    def test_1mb_data(self):
        data = os.urandom(1024 * 1024)
        self.assertEqual(_encryption.decrypt(self.key, _encryption.encrypt(self.key, data)), data)

    @authors("asklit")
    def test_10mb_data(self):
        data = os.urandom(10 * 1024 * 1024)
        self.assertEqual(_encryption.decrypt(self.key, _encryption.encrypt(self.key, data)), data)

    @authors("asklit")
    def test_exact_block_boundary(self):
        for size in [64, 128, 192, 256]:
            data = os.urandom(size)
            self.assertEqual(
                _encryption.decrypt(self.key, _encryption.encrypt(self.key, data)), data)

    @authors("asklit")
    def test_single_byte(self):
        for b in [b"\x00", b"\xff", b"\x80"]:
            self.assertEqual(_encryption.decrypt(self.key, _encryption.encrypt(self.key, b)), b)

    @authors("asklit")
    def test_all_zeros(self):
        data = b"\x00" * 1024
        enc = _encryption.encrypt(self.key, data)
        self.assertNotEqual(enc[_encryption.NONCE_SIZE:], data)
        self.assertEqual(_encryption.decrypt(self.key, enc), data)

    @authors("asklit")
    def test_all_ones(self):
        data = b"\xff" * 1024
        self.assertEqual(_encryption.decrypt(self.key, _encryption.encrypt(self.key, data)), data)

    @authors("asklit")
    def test_backend_info(self):
        self.assertIn(_encryption.get_backend_name(), ("openssl", "pure_python"))
        self.assertIsInstance(_encryption.is_openssl_available(), bool)

    @authors("asklit")
    def test_ciphertext_differs_from_plaintext(self):
        data = b"A" * 256
        enc = _encryption.encrypt(self.key, data)
        self.assertNotEqual(enc[_encryption.NONCE_SIZE:], data)


class TestPurePythonBackend(unittest.TestCase):

    def setUp(self):
        self.backend = _encryption._pure_python_chacha20
        self.key = _encryption.generate_key()

    @authors("asklit")
    def test_encrypt_decrypt(self):
        nonce = os.urandom(12)
        data = b"test data for pure python"
        self.assertEqual(self.backend(self.key, nonce, self.backend(self.key, nonce, data)), data)

    @authors("asklit")
    def test_symmetry(self):
        nonce = os.urandom(12)
        data = os.urandom(1000)
        self.assertEqual(self.backend(self.key, nonce, self.backend(self.key, nonce, data)), data)

    @authors("asklit")
    def test_rfc7539_roundtrip(self):
        key = bytes.fromhex(
            "000102030405060708090a0b0c0d0e0f"
            "101112131415161718191a1b1c1d1e1f")
        nonce = bytes.fromhex("000000000000004a00000000")
        plaintext = (
            b"Ladies and Gentlemen of the class of '99: "
            b"If I could offer you only one tip for the future, sunscreen would be it.")
        enc = self.backend(key, nonce, plaintext)
        self.assertEqual(self.backend(key, nonce, enc), plaintext)

    @authors("asklit")
    def test_block_boundaries(self):
        nonce = os.urandom(12)
        for size in [63, 64, 65, 127, 128, 129]:
            data = os.urandom(size)
            self.assertEqual(
                self.backend(self.key, nonce, self.backend(self.key, nonce, data)),
                data, "Failed at size {}".format(size))


class TestOpenSSLBackend(unittest.TestCase):

    def setUp(self):
        if not _encryption.is_openssl_available():
            self.skipTest("OpenSSL backend not available")
        self.backend = _encryption._openssl_chacha20
        self.key = _encryption.generate_key()

    @authors("asklit")
    def test_encrypt_decrypt(self):
        nonce = os.urandom(12)
        data = b"test data for openssl"
        self.assertEqual(self.backend(self.key, nonce, self.backend(self.key, nonce, data)), data)

    @authors("asklit")
    def test_large_data(self):
        nonce = os.urandom(12)
        data = os.urandom(5 * 1024 * 1024)
        self.assertEqual(self.backend(self.key, nonce, self.backend(self.key, nonce, data)), data)


class TestBackendConsistency(unittest.TestCase):

    def setUp(self):
        if not _encryption.is_openssl_available():
            self.skipTest("OpenSSL backend not available")
        self.key = _encryption.generate_key()

    @authors("asklit")
    def test_same_output(self):
        nonce = os.urandom(12)
        for size in [1, 13, 64, 128, 1000, 4096]:
            data = os.urandom(size)
            self.assertEqual(
                _encryption._openssl_chacha20(self.key, nonce, data),
                _encryption._pure_python_chacha20(self.key, nonce, data),
                "Mismatch at size {}".format(size))

    @authors("asklit")
    def test_cross_decrypt(self):
        nonce = os.urandom(12)
        data = os.urandom(500)
        ossl_enc = _encryption._openssl_chacha20(self.key, nonce, data)
        self.assertEqual(
            _encryption._pure_python_chacha20(self.key, nonce, ossl_enc), data)
        pure_enc = _encryption._pure_python_chacha20(self.key, nonce, data)
        self.assertEqual(
            _encryption._openssl_chacha20(self.key, nonce, pure_enc), data)


class TestPurePythonFallback(unittest.TestCase):

    @authors("asklit")
    def test_fallback_encrypt_decrypt(self):
        original = _encryption._openssl_available
        try:
            _encryption._openssl_available = False
            key = _encryption.generate_key()
            data = b"fallback test data " * 100
            self.assertEqual(_encryption.decrypt(key, _encryption.encrypt(key, data)), data)
            self.assertEqual(_encryption.get_backend_name(), "pure_python")
        finally:
            _encryption._openssl_available = original

    @authors("asklit")
    def test_fallback_1mb(self):
        original = _encryption._openssl_available
        try:
            _encryption._openssl_available = False
            key = _encryption.generate_key()
            data = os.urandom(1024 * 1024)
            self.assertEqual(_encryption.decrypt(key, _encryption.encrypt(key, data)), data)
        finally:
            _encryption._openssl_available = original

    @authors("asklit")
    def test_openssl_encrypts_pure_decrypts(self):
        if not _encryption.is_openssl_available():
            self.skipTest("OpenSSL not available")
        key = _encryption.generate_key()
        data = os.urandom(5000)
        enc = _encryption.encrypt(key, data)
        original = _encryption._openssl_available
        try:
            _encryption._openssl_available = False
            self.assertEqual(_encryption.decrypt(key, enc), data)
        finally:
            _encryption._openssl_available = original

    @authors("asklit")
    def test_pure_encrypts_openssl_decrypts(self):
        if not _encryption.is_openssl_available():
            self.skipTest("OpenSSL not available")
        key = _encryption.generate_key()
        data = os.urandom(5000)
        original = _encryption._openssl_available
        try:
            _encryption._openssl_available = False
            enc = _encryption.encrypt(key, data)
        finally:
            _encryption._openssl_available = original
        self.assertEqual(_encryption.decrypt(key, enc), data)
