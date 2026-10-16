"""ChaCha20 encryption backends for yt_wrapper pickle file protection.

Clarify (@denvr) the use of ssl (Apache 2.0) in case of use through transitive dependencies.

Two backends:
  1. OpenSSL via ctypes (fast) - used when libcrypto is available
  2. Pure Python (slow) - fallback when OpenSSL is unavailable

Wire format: NONCE (12 bytes) || CIPHERTEXT (same length as plaintext)
"""

import os
import struct
import typing
from typing import Literal

import yt.logger as logger

NONCE_SIZE = 12
KEY_SIZE = 32

_M32 = 0xFFFFFFFF

# ---------------------------------------------------------------------------
# Backend 1: ctypes + OpenSSL (lazy initialization)
# ---------------------------------------------------------------------------
# NOTE: OpenSSL (libcrypto) is used via ctypes as a transitive dependency.
# OpenSSL is licensed under Apache License 2.0.
# See: https://www.openssl.org/source/license.html

_openssl_initialized = False
_openssl_available = False
_openssl_import_error = None
_libcrypto = None
_cipher = None


def _init_libcrypto():
    """Initialize OpenSSL backend on first use."""
    global _openssl_initialized, _openssl_available, _openssl_import_error
    global _libcrypto, _cipher

    if _openssl_initialized:
        return
    _openssl_initialized = True

    try:
        import ctypes
        import ctypes.util

        crypto_path = ctypes.util.find_library("crypto")
        if not crypto_path:
            raise ImportError("libcrypto not found on this system")

        libcrypto = ctypes.CDLL(crypto_path)

        libcrypto.EVP_CIPHER_CTX_new.restype = ctypes.c_void_p
        libcrypto.EVP_CIPHER_CTX_free.argtypes = [ctypes.c_void_p]
        libcrypto.EVP_chacha20.restype = ctypes.c_void_p

        libcrypto.EVP_EncryptInit_ex.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
            ctypes.c_char_p, ctypes.c_char_p,
        ]
        libcrypto.EVP_EncryptInit_ex.restype = ctypes.c_int

        libcrypto.EVP_EncryptUpdate.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_int),
            ctypes.c_char_p, ctypes.c_int,
        ]
        libcrypto.EVP_EncryptUpdate.restype = ctypes.c_int

        libcrypto.EVP_EncryptFinal_ex.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_int),
        ]
        libcrypto.EVP_EncryptFinal_ex.restype = ctypes.c_int

        cipher = libcrypto.EVP_chacha20()
        if not cipher:
            raise ImportError("EVP_chacha20 not supported by this OpenSSL build")

        _libcrypto = libcrypto
        _cipher = cipher
        _openssl_available = True
        logger.debug(f"OpenSSL encryption backend initialized (lib: {crypto_path})")

    except Exception as ex:
        _openssl_import_error = str(ex)
        logger.debug(f"OpenSSL encryption backend unavailable, using pure Python fallback: {ex}")


def _openssl_chacha20(key: bytes, nonce: bytes, data: bytes) -> bytes:
    """Encrypt/decrypt data using ChaCha20 via OpenSSL EVP API."""
    import ctypes

    iv = b"\x00\x00\x00\x00" + nonce
    ctx = _libcrypto.EVP_CIPHER_CTX_new()
    if not ctx:
        raise MemoryError()
    try:
        if _libcrypto.EVP_EncryptInit_ex(ctx, _cipher, None, key, iv) != 1:
            raise RuntimeError("EVP_EncryptInit_ex failed")

        out_buf = ctypes.create_string_buffer(len(data) + 64)
        out_len = ctypes.c_int(0)

        if _libcrypto.EVP_EncryptUpdate(ctx, out_buf, ctypes.byref(out_len), data, len(data)) != 1:
            raise RuntimeError("EVP_EncryptUpdate failed")
        total = out_len.value

        if _libcrypto.EVP_EncryptFinal_ex(ctx, ctypes.addressof(out_buf) + total, ctypes.byref(out_len)) != 1:
            raise RuntimeError("EVP_EncryptFinal_ex failed")
        total += out_len.value

        return out_buf.raw[:total]
    finally:
        _libcrypto.EVP_CIPHER_CTX_free(ctx)


# ---------------------------------------------------------------------------
# Backend 2: Pure Python ChaCha20 (RFC 7539)
# ---------------------------------------------------------------------------

_CONSTANTS = (0x61707865, 0x3320646E, 0x79622D32, 0x6B206574)
_PACK_16I = struct.Struct("<16I")
_UNPACK_8I = struct.Struct("<8I")
_UNPACK_3I = struct.Struct("<3I")


def _quarter_round(a, b, c, d):
    """ChaCha20 quarter round (RFC 7539 section 2.1)."""
    M = _M32

    a = (a + b) & M
    d ^= a
    d = ((d << 16) | (d >> 16)) & M

    c = (c + d) & M
    b ^= c
    b = ((b << 12) | (b >> 20)) & M

    a = (a + b) & M
    d ^= a
    d = ((d << 8) | (d >> 24)) & M

    c = (c + d) & M
    b ^= c
    b = ((b << 7) | (b >> 25)) & M

    return a, b, c, d


def _chacha20_block(key_words, nonce_words, counter):
    """Generate one 64-byte keystream block (RFC 7539 section 2.3)."""
    state = list(_CONSTANTS) + list(key_words) + [counter] + list(nonce_words)
    x = list(state)

    for _ in range(10):  # 20 rounds = 10 double-rounds
        # Column rounds
        x[0], x[4], x[8],  x[12] = _quarter_round(x[0], x[4], x[8],  x[12])
        x[1], x[5], x[9],  x[13] = _quarter_round(x[1], x[5], x[9],  x[13])
        x[2], x[6], x[10], x[14] = _quarter_round(x[2], x[6], x[10], x[14])
        x[3], x[7], x[11], x[15] = _quarter_round(x[3], x[7], x[11], x[15])
        # Diagonal rounds
        x[0], x[5], x[10], x[15] = _quarter_round(x[0], x[5], x[10], x[15])
        x[1], x[6], x[11], x[12] = _quarter_round(x[1], x[6], x[11], x[12])
        x[2], x[7], x[8],  x[13] = _quarter_round(x[2], x[7], x[8],  x[13])
        x[3], x[4], x[9],  x[14] = _quarter_round(x[3], x[4], x[9],  x[14])

    return _PACK_16I.pack(*((xi + si) & _M32 for xi, si in zip(x, state)))


def _pure_python_chacha20(key: bytes, nonce: bytes, data: bytes) -> bytes:
    """Encrypt/decrypt data using pure Python ChaCha20 (RFC 7539)."""
    key_words = _UNPACK_8I.unpack(key)
    nonce_words = _UNPACK_3I.unpack(nonce)
    data_len = len(data)
    num_blocks = (data_len + 63) // 64

    if num_blocks > _M32:
        raise ValueError("Data too large for ChaCha20 (max ~256 GB)")

    keystream = b"".join(
        _chacha20_block(key_words, nonce_words, counter)
        for counter in range(num_blocks)
    )

    ks_int = int.from_bytes(keystream[:data_len], "little")
    data_int = int.from_bytes(data, "little")
    return (ks_int ^ data_int).to_bytes(data_len, "little")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_key() -> bytes:
    """Generate a random 32-byte ChaCha20 key."""
    return os.urandom(KEY_SIZE)


def encrypt(key: bytes, plaintext: bytes) -> bytes:
    """Encrypt plaintext with ChaCha20, prepending a random nonce.

    Returns: NONCE (12 bytes) || CIPHERTEXT
    """
    if len(key) != KEY_SIZE:
        raise ValueError(f"Key must be {KEY_SIZE} bytes, got {len(key)}")
    if not plaintext:
        return b""

    _init_libcrypto()
    nonce = os.urandom(NONCE_SIZE)
    backend = _openssl_chacha20 if _openssl_available else _pure_python_chacha20
    return nonce + backend(key, nonce, plaintext)


def decrypt(key: bytes, data: bytes) -> bytes:
    """Decrypt data produced by encrypt().

    Expects: NONCE (12 bytes) || CIPHERTEXT
    """
    if len(key) != KEY_SIZE:
        raise ValueError(f"Key must be {KEY_SIZE} bytes, got {len(key)}")
    if not data:
        return b""
    if len(data) < NONCE_SIZE:
        raise ValueError(f"Data too short: must be at least {NONCE_SIZE} bytes")

    _init_libcrypto()
    nonce, ciphertext = data[:NONCE_SIZE], data[NONCE_SIZE:]
    backend = _openssl_chacha20 if _openssl_available else _pure_python_chacha20
    return backend(key, nonce, ciphertext)


def is_openssl_available() -> bool:
    """Check whether the OpenSSL backend is available."""
    _init_libcrypto()
    return _openssl_available


def get_backend_name() -> Literal["openssl", "pure_python"]:
    """Return the name of the active backend."""
    _init_libcrypto()
    return "openssl" if _openssl_available else "pure_python"


def get_openssl_import_error() -> typing.Optional[str]:
    """Return the OpenSSL import error message, if any."""
    _init_libcrypto()
    return _openssl_import_error
