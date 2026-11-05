import typing
from typing import Literal

if typing.TYPE_CHECKING:
    from importlib import import_module
    from cryptography.fernet import Fernet, InvalidToken
else:
    try:
        from importlib import import_module
    except ImportError:
        from yt.packages.importlib import import_module

    try:
        from cryptography.fernet import Fernet, InvalidToken
        fernet_import_error = None
    except ImportError as ex:
        fernet_import_error = ex.msg

from yt.wrapper.errors import YtError

from yt.wrapper import encryption as _encryption_mod

EncryptionEngine = Literal["native_chacha", "cryptography_fernet"]

FRAMEWORKS = {
    "dill": ("yt.packages.dill",),
    "cloudpickle": ("yt.packages.cloudpickle", "cloudpickle",),
    "pickle": ("pickle",),
}


def import_framework_module(framework):
    if framework not in FRAMEWORKS:
        raise YtError("Cannot find pickling framework {0}. Available frameworks: {1}."
                      .format(framework, list(FRAMEWORKS)))
    result_module = None
    modules = FRAMEWORKS[framework]
    for module in modules:
        try:
            result_module = import_module(module)
        except ImportError:
            pass

    if framework == "dill":
        # NB: python3.8 has changes DEFAULT_PROTOCTOL to 4.
        # We set protocol implicitly for client<->server compatibility.
        result_module.settings["protocol"] = 3
        result_module.settings["byref"] = True

    if result_module is None:
        raise RuntimeError("Failed to find module for framework '{}', tried modules {}".format(framework, modules))

    return result_module


class _EncryptBase:
    data_base_prefix = b"ENC"
    data_prefix = b""
    module_to_check = ""

    @staticmethod
    def create(classes: typing.List["_EncryptBase"]) -> typing.Optional["_EncryptBase"]:
        for cls in classes:
            if cls.module_to_check in globals():
                return cls()
            return None

    def __init__(self):
        self.key: bytes = None

    def _generate_key(self) -> bytes:
        pass

    def _encrypt(self, data: bytes) -> bytes:
        pass

    def _decrypt(self, data: bytes) -> typing.Optional[bytes]:
        pass

    def set_key(self, key: bytes = None) -> bytes:
        if key:
            self.key = key
        else:
            self.key = self._generate_key()
        return self.key

    def encrypt(self, data: bytes) -> typing.Optional[bytes]:
        if data:
            return self.data_prefix + self._encrypt(data)
        return None

    def decrypt(self, data: bytes) -> typing.Optional[bytes]:
        if data and data.startswith(self.data_base_prefix):
            if data.startswith(self.data_prefix):
                return self._decrypt(data[len(self.data_prefix):])
            return None
        else:
            return data


class _EncryptFernet(_EncryptBase):
    data_prefix = _EncryptBase.data_base_prefix + b'1'
    module_to_check = "Fernet"

    def _generate_key(self) -> bytes:
        return Fernet.generate_key()

    def _encrypt(self, data: bytes) -> bytes:
        return Fernet(self.key).encrypt(data)

    def _decrypt(self, data: bytes) -> typing.Optional[bytes]:
        try:
            return Fernet(self.key).decrypt(data)
        except InvalidToken:
            raise YtError("Cannot decrypt pickled file")


class _EncryptChaCha20(_EncryptBase):
    """ChaCha20 via ctypes+OpenSSL with pure Python fallback."""
    data_prefix = _EncryptBase.data_base_prefix + b'2'
    module_to_check = "_encryption_mod"

    def _generate_key(self) -> bytes:
        return _encryption_mod.generate_key()

    def set_key(self, key: bytes = None) -> bytes:
        if key:
            try:
                self.key = bytes.fromhex(key.decode())
            except (ValueError, UnicodeDecodeError):
                self.key = key
        else:
            self.key = self._generate_key()
        return self.key.hex().encode()

    def _encrypt(self, data: bytes) -> bytes:
        return _encryption_mod.encrypt(self.key, data)

    def _decrypt(self, data: bytes) -> typing.Optional[bytes]:
        try:
            return _encryption_mod.decrypt(self.key, data)
        except (ValueError, RuntimeError) as ex:
            raise YtError(f"Cannot decrypt pickled file: {ex}")


class Pickler(object):
    def __init__(self, framework):
        self._cypher = None
        self.framework_module = import_framework_module(framework)

    def enable_encryption(self, key: str = None, engine: EncryptionEngine = "cryptography_fernet") -> typing.Optional[str]:
        if key is None:
            return None
        if engine == "native_chacha":
            self._cypher = _EncryptBase.create(classes=(_EncryptChaCha20,))
        elif engine == "cryptography_fernet":
            self._cypher = _EncryptBase.create(classes=(_EncryptFernet,))
            if not self._cypher:
                raise YtError(f"Cannot encrypt pickled file, missing module: \"cryptography\" ({fernet_import_error})."
                              " Either install one or disable encryption in config (pickling/encrypt_pickle_files).")
        else:
            raise YtError(f"Unknown encryption engine: \"{engine}\"."
                          " Supported: \"native_chacha\", \"cryptography_fernet\".")
        return self._cypher.set_key(key.encode()).decode() if self._cypher else None

    def dumps(self, obj: object, *args, **kwargs) -> bytes:
        pickled_data = self.framework_module.dumps(obj, *args, **kwargs)
        if self._cypher:
            pickled_data = self._cypher.encrypt(pickled_data)
        return pickled_data

    def dump(self, obj: object, file: typing.IO, *args, **kwargs):
        pickled_data = self.dumps(obj, *args, **kwargs)
        file.write(pickled_data)

    def __getattr__(self, name):
        return getattr(self.framework_module, name)


class Unpickler(object):
    def __init__(self, framework):
        self._ciphers = {}
        self._encryption_enabled = False
        self.framework_module = import_framework_module(framework)

    def enable_encryption(self, key: str) -> typing.Optional[str]:
        if key is None:
            return None
        self._encryption_enabled = True
        key_bytes = key.encode()

        chacha = _EncryptBase.create(classes=(_EncryptChaCha20,))
        if chacha:
            chacha.set_key(key_bytes)
            self._ciphers[_EncryptChaCha20.data_prefix] = chacha

        fernet = _EncryptBase.create(classes=(_EncryptFernet,))
        if fernet:
            fernet.set_key(key_bytes)
            self._ciphers[_EncryptFernet.data_prefix] = fernet

        return key

    def _decrypt_data(self, data: bytes) -> typing.Optional[bytes]:
        if not data or not data.startswith(_EncryptBase.data_base_prefix):
            return data
        for prefix, cipher in self._ciphers.items():
            if data.startswith(prefix):
                return cipher.decrypt(data)
        raise YtError("Cannot decrypt pickled file: unsupported encryption format")

    def loads(self, data: bytes, *args, **kwargs):
        if self._encryption_enabled and data:
            data = self._decrypt_data(data)
        return self.framework_module.loads(data)

    def load(self, file: typing.IO, *args, **kwargs):
        file_data = file.read()
        return self.loads(file_data, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.framework_module, name)
