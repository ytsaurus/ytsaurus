import typing

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
    except ImportError:
        pass

from yt.wrapper.errors import YtError

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
            else:
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


class Pickler(object):
    def __init__(self, framework):
        self._cypher = None
        self.framework_module = import_framework_module(framework)

    def enable_encryption(self, key: str = None) -> typing.Optional[str]:
        if key is None:
            return None
        self._cypher = _EncryptBase.create(classes=(_EncryptFernet, ))
        if not self._cypher:
            raise YtError("Cannot encrypt pickled file, missing module: \"cryptography\"."
                          "Either install one or disable encryption in config (pickling/encrypt_pickle_files).")
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
        self._cypher = None
        self.framework_module = import_framework_module(framework)

    def enable_encryption(self, key: str) -> typing.Optional[str]:
        if key is None:
            return None
        self._cypher = _EncryptBase.create(classes=(_EncryptFernet, ))
        if not self._cypher:
            raise YtError("Cannot encrypt pickled file, missing module: \"cryptography\"."
                          "Either install one or disable encryption in config (pickling/encrypt_pickle_files).")
        return self._cypher.set_key(key.encode()).decode() if self._cypher else None

    def loads(self, data: bytes, *args, **kwargs):
        if self._cypher:
            if data:
                data = self._cypher.decrypt(data)
        return self.framework_module.loads(data)

    def load(self, file: typing.IO, *args, **kwargs):
        file_data = file.read()
        return self.loads(file_data, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.framework_module, name)
