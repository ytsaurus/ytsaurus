from .config import get_config
from .schema.types import typing  # noqa

import yt.logger as logger
import yt.wrapper as yt

try:
    import yt.packages.distro as distro
except ImportError:
    import distro

import platform


class BaseLayerDetector:

    # Mined as:
    #  docker run -it --rm ubuntu:$h bash -c 'apt-get update && apt-get install -y python3 && python3 -V; grep CODENAME /etc/lsb-release;'; done
    #  or: spec = yt.spec_builders.VanillaSpecBuilder().begin_task("foo").command("python3 -V 1>&2").job_count(1).layer_paths([...]).end_task()
    UBUNTU_DEFAULT_PYTHON = {
        "precise": (3, 2),
        "trusty": (3, 4),
        "xenial": (3, 5),
        "bionic": (3, 6),
        "focal": (3, 8),
        "groovy": (3, 8),
        "jammy": (3, 10),
        "lunar": (3, 11),
        "mantic": (3, 11),
        "noble": (3, 11),
    }

    @classmethod
    def _detect_os_env(cls):
        # type: () -> typing.Tuple[str, str, typing.Tuple[int, int] | None] | None
        # ("<os>", "<os_codename>", (custom_python_major, custom_python_minor))
        python_major, python_minor = map(int, platform.python_version_tuple()[:2])
        if distro.id() == "ubuntu":
            ubuntu_codename = distro.codename()
            if ubuntu_codename:
                ubuntu_codename = ubuntu_codename.lower()

                if cls.UBUNTU_DEFAULT_PYTHON.get(ubuntu_codename) != (python_major, python_minor):
                    return ("ubuntu", ubuntu_codename, (python_major, python_minor))
                else:
                    return ("ubuntu", ubuntu_codename, None)
        return None

    @classmethod
    def _get_default_layer(cls, client, layer_type):
        # type: (yt.wrapper.YtClient, typing.Literal["porto", "docker"]) -> typing.List[str] | None

        def _filter_registry(registry_dict, os=None, tags=None):
            # type: (typing.Dict[str, dict], str | None, typing.List[str] | None) -> typing.Dict[str, dict]
            registry_items = registry_dict.items() if isinstance(registry_dict, typing.Mapping) else list()

            def _filter_rec(rec_kv):
                if not isinstance(rec_kv, typing.Tuple) or len(rec_kv) != 2:
                    return False
                path, rec = rec_kv
                if not path or not isinstance(rec, typing.Mapping):
                    return False

                if os and rec.get("os") != os:
                    return False
                if tags and isinstance(rec.get("tags"), typing.Iterable) and not all(map(lambda t: t in rec.get("tags"), tags)):
                    return False

                return True

            return dict(
                filter(
                    _filter_rec,
                    registry_items
                )
            )

        def _select_registry(registry_dict):
            # type: (typing.Dict[str, dict]) -> typing.Tuple[str, dict] | None
            if registry_dict:
                layer_path = min(registry_dict.keys())
                return (layer_path, registry_dict[layer_path])
            else:
                return None

        os, os_codename, python_version = cls._detect_os_env()
        registry_path = get_config(client)["base_layers_registry_path"]
        if os == "ubuntu":

            layer_path = []

            # get base layer from registry
            if yt.exists(registry_path, client=client):
                matched = None
                registry = yt.get(registry_path, client=client)
                matched_by_os = _filter_registry(registry.get(layer_type, {}) if isinstance(registry, typing.Mapping) else {}, os=os, tags=["os_codename={}".format(os_codename)])
                if python_version:
                    # try layer with specific python
                    matched_by_python = _filter_registry(matched_by_os, tags=["python={}.{}".format(python_version[0], python_version[1])])
                    if matched_by_python:
                        matched = _select_registry(matched_by_python)
                    else:
                        logger.warning("Can not guess base layer for this combination of python \"%s\" and \"%s\" from registry.", python_version, os_codename)

                if not matched and matched_by_os:
                    # try default layer
                    matched_by_default_python = _filter_registry(matched_by_os, tags=["python=default"])
                    if matched_by_default_python:
                        matched = _select_registry(matched_by_default_python)

                if matched:
                    if matched[1].get("base_layers"):
                        layer_path = [matched[0]] + (matched[1]["base_layers"] if isinstance(matched[1]["base_layers"], typing.List) else [matched[1]["base_layers"]])
                    else:
                        layer_path = [matched[0]]
                    logger.debug("Guessed base layer \"%s\" from registry \"%s\"", layer_path, registry_path)

                if layer_type == "porto" and not all(map(lambda p: yt.exists(p, client=client), layer_path)):
                    logger.warning("Guessed base layer from registry is absent on the cluster \"%s\"", layer_path)
                    layer_path = None

            if layer_type == "porto":
                # get base layer from default place
                if not layer_path:
                    if python_version:
                        logger.warning("Assume default porto base layer for \"%s\".", os_codename)
                    layer_path = ["//porto_layers/base/{}/porto_layer_search_ubuntu_{}_app_lastest.tar.gz".format(os_codename, os_codename)]
                    logger.debug("Guessed porto base layer \"%s\" (without registry)", layer_path)

                if not all(map(lambda p: yt.exists(p, client=client), layer_path)):
                    logger.warning("Guessed base porto layer is absent on the cluster \"%s\"", layer_path)
                    layer_path = None

            logger.debug("Guessed base layer \"%s\"", layer_path)

        return layer_path

    @classmethod
    def guess_base_layers(cls, spec, client):
        # type: (dict, yt.YtClient) -> dict

        user_layer = get_config(client)["operation_base_layer"]

        if not user_layer:
            return spec

        user_layer = user_layer.lower().strip()

        if spec.get("layer_paths") or spec.get("docker_image"):
            # do not change user spec
            if user_layer in ["auto", "porto:auto", "docker:auto"]:
                logger.debug("Operation has layer spec. Do not guess base layer")
            return spec

        if user_layer in ["auto", "porto:auto"]:
            # detect porto
            detected_layers = cls._get_default_layer(client, layer_type="porto")
            if detected_layers:
                user_layer = ",".join(detected_layers)
            else:
                logger.warning("Can not guess appropriate base porto layer")

        if user_layer in ["auto", "docker:auto"]:
            # detect docker
            detected_layers = cls._get_default_layer(client, layer_type="docker")
            if detected_layers:
                user_layer = detected_layers[0]
            else:
                logger.warning("Can not guess appropriate base docker layer")

        if user_layer in ["auto", "docker:auto", "porto:auto"]:
            return spec

        if user_layer.startswith("//"):
            spec["layer_paths"] = list(map(str.strip, user_layer.split(",")))
        else:
            spec["docker_image"] = user_layer

        return spec
