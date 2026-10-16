PACKAGE_NAME = "ytsaurus-flow-companion"

# The exact protoc/gencode version is fixed by the grpcio-tools pin in pyproject.toml;
# these are the matching runtime floors for the generated stubs.
GRPCIO_RUNTIME = "grpcio>=1.70.0"
PROTOBUF_RUNTIME = "protobuf>=5.29.0,<7.0.0"

# Flow protos compiled into the wheel at build time. Paths are proto import paths
# (Arcadia's PROTO_NAMESPACE(yt)), resolved against <repo>/yt as the proto root.
GRPC_PROTOS = [
    "yt/flow/library/cpp/companion/proto/companion_service.proto",
]
PLAIN_PROTOS = [
    "yt/flow/library/cpp/common/proto/message.proto",
    "yt/flow/library/cpp/common/proto/timer.proto",
    "yt/flow/library/cpp/common/proto/visit.proto",
]

# The core protos the Flow ones import. Their stubs are compiled into this wheel too, so that a
# plain `pip install` yields an importable package: the generated *_pb2.py import yt_proto at
# runtime, and no package publishing it exists on PyPI.
CORE_PROTOS = [
    "yt_proto/yt/core/misc/proto/guid.proto",
    "yt_proto/yt/core/misc/proto/error.proto",
    "yt_proto/yt/core/ytree/proto/attributes.proto",
    "yt_proto/yt/core/yson/proto/protobuf_interop.proto",
]


def get_version():
    # The flow/X.Y.Z tag is the only source of the version: the release workflow exports it
    # as YTSAURUS_PACKAGE_VERSION. A plain local build gets a dev version.
    import os

    return os.environ.get("YTSAURUS_PACKAGE_VERSION") or "0.0.dev0"


def main():
    import os

    from setuptools import setup
    from setuptools.command.build_py import build_py

    here = os.path.dirname(os.path.abspath(__file__))
    # This setup.py lives at yt/python/packages/ytsaurus-flow-companion/; the repo root is four levels up.
    repo_root = os.path.normpath(os.path.join(here, *[os.pardir] * 4))
    proto_root = os.path.join(repo_root, "yt")

    class BuildPyWithProtoStubs(build_py):
        """build_py that compiles the Flow proto stubs straight into the wheel.

        The stubs are generated under the real Arcadia prefix yt/yt/flow/library/cpp/...:
        the SDK's _proto_compat aliases yt.flow.* onto yt.yt.flow.*, so stubs installed
        at the literal proto path yt/flow/ would be shadowed by that alias.
        """

        def run(self):
            super().run()
            self._generate_proto_stubs()

        def _generate_proto_stubs(self):
            import shutil
            import tempfile

            import grpc_tools
            from grpc_tools import protoc

            # The google well-known types bundled with grpc_tools; `python -m grpc_tools.protoc`
            # adds this include automatically, protoc.main() does not.
            wkt_include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")

            missing = [
                proto
                for proto in GRPC_PROTOS + PLAIN_PROTOS + CORE_PROTOS
                if not os.path.exists(os.path.join(proto_root, proto))
            ]
            if missing:
                raise RuntimeError(
                    "Proto sources not found (build from a full ytsaurus checkout, "
                    "e.g. `pip install <checkout>/yt/python/packages/ytsaurus-flow-companion`): " + ", ".join(missing)
                )

            gen_dir = tempfile.mkdtemp(prefix="flow_companion_protos_")
            try:
                for proto, with_grpc in [(p, True) for p in GRPC_PROTOS] + [
                    (p, False) for p in PLAIN_PROTOS + CORE_PROTOS
                ]:
                    args = [
                        "grpc_tools.protoc",
                        "-I" + proto_root,
                        "-I" + wkt_include,
                        "--python_out=" + gen_dir,
                    ]
                    if with_grpc:
                        args.append("--grpc_python_out=" + gen_dir)
                    args.append(proto)
                    if protoc.main(args) != 0:
                        raise RuntimeError("protoc failed on " + proto)

                # Relocate yt/flow/... to the real prefix, and make each relocated subtree
                # regular packages. Both roots below are owned by this wheel exclusively;
                # the levels above them stay implicit namespace packages shared with
                # ytsaurus-client and ytsaurus-flow-yt-sync-mini.
                relocations = [
                    (
                        os.path.join(gen_dir, "yt", "flow", "library", "cpp"),
                        os.path.join(self.build_lib, "yt", "yt", "flow", "library", "cpp"),
                    ),
                    # yt_proto keeps its own top-level name: that is the path its generated
                    # siblings import it by.
                    (os.path.join(gen_dir, "yt_proto"), os.path.join(self.build_lib, "yt_proto")),
                ]
                for src, dst in relocations:
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                    for dirpath, _, _ in os.walk(dst):
                        init_py = os.path.join(dirpath, "__init__.py")
                        if not os.path.exists(init_py):
                            with open(init_py, "w"):
                                pass
            finally:
                shutil.rmtree(gen_dir, ignore_errors=True)

    # The python packages are installed under their real Arcadia import path
    # `yt.yt.flow.library.python.*`, so imports are identical to the in-Arcadia
    # (ya.make) build. The intermediate levels are PEP 420 implicit namespace
    # packages, so this wheel layers onto the `yt` package shipped by
    # ytsaurus-client without file collisions (same scheme as
    # ytsaurus-flow-yt-sync-mini).
    setup(
        name=PACKAGE_NAME,
        version=get_version(),
        python_requires=">=3.9",
        packages=[
            "yt.yt.flow.library.python.companion",
            "yt.yt.flow.library.python.companion.test_harness",
            "yt.yt.flow.library.python.runner",
        ],
        package_dir={
            "yt.yt.flow.library.python.companion": "../../../../yt/yt/flow/library/python/companion",
            "yt.yt.flow.library.python.companion.test_harness": "../../../../yt/yt/flow/library/python/companion/test_harness",
            "yt.yt.flow.library.python.runner": "../../../../yt/yt/flow/library/python/runner",
        },
        cmdclass={"build_py": BuildPyWithProtoStubs},
        author="timoninmaxim, sergeypozdeev, blinkov",
        author_email="timoninmaxim@ytsaurus.tech, sergeypozdeev@ytsaurus.tech, blinkov@ytsaurus.tech",
        license="Apache 2.0",
        description="Flow Python SDK: write YT Flow computations in Python.",
        long_description="YTsaurus — is a platform for distributed storage and processing of large amounts of data with support of MapReduce, "
        "distributed file system and NoSQL key-value storage."
        "\n\n"
        "This library provides the Python SDK for YT Flow pipelines: the gRPC server the Flow worker "
        "drives, the computation/state/timer API, a test harness for unit-testing computations without a cluster, "
        "and the runner helper that ships the user code into the pipeline's vanilla jobs.",
        keywords="yt ytsaurus flow sdk streaming pipeline",
        install_requires=[
            GRPCIO_RUNTIME,
            PROTOBUF_RUNTIME,
            "ytsaurus-client",
        ],
    )


if __name__ == "__main__":
    main()
