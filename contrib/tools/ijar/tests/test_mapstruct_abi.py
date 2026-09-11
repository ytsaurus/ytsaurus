import logging
import os
import pathlib
import re
import runpy
import shutil
import struct
import subprocess
import sys
import zipfile

import pytest
import yatest.common

from build.scripts.canonicalize_java_abi_jar import canonicalize


MAPSTRUCT_VERSION = "1.5.5.Final"


def _binary_path(path):
    return yatest.common.binary_path(path)


@pytest.fixture(scope="session")
def tools():
    jdk = pathlib.Path(yatest.common.runtime.global_resources()["JDK_DEFAULT_RESOURCE_GLOBAL"])
    kotlin = pathlib.Path(yatest.common.runtime.global_resources()["KOTLIN_COMPILER_RESOURCE_GLOBAL"])
    return {
        "ijar": _binary_path("contrib/tools/ijar/tests/source/ijar"),
        "java": str(jdk / "bin" / "java"),
        "javac": str(jdk / "bin" / "javac"),
        "kotlin_compiler": str(kotlin / "kotlin-compiler.jar"),
        "kotlin_abi_plugin": str(kotlin / "plugins" / "kotlin-jvm-abi-gen-plugin.jar"),
        "kotlin_stdlib": _binary_path(
            "contrib/java/org/jetbrains/kotlin/kotlin-stdlib/2.3.10/kotlin-stdlib-2.3.10.jar"
        ),
        "mapstruct": _binary_path(
            "contrib/java/org/mapstruct/mapstruct/{0}/mapstruct-{0}.jar".format(MAPSTRUCT_VERSION)
        ),
        "processor": _binary_path(
            "contrib/java/org/mapstruct/mapstruct-processor/{0}/mapstruct-processor-{0}.jar".format(
                MAPSTRUCT_VERSION
            )
        ),
    }


def _run(command, *, expect_success=True):
    logging.info("Running: %r", list(map(str, command)))
    result = subprocess.run(
        command,
        text=True,
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if expect_success and result.returncode != 0:
        raise AssertionError(
            "command failed with exit code {}:\n{}\nstdout:\n{}\nstderr:\n{}".format(
                result.returncode,
                " ".join(map(str, command)),
                result.stdout,
                result.stderr,
            )
        )
    return result


def _write_sources(root, sources):
    paths = []
    for relative, content in sources.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        paths.append(path)
    return paths


def _compile_java(
    tools, root, sources, *, classpath=(), processorpath=(), generated=None, debug=True
):
    source_root = root / "src"
    classes = root / "classes"
    classes.mkdir(parents=True)
    source_paths = _write_sources(source_root, sources)
    command = [tools["javac"], "-g" if debug else "-g:none", "-d", str(classes)]
    if classpath:
        command += ["-classpath", os.pathsep.join(map(str, classpath))]
    if processorpath:
        command += [
            "-processorpath",
            os.pathsep.join(map(str, processorpath)),
            "-processor",
            "org.mapstruct.ap.MappingProcessor",
            "-Amapstruct.unmappedTargetPolicy=ERROR",
            "-Amapstruct.suppressGeneratorTimestamp=true",
            "-Amapstruct.suppressGeneratorVersionInfoComment=true",
        ]
    else:
        command += ["-proc:none"]
    if generated is not None:
        generated.mkdir(parents=True)
        command += ["-s", str(generated)]
    command += list(map(str, source_paths))
    return _run(command), classes


def _make_jar(classes, output, extra_entries=None):
    with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED) as jar:
        for path in sorted(classes.rglob("*")):
            if path.is_file():
                jar.write(path, path.relative_to(classes).as_posix())
        for name, content in (extra_entries or {}).items():
            jar.writestr(name, content)
    return output


def _compile_model_jar(tools, root, *, debug=True):
    _, classes = _compile_java(
        tools,
        root,
        {
            "fixture/ImmutableTarget.java": """
                package fixture;

                public final class ImmutableTarget {
                    private final String displayName;
                    private final long[] buckets;
                    private final long itemCount;
                    private final double ratio;

                    public ImmutableTarget(
                            String displayName, long[] buckets, long itemCount, double ratio) {
                        this.displayName = displayName;
                        this.buckets = buckets;
                        this.itemCount = itemCount;
                        this.ratio = ratio;
                    }

                    public String getDisplayName() {
                        return displayName;
                    }

                    public long[] getBuckets() {
                        return buckets;
                    }

                    public long getItemCount() {
                        return itemCount;
                    }

                    public double getRatio() {
                        return ratio;
                    }
                }
            """,
        },
        debug=debug,
    )
    return _make_jar(classes, root / "model.jar")


def _compile_kotlin_model_jars(
    tools, root, implementation_marker="FULL_IMPLEMENTATION_ONLY_MARKER"
):
    source = root / "src" / "fixture" / "ImmutableTarget.kt"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
            package fixture

            data class ImmutableTarget(
                val displayName: String,
                val buckets: LongArray,
                val itemCount: Long,
                val ratio: Double,
            ) {
                fun implementationDetail(): String = "__IMPLEMENTATION_MARKER__"
            }

            private class ImplementationOnly
        """.replace("__IMPLEMENTATION_MARKER__", implementation_marker),
        encoding="utf-8",
    )
    classes = root / "classes"
    abi = root / "abi"
    classes.mkdir()
    abi.mkdir()
    _run(
        [
            tools["java"],
            "-jar",
            tools["kotlin_compiler"],
            "-no-stdlib",
            "-classpath",
            tools["kotlin_stdlib"],
            "-jvm-target",
            "17",
            "-Xplugin={}".format(tools["kotlin_abi_plugin"]),
            "-P",
            "plugin:org.jetbrains.kotlin.jvm.abi:outputDir={}".format(abi),
            "-P",
            "plugin:org.jetbrains.kotlin.jvm.abi:removePrivateClasses=true",
            "-d",
            str(classes),
            str(source),
        ]
    )
    return _make_jar(classes, root / "model.jar"), _make_jar(abi, root / "model-stripped.jar")


MAPPER_SOURCES = {
    "mapper/Source.java": """
        package mapper;

        public final class Source {
            private final String displayName;
            private final long[] buckets;
            private final long itemCount;
            private final double ratio;

            public Source(String displayName, long[] buckets, long itemCount, double ratio) {
                this.displayName = displayName;
                this.buckets = buckets;
                this.itemCount = itemCount;
                this.ratio = ratio;
            }

            public String getDisplayName() {
                return displayName;
            }

            public long[] getBuckets() {
                return buckets;
            }

            public long getItemCount() {
                return itemCount;
            }

            public double getRatio() {
                return ratio;
            }
        }
    """,
    "mapper/TargetMapper.java": """
        package mapper;

        import fixture.ImmutableTarget;
        import org.mapstruct.Mapper;

        @Mapper
        public interface TargetMapper {
            ImmutableTarget map(Source source);
        }
    """,
}


def _compile_mapper(tools, root, model_jar):
    generated = root / "generated"
    _compile_java(
        tools,
        root,
        MAPPER_SOURCES,
        classpath=[model_jar, tools["mapstruct"]],
        processorpath=[tools["processor"]],
        generated=generated,
    )
    return (generated / "mapper" / "TargetMapperImpl.java").read_text(encoding="utf-8")


def _normalized_generated_mapper(source):
    return re.sub(r"\s+", " ", source).strip()


def test_mapstruct_constructor_mapping_matches_full_jar(tools, tmp_path):
    full_jar = _compile_model_jar(tools, tmp_path / "model")
    interface_jar = tmp_path / "model-interface.jar"
    _run([tools["ijar"], str(full_jar), str(interface_jar)])

    canonicalize(interface_jar, ijar=True)
    expected = _compile_mapper(tools, tmp_path / "full-mapper", full_jar)
    actual = _compile_mapper(tools, tmp_path / "interface-mapper", interface_jar)

    assert _normalized_generated_mapper(actual) == _normalized_generated_mapper(expected)


def test_mapstruct_constructor_mapping_uses_full_jar_as_metadata_source(tools, tmp_path):
    full_jar = _compile_model_jar(tools, tmp_path / "full-model")
    stripped_jar = _compile_model_jar(tools, tmp_path / "stripped-model", debug=False)
    interface_jar = tmp_path / "model-interface.jar"
    _run(
        [
            tools["ijar"],
            "--metadata_source",
            str(full_jar),
            str(stripped_jar),
            str(interface_jar),
        ]
    )
    canonicalize(interface_jar, ijar=True)
    expected = _compile_mapper(tools, tmp_path / "full-mapper", full_jar)
    actual = _compile_mapper(tools, tmp_path / "interface-mapper", interface_jar)

    assert _normalized_generated_mapper(actual) == _normalized_generated_mapper(expected)


def test_mapstruct_kotlin_data_class_uses_full_jar_as_metadata_source(tools, tmp_path):
    full_jar, stripped_jar = _compile_kotlin_model_jars(tools, tmp_path / "model")
    canonicalize(stripped_jar)
    interface_jar = tmp_path / "model-interface.jar"
    _run(
        [
            tools["ijar"],
            "--metadata_source",
            str(full_jar),
            str(stripped_jar),
            str(interface_jar),
        ]
    )
    with zipfile.ZipFile(interface_jar) as jar:
        assert "fixture/ImplementationOnly.class" not in jar.namelist()
        assert b"FULL_IMPLEMENTATION_ONLY_MARKER" not in jar.read(
            "fixture/ImmutableTarget.class"
        )

    canonicalize(interface_jar, ijar=True)
    expected = _compile_mapper(tools, tmp_path / "full-mapper", full_jar)
    actual = _compile_mapper(tools, tmp_path / "interface-mapper", interface_jar)

    assert _normalized_generated_mapper(actual) == _normalized_generated_mapper(expected)


def test_kotlin_implementation_change_does_not_change_interface_jar(tools, tmp_path):
    first_full, first_stripped = _compile_kotlin_model_jars(
        tools, tmp_path / "first-model", "FIRST_IMPLEMENTATION_MARKER"
    )
    second_full, second_stripped = _compile_kotlin_model_jars(
        tools, tmp_path / "second-model", "SECOND_IMPLEMENTATION_MARKER"
    )
    canonicalize(first_stripped)
    canonicalize(second_stripped)
    first_interface = tmp_path / "first-interface.jar"
    second_interface = tmp_path / "second-interface.jar"

    _run(
        [
            tools["ijar"],
            "--metadata_source",
            str(first_full),
            str(first_stripped),
            str(first_interface),
        ]
    )
    _run(
        [
            tools["ijar"],
            "--metadata_source",
            str(second_full),
            str(second_stripped),
            str(second_interface),
        ]
    )

    canonicalize(first_interface, ijar=True)
    canonicalize(second_interface, ijar=True)
    assert first_interface.read_bytes() == second_interface.read_bytes()


def test_mapstruct_spi_provider_cannot_be_silently_stripped(tools, tmp_path):
    _, classes = _compile_java(
        tools,
        tmp_path / "provider",
        {
            "fixture/Provider.java": """
                package fixture;

                import org.mapstruct.ap.spi.DefaultAccessorNamingStrategy;

                public final class Provider extends DefaultAccessorNamingStrategy {
                }
            """,
        },
        classpath=[tools["processor"]],
    )
    provider_jar = _make_jar(
        classes,
        tmp_path / "provider.jar",
        {
            "META-INF/services/org.mapstruct.ap.spi.AccessorNamingStrategy": "fixture.Provider\n",
        },
    )

    result = _run(
        [tools["ijar"], str(provider_jar), str(tmp_path / "provider-interface.jar")],
        expect_success=False,
    )

    assert result.returncode != 0
    assert "META-INF/services/org.mapstruct.ap.spi.AccessorNamingStrategy" in result.stderr
    assert "JAVA_LIBRARY_NON_SPLIT" in result.stderr


def _payloads(path):
    with zipfile.ZipFile(path) as archive:
        return sorted((info.filename, archive.read(info), info.compress_type) for info in archive.infolist())


def _raw_names(path):
    data = path.read_bytes()
    with zipfile.ZipFile(path) as archive:
        return sorted(
            data[
                info.header_offset
                + 30 : info.header_offset
                + 30
                + struct.unpack_from("<H", data, info.header_offset + 26)[0]
            ]
            for info in archive.infolist()
        )


def _normalize(path, *, ijar=False):
    before = _payloads(path)
    raw_names = _raw_names(path)
    assert canonicalize(path, ijar=ijar)
    assert _raw_names(path) == raw_names
    if not ijar:
        assert _payloads(path) == before
    else:
        # ijar's unflagged names decode as CP437 before recovery. Payload and
        # compression must still agree for each raw-name occurrence.
        recovered = sorted((name.encode("cp437").decode("utf-8"), body, method) for name, body, method in before)
        assert _payloads(path) == recovered
    after = path.read_bytes()
    assert canonicalize(path, ijar=ijar)
    assert path.read_bytes() == after


def test_java_full_input_order_and_unicode_resolution(tools, tmp_path):
    names = ["Caf\u00e9", "\U00010400", "Cafe\u0301"]
    _, classes = _compile_java(
        tools,
        tmp_path / "model",
        {
            "fixture/{}.java".format(name): "package fixture; public class "
            + name
            + " { public static int value() { return 7; } }"
            for name in names
        },
    )
    outputs = []
    for variant in range(2):
        full = tmp_path / "full-{}.jar".format(variant)
        interface = tmp_path / "interface-{}.jar".format(variant)
        paths = sorted(classes.rglob("*.class"), reverse=bool(variant))
        with zipfile.ZipFile(full, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in paths:
                info = zipfile.ZipInfo(path.relative_to(classes).as_posix(), (2020 + variant, 1, 1, 0, 0, 0))
                archive.writestr(info, path.read_bytes())
            archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\r\nBuild: {}\r\n\r\n".format(variant))
            archive.writestr("META-INF/SBOM.json", str(variant))
        original = full.read_bytes()
        _run(
            [
                tools["ijar"],
                "--target_label",
                "//fixture:model",
                "--injecting_rule_kind",
                "arcadia_ijar",
                str(full),
                str(interface),
            ]
        )
        _normalize(interface, ijar=True)
        assert full.read_bytes() == original
        _, consumer = _compile_java(
            tools,
            tmp_path / "consumer-{}".format(variant),
            {
                "Main.java": "public class Main { public static void main(String[] args) { System.out.println("
                + "+".join("fixture.{}.value()".format(name) for name in names)
                + "); } }"
            },
            classpath=[interface],
        )
        assert _run([tools["java"], "-cp", os.pathsep.join(map(str, [consumer, full])), "Main"]).stdout.strip() == "21"
        outputs.append(interface.read_bytes())
    assert outputs[0] == outputs[1]


def _tree_state(root):
    return {
        path.relative_to(root).as_posix(): (path.stat().st_mtime_ns, path.read_bytes() if path.is_file() else None)
        for path in root.rglob("*")
    }


def test_actual_stripped_pack_with_adversarial_trees(tools, tmp_path):
    full, _ = _compile_kotlin_model_jars(tools, tmp_path / "model")
    original_full = full.read_bytes()
    source = tmp_path / "model" / "abi"
    outputs = []
    interfaces = []
    for variant in range(2):
        tree = tmp_path / "tree-{}".format(variant)
        tree.mkdir()
        for path in sorted(source.rglob("*"), reverse=bool(variant)):
            dest = tree / path.relative_to(source)
            if path.is_file():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, dest)
            else:
                dest.mkdir(parents=True, exist_ok=True)
        for path in tree.rglob("*"):
            os.utime(path, (1500000000 + variant * 100000,) * 2)
        before = _tree_state(tree)
        stripped = tmp_path / "stripped-{}.jar".format(variant)
        _run([str(pathlib.Path(tools["javac"]).with_name("jar")), "cfM", str(stripped), "-C", str(tree), "."])
        _normalize(stripped)
        assert _tree_state(tree) == before
        interface = tmp_path / "abi-{}.jar".format(variant)
        _run(
            [
                tools["ijar"],
                "--metadata_source",
                str(full),
                "--target_label",
                "//fixture:model",
                "--injecting_rule_kind",
                "arcadia_ijar",
                str(stripped),
                str(interface),
            ]
        )
        _normalize(interface, ijar=True)
        with zipfile.ZipFile(interface) as archive:
            assert "fixture/ImplementationOnly.class" not in archive.namelist()
            assert b"FULL_IMPLEMENTATION_ONLY_MARKER" not in archive.read("fixture/ImmutableTarget.class")
        outputs.append(stripped.read_bytes())
        interfaces.append(interface.read_bytes())
    assert outputs[0] == outputs[1]
    assert interfaces[0] == interfaces[1]
    assert full.read_bytes() == original_full


def _run_script(monkeypatch, script, *args):
    # Execute the production script without assuming the native test binary is
    # a standalone Python interpreter. Restore argv/import paths afterwards.
    with monkeypatch.context() as patch:
        patch.setattr(sys, "argv", [script] + list(map(str, args)))
        patch.setattr(sys, "path", list(sys.path))
        runpy.run_path(yatest.common.source_path(script), run_name="__main__")


def test_mixed_kotlin_inline_sam_across_two_interfaces(tools, tmp_path, monkeypatch):
    fixture = pathlib.Path(yatest.common.source_path("devtools/dummy_arcadia/kotlin/abi_jar"))
    jars = []
    interfaces = []
    for module in ("producer", "middle", "consumer"):
        root = tmp_path / module
        root.mkdir()
        classes, abi, java_classes = root / "kt_cls", root / "abi", root / "cls"
        classes.mkdir()
        abi.mkdir()
        java_classes.mkdir()
        sources = list((fixture / module).glob("*.kt")) + list((fixture / module).glob("*.java"))
        classpath = interfaces + [pathlib.Path(tools["kotlin_stdlib"])]
        _run(
            [
                tools["java"],
                "-jar",
                tools["kotlin_compiler"],
                "-no-stdlib",
                "-classpath",
                os.pathsep.join(map(str, classpath)),
                "-jvm-target",
                "17",
                "-Xplugin=" + tools["kotlin_abi_plugin"],
                "-P",
                "plugin:org.jetbrains.kotlin.jvm.abi:outputDir=" + str(abi),
                "-P",
                "plugin:org.jetbrains.kotlin.jvm.abi:removePrivateClasses=true",
                "-d",
                str(classes),
            ]
            + list(map(str, sources))
        )
        java_sources = [path for path in sources if path.suffix == ".java"]
        if java_sources:
            _run(
                [
                    tools["javac"],
                    "-d",
                    str(java_classes),
                    "-classpath",
                    os.pathsep.join(map(str, [classes] + classpath)),
                ]
                + list(map(str, java_sources))
            )
        _run_script(monkeypatch, "build/conf/copy_kotlin_inline_sam_classes.py", classes, abi)
        java_before = _tree_state(java_classes)
        _run_script(monkeypatch, "build/scripts/fs_tools.py", "link_or_copy_all_files", java_classes, abi)
        stripped, full, interface = root / "stripped.jar", root / "full.jar", root / "interface.jar"
        jar = str(pathlib.Path(tools["javac"]).with_name("jar"))
        _run([jar, "cfM", str(stripped), "-C", str(abi), "."])
        _normalize(stripped)
        assert java_before == _tree_state(java_classes)
        # Full Kotlin classes merge only AFTER the stripped jar is packed.
        _run_script(monkeypatch, "build/scripts/fs_tools.py", "link_or_copy_all_files", classes, java_classes)
        _run([jar, "cfM", str(full), "-C", str(java_classes), "."])
        original = full.read_bytes()
        for path in classes.rglob("*$inlined$sam$*.class"):
            with zipfile.ZipFile(stripped) as archive:
                assert archive.read(path.relative_to(classes).as_posix()) == path.read_bytes()
        _run([tools["ijar"], "--metadata_source", str(full), str(stripped), str(interface)])
        _normalize(interface, ijar=True)
        assert full.read_bytes() == original
        jars.append(full)
        interfaces.append(interface)
    result = _run(
        [tools["java"], "-cp", os.pathsep.join(map(str, jars + [tools["kotlin_stdlib"]])), "ru.yandex.abi.MainKt"]
    )
    assert result.stdout.strip() == "[full implementation]"
