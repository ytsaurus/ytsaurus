import os
import pathlib
import re
import subprocess
import zipfile

import pytest
import yatest.common


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
    expected = _compile_mapper(tools, tmp_path / "full-mapper", full_jar)
    actual = _compile_mapper(tools, tmp_path / "interface-mapper", interface_jar)

    assert _normalized_generated_mapper(actual) == _normalized_generated_mapper(expected)


def test_mapstruct_kotlin_data_class_uses_full_jar_as_metadata_source(tools, tmp_path):
    full_jar, stripped_jar = _compile_kotlin_model_jars(tools, tmp_path / "model")
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
