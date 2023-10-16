INCLUDE(../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    error.cpp
    helpers.cpp
    lazy_dict.cpp
    limited_yson_writer.cpp
    list_fragment_parser.cpp
    pull_object_builder.cpp
    serialize.cpp
    yson_lazy_map.cpp

    arrow/parquete.cpp

    skiff/consumer.cpp
    skiff/converter_common.cpp
    skiff/converter_skiff_to_python.cpp
    skiff/converter_python_to_skiff.cpp
    skiff/error.cpp
    skiff/other_columns.cpp
    skiff/parser.cpp
    skiff/raw_consumer.cpp
    skiff/raw_iterator.cpp
    skiff/record.cpp
    skiff/schema.cpp
    skiff/serialize.cpp
    skiff/structured_iterator.cpp
    skiff/switch.cpp

    GLOBAL yson.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/skiff_ext
    yt/yt/python/common
    contrib/libs/pycxx
    library/cpp/skiff
    contrib/libs/apache/arrow
)

ADDINCL(
    GLOBAL contrib/libs/pycxx
)

IF(USE_ARCADIA_PYTHON)
    PY_REGISTER(yt_yson_bindings.yson_lib)
ENDIF()

END()
