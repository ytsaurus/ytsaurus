SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/common.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/id_generator.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/lease_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/metric.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/config.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/cache.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/cache-inl.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/serialize.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/lazy_ptr.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/hash.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/fs.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/pattern_formatter.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/enum.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/checksum.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/backtrace.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/demangle.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/ref_counted_tracker.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/delayed_invoker.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/semaphore.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/guid.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/assert.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/ref.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/string.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/foreach.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/thread_affinity.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/periodic_invoker.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/zigzag.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/property.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/ref_counted_base.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/intrusive_ptr.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/singleton.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/rvalue.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/codec.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/async_stream_state.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/cyclic_buffer.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/mpl.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new_config.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new_config-inl.h


    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor-gen.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor-gen.h
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor-gen.py > ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor-gen.h
    DEPENDS ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/preprocessor-gen.py
    COMMENT "Regenerating preprocessor-gen.h..."
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new.h
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/pump.py ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new.h.pump > ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new.h
    DEPENDS ${CMAKE_SOURCE_DIR}/yt/ytlib/misc/new.h.pump
    COMMENT "Regenerating new.h..."
)
