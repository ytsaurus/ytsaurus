SET(YT_SOURCE_FILES
    common.h
    id_generator.h
    lease_manager.cpp
    metric.cpp
    config.h
    cache.h
    cache-inl.h
    serialize.cpp
    lazy_ptr.h
    hash.h
    fs.cpp
    preprocessor.h
    pattern_formatter.cpp
    enum.h
    checksum.cpp
    backtrace.cpp
    demangle.cpp
    ref_counted_tracker.cpp
    delayed_invoker.cpp
    semaphore.cpp
    guid.cpp
    assert.h
    ref.h
    string.h
    foreach.h
    thread_affinity.h
    periodic_invoker.cpp
    zigzag.h
    property.h
    ref_counted_base.h
    intrusive_ptr.h
    singleton.h
    rvalue.h


    ${SRCDIR}/preprocessor-gen.h
    ${SRCDIR}/new.h
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${SRCDIR}/preprocessor-gen.h
    COMMAND ${PYTHON} ${SRCDIR}/preprocessor-gen.py > ${SRCDIR}/preprocessor-gen.h
    DEPENDS ${SRCDIR}/preprocessor-gen.py
    COMMENT "Regenerating preprocessor-gen.h..."
)

ADD_CUSTOM_COMMAND(
    OUTPUT  ${SRCDIR}/new.h
    COMMAND ${PYTHON} ${ARCADIA_ROOT}/scripts/pump.py ${SRCDIR}/new.h.pump > ${SRCDIR}/new.h
    DEPENDS ${SRCDIR}/new.h.pump
    COMMENT "Regenerating new.h..."
)
