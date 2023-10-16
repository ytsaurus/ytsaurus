PY23_LIBRARY(porto)

BUILD_ONLY_IF(WARNING WARNING LINUX)

PEERDIR(
    library/cpp/porto/proto
)

PY_SRCS(
    NAMESPACE porto
    __init__.py
    api.py
    exceptions.py
    container.py
    volume.py
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(test)
ENDIF()
