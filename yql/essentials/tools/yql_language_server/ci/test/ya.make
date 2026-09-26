EXECTEST()

RUN(
    ${ARCADIA_BUILD_ROOT}/contrib/tools/python3/bin/python3
    ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/ci/test/version_test.py
    ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/ci/version.py
    ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/CHANGELOG.md
    ${ARCADIA_BUILD_ROOT}/yql/essentials/tools/yql_language_server/yql_language_server
    CWD ${ARCADIA_BUILD_ROOT}/yql/essentials/tools/yql_language_server
    NAME version
)

RUN(
    ${ARCADIA_BUILD_ROOT}/contrib/tools/python3/bin/python3
    ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/ci/release_notes.py
    STDIN ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/CHANGELOG.md
    NAME release_notes
)

DEPENDS(
    contrib/tools/python3/bin
    yql/essentials/tools/yql_language_server
)

DATA(
    arcadia/yql/essentials/tools/yql_language_server/CHANGELOG.md
    arcadia/yql/essentials/tools/yql_language_server/ci/release_notes.py
    arcadia/yql/essentials/tools/yql_language_server/ci/test/version_test.py
    arcadia/yql/essentials/tools/yql_language_server/ci/version.py
)

END()
