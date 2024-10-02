PY23_LIBRARY()

PEERDIR(
    yt/python/yt/environment/arcadia_interop
)

PY_SRCS(
    NAMESPACE yt.orm.tests

    base_object_test.py
    helpers.py
    migrations.py
    orm_test_environment.py
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        blackbox_recipe
        secret_vault_recipe
    )
ENDIF()
