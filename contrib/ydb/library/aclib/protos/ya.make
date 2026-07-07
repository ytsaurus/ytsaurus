LIBRARY()

PEERDIR(
    contrib/ydb/library/aclib/protos/identity
    contrib/ydb/library/aclib/protos/acl
)

END()

RECURSE(
    identity
    acl
)
