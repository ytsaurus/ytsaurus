UNION()

FILES(
    build_yql_python_udfs_with_docker.sh
)

END()

RECURSE(
    ytsaurus
    ytsaurus-server-override
)
