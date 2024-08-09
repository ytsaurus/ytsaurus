UNION()

FILES(
    build_yql_python_udfs_with_docker.sh
)

END()

RECURSE(
    chyt
    strawberry
    ytsaurus
    ytsaurus-server-override
)
