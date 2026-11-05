PY3_LIBRARY()

PEERDIR(
    contrib/python/pandas
    library/python/resource
    yt/yt/tools/pod_size_actualization/lib
)

# pulp + highspy are not in arcadia contrib — they must be present in the porto layer at runtime

PY_SRCS(
    __init__.py
    cluster.py
    configs.py
    ilp.py
    validation.py
    placement.py
    scripts/__init__.py
    scripts/shared.py
)

# Solver scripts embedded as resources under defrag_solver/ prefix.
# Both files are extracted to a temp dir at runtime and run via system Python.
RESOURCE_FILES(
    PREFIX defrag_solver/
    scripts/shared.py
    scripts/solver.py
)

END()
