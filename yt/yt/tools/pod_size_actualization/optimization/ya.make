PY3_LIBRARY()

PEERDIR(
    contrib/python/inflection
    contrib/python/pandas
    contrib/python/numpy
    contrib/python/tqdm
    library/python/resource
    yt/python/yt/yson
    yt/yt/tools/pod_size_actualization/lib
)

# ortools (cp-sat) is not in arcadia contrib — it must be present in the porto layer at runtime

PY_SRCS(
    __init__.py
    simple.py
    data.py
    catalog.py
    results.py
    precompute.py
    optimize.py
    instance_sizes.py
    scripts/__init__.py
    scripts/shared.py
)

# Solver scripts embedded as resources under pod_solver/ prefix.
# All three files live in lib/scripts/ and are co-located in a temp dir at runtime.
RESOURCE_FILES(
    PREFIX pod_solver/
    scripts/shared.py
    scripts/model.py
    scripts/solver.py
)

END()
