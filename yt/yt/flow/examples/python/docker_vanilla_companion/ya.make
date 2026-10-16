UNION()

# The example is never built here: its sources are copied into a docker image and run there, and
# the spec is filled in by hand. UNION keeps `pipeline.yson` in the module so that all of it is
# published, which PY_SRCS would not do.
FILES(
    README.md
    __init__.py
    __main__.py
    pipeline.yson
    text_mapper.py
)

END()
