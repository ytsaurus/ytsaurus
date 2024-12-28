PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    model/__init__.py
    model/field.py
    model/many_to_one_reference.py
    model/object.py
    model/one_to_many_reference.py
    model/snapshot.py

    config.py
    name.py
    render.py
)

PEERDIR(
    library/python/resource

    contrib/python/dacite
    contrib/python/Jinja2
    contrib/python/PyYAML
)

RESOURCE(
    templates/objects-inl.h.j2 /yt/yt/orm/library/snapshot/codegen/templates/objects-inl.h.j2
    templates/objects.cpp.j2 /yt/yt/orm/library/snapshot/codegen/templates/objects.cpp.j2
    templates/objects.h.j2 /yt/yt/orm/library/snapshot/codegen/templates/objects.h.j2
    templates/public.h.j2 /yt/yt/orm/library/snapshot/codegen/templates/public.h.j2
    templates/snapshot.cpp.j2 /yt/yt/orm/library/snapshot/codegen/templates/snapshot.cpp.j2
    templates/snapshot.h.j2 /yt/yt/orm/library/snapshot/codegen/templates/snapshot.h.j2
    templates/ya.make.j2 /yt/yt/orm/library/snapshot/codegen/templates/ya.make.j2
)

END()

RECURSE(
    bin
)

RECURSE_FOR_TESTS(
    unittests
)
