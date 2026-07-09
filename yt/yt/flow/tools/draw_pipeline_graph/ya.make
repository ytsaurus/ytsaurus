PY3_PROGRAM()

STYLE_PYTHON()

PY_SRCS(
    __main__.py
    dot_graph.py
    graph_model.py
    graph_utils.py
    mermaid_graph.py
)

PEERDIR(
    contrib/python/pydot
    contrib/python/requests
    library/python/resource
    yt/python/client_with_rpc
    yt/python/yt/wrapper
    yt/yt/flow/library/python/client/flow_view
    yt/yt/flow/tools/draw_pipeline_graph/bindings
)

# Embed graphviz dot.
# Built package bin/graphviz with sandbox task IX_BUILD.
FROM_SANDBOX(8861278225 PREFIX dot_archive OUT_NOAUTO dot_archive/bin/dot)
RESOURCE(${BINDIR}/dot_archive/bin/dot dot_binary)

# Embed crimson fonts (license=OFL (Open Font License)).
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-BoldItalic.ttf)
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-Bold.ttf)
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-Italic.ttf)
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-Roman.ttf)
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-SemiboldItalic.ttf)
FROM_SANDBOX(8862821370 PREFIX crimson OUT_NOAUTO crimson/crimson/Crimson-Semibold.ttf)

RESOURCE(${BINDIR}/crimson/crimson/Crimson-BoldItalic.ttf Crimson-BoldItalic.ttf)
RESOURCE(${BINDIR}/crimson/crimson/Crimson-Bold.ttf Crimson-Bold.ttf)
RESOURCE(${BINDIR}/crimson/crimson/Crimson-Italic.ttf Crimson-Italic.ttf)
RESOURCE(${BINDIR}/crimson/crimson/Crimson-Roman.ttf Crimson-Roman.ttf)
RESOURCE(${BINDIR}/crimson/crimson/Crimson-SemiboldItalic.ttf Crimson-SemiboldItalic.ttf)
RESOURCE(${BINDIR}/crimson/crimson/Crimson-Semibold.ttf Crimson-Semibold.ttf)

END()
