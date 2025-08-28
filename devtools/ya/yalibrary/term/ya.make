PY23_LIBRARY()

PY_SRCS(
    NAMESPACE yalibrary.term
    console.py
    size.py
)

PEERDIR(
    devtools/ya/exts
)

END()
