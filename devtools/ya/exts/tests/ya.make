OWNER(
    g:yatool
    g:ymake
)

PY23_TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_archive.py
    test_async.py
    test_compress.py
    test_decompress.py
    test_detect_recursive.py
    test_flatten.py
    test_flock.py
    test_hashing.py
    test_http.py
    test_os2.py
    test_path2.py
    test_process.py
    test_yjdump.py
)

PEERDIR(
    devtools/ya/exts
    library/python/compress
    contrib/python/six
)

NO_LINT()

DEPENDS(
    devtools/ya/exts/tests/data
)

DATA(sbr://334044124)

END()
