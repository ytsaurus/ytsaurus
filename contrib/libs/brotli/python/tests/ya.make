PY2TEST()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_LINT()

PEERDIR(
    contrib/libs/brotli/python
)

DEPENDS(contrib/libs/brotli/python/bro)

DATA(sbr://423350049)

TEST_SRCS(
    __init__.py
    _test_utils.py
    bro_test.py
    compressor_test.py
    compress_test.py
    decompressor_test.py
    decompress_test.py
)

SIZE(MEDIUM)

END()
