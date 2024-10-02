PY23_TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_ssh_sign_load_key.py
)

PEERDIR(
    library/python/ssh_sign
    contrib/python/paramiko
)

END()
