GO_LIBRARY()

RESOURCE(
    certs/cacert.pem /certifi/common.pem
    certs/yandex_internal.pem /certifi/internal.pem
    certs/YCKZInternalRootCA.crt /certifi/internalYCKZ.pem
)

SRCS(certs.go)

END()
