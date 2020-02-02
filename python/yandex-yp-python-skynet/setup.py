PACKAGE_NAME = "yandex-yp-skynet"


if __name__ == "__main__":
    from yp_package import setup_package
    setup_package(
        PACKAGE_NAME,
        [
            "yandex-yt-yson-bindings-skynet >= 0.3.28-0",
        ],
    )
