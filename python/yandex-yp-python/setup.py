PACKAGE_NAME = "yandex-yp"


if __name__ == "__main__":
    from yp_package import setup_package
    setup_package(
        PACKAGE_NAME,
        [
            "yandex-yt-yson-bindings >= 0.3.28-0",
        ],
    )
