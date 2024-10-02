def mypy_config_resource() -> tuple[str, str]:
    return "__tests__", "yt/yt/orm/codegen/mypy.ini"


def mypy_check_root() -> str:
    return "yt/yt/orm/codegen"
