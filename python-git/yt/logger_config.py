from .common import update_from_env

LOG_LEVEL = "INFO"
LOG_PATTERN = "%(asctime)-15s\t%(levelname)s\t%(message)s"
LOG_PATH = None

update_from_env(globals())
