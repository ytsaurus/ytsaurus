from common import update_from_env

LOG_LEVEL = "INFO"
LOG_PATTERN = "%(asctime)-15s, %(levelname)s: %(message)s"

update_from_env(globals())
