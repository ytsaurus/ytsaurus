from yt.wrapper.retries import ( # noqa
    Retrier,
    run_with_retries,
)

try:
    from yt.wrapper.version import VERSION as YT_VERSION
except ImportError:
    YT_VERSION = "unknown"


def make_retrier(logger, action, description, *args, **kwargs):
    def except_action(exception, attempt):
        warning_prefix = "Retry ({})".format(attempt)
        if description is not None and len(description) > 0:
            warning_prefix += " of the {}".format(description)
        logger.warning("{} has failed with error {}".format(warning_prefix, str(type(exception))))
        logger.debug("Full error message:\n{}".format(exception))

    kwargs["logger"] = logger
    retrier = Retrier(*args, **kwargs)
    retrier.action = action
    retrier.except_action = except_action
    return retrier


def get_disabled_retries_config():
    return {"enable": False}


def get_default_retries_config():
    start_timeout = 1000
    if YT_VERSION != "unknown" and tuple(map(int, YT_VERSION.split(".")[:2])) < (0, 9):
        start_timeout = 1

    return {
        "enable": True,
        "count": 8,
        "additional_retriable_error_codes": [],
        "backoff": {
            "policy": "exponential",
            "exponential_policy": {
                "start_timeout": start_timeout,
                "base": 2,
                "max_timeout": 10000,
                "decay_factor_bound": 0.2,
            },
        },
    }


def get_retrier_factory(
    logger, description=None, exceptions=tuple(), config=None, request_timeout=None
):
    if config is None:
        config = get_default_retries_config()

    chaos_monkey = None
    if "_CHAOS_MONKEY_FACTORY" in config:
        chaos_monkey = config["_CHAOS_MONKEY_FACTORY"]()

    return lambda action: make_retrier(
        logger,
        action,
        description,
        config,
        timeout=request_timeout,
        exceptions=exceptions,
        chaos_monkey=chaos_monkey,
    )
