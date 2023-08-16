import logging

def setup_logger():
    _logger = logging.getLogger("stress")
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
    handler.setFormatter(formatter)
    _logger.handlers.clear()
    _logger.addHandler(handler)
    _logger.propagate = False

    class Adapter(logging.LoggerAdapter):
        def __init__(self, logger):
            super(Adapter, self).__init__(logger, {})
            self.iteration = None
            self.generation = None
            self.kind = None

            self.warn = self.warning

        def setLevel(self, *args, **kwargs):
            self.logger.setLevel(*args, **kwargs)

        def process(self, msg, kwargs):
            extra = ["extra"]
            if self.generation is not None:
                extra += ["gen {}".format(self.generation)]
            if self.kind is not None:
                extra += ["kind {}".format(self.kind)]
            if self.iteration is not None:
                extra += ["iter {}".format(self.iteration)]
            prefix = "[" + ", ".join(extra) + "] " if extra else ""
            return prefix + msg, kwargs

    global logger
    logger = Adapter(_logger)
    logger.setLevel(logging.INFO)

setup_logger()
