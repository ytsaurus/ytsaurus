import logging

class Extras:
    iteration = None
    generation = None
    kind = None
    pid = None
    func = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_prefix(self):
        extra = []
        if self.generation is not None:
            extra += ["gen {}".format(self.generation)]
        if self.kind is not None:
            extra += ["kind {}".format(self.kind)]
        if self.iteration is not None:
            extra += ["iter {}".format(self.iteration)]
        if self.pid is not None:
            extra += ["PID {}".format(self.pid)]
        if self.func is not None:
            extra += ["{}".format(self.func)]
        return "[" + ", ".join(extra) + "] " if extra else ""

class Adapter(logging.LoggerAdapter, Extras):
    def __init__(self, logger):
        super().__init__(logger, {})

    def process(self, *args, **kwargs):
        return super().process(*args, **kwargs)

def setup_logger():
    global logger
    logger = Adapter(logging.getLogger("stress"))

    logging.basicConfig(format="%(asctime)s\t%(levelname)s\t%(message)s", level=logging.INFO)

    old_factory = logging.getLogRecordFactory()

    def record_factory(name, *args, **kwargs):
        record = old_factory(name, *args, **kwargs)
        record.msg = logger.get_prefix() + record.msg
        return record

    logging.setLogRecordFactory(record_factory)

setup_logger()
