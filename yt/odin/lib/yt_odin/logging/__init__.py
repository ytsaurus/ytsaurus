import logging


class TaskLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, task_id):
        self._task_id = task_id
        super(TaskLoggerAdapter, self).__init__(logger, dict())

    def process(self, msg, kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["odin_task_id"] = self._task_id
        return "{0}\t{1}".format(self._task_id, msg), kwargs

    @property
    def handlers(self):
        return self.logger.handlers

    @handlers.setter
    def handlers(self, value):
        self.logger.handlers = value
