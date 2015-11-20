import yt.logger
import logging

class TaskIdLogger(logging.LoggerAdapter):
    def __init__(self, task_id):
        self.task_id = task_id
        self.handlers = yt.logger.LOGGER.handlers
        super(TaskIdLogger, self).__init__(yt.logger.LOGGER, {})

    def process(self, msg, kwargs):
        return "{0} (task id: {1})".format(msg, self.task_id), kwargs
