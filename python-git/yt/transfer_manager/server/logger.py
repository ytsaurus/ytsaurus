import yt.logger
import logging

class TaskIdLogger(logging.LoggerAdapter):
    def __init__(self, task_id, request_id=None):
        self.task_id = task_id
        self.request_id = request_id
        self.handlers = yt.logger.LOGGER.handlers
        super(TaskIdLogger, self).__init__(yt.logger.LOGGER, {})

    def process(self, msg, kwargs):
        if self.request_id:
            return "[request {2}] [task {1}] {0}".format(msg, self.task_id, self.request_id), kwargs
        return "[task {1}] {0}".format(msg, self.task_id), kwargs

class RequestIdLogger(logging.LoggerAdapter):
    def __init__(self, request_id):
        self.request_id = request_id
        self.handlers = yt.logger.LOGGER.handlers
        super(RequestIdLogger, self).__init__(yt.logger.LOGGER, {})

    def process(self, msg, kwargs):
        return "[request {1}] {0}".format(msg, self.request_id), kwargs
