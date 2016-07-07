import yt.wrapper as yt

class SchedulingError(yt.YtError):
    pass

class RequestFailed(yt.YtError):
    def __init__(self, *args, **kwargs):
        http_code = kwargs.pop("http_code", 404)
        super(RequestFailed, self).__init__(*args, **kwargs)
        self.attributes["http_code"] = http_code

class IncorrectTokenError(RequestFailed):
    pass
