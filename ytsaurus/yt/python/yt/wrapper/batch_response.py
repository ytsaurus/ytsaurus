from .common import YtError


class BatchResponse(object):
    def __init__(self):
        self._output = None
        self._error = None
        self._executed = False
        self._output_transformation_functions = []

    def get_result(self):
        if not self._executed:
            raise YtError("Batch has not been executed yet")

        for function, include_error in self._output_transformation_functions:
            if not include_error:
                if self._error is None:
                    self._output = function(self._output)
            else:
                self._output, self._error = function(self._output, self._error)

        self._output_transformation_functions = []
        return self._output

    def get_error(self):
        if not self._executed:
            raise YtError("Batch has not been executed yet")
        return self._error

    def add_output_transformation_function(self, function, include_error):
        self._output_transformation_functions.append((function, include_error))

    def is_ok(self):
        if not self._executed:
            raise YtError("Batch has not been executed yet")
        return self._error is None

    def set_result(self, result):
        self._output = result.get("output")
        self._error = result.get("error")
        self._executed = True


def apply_function_to_result(function, result, include_error=False):
    if not isinstance(result, BatchResponse):
        return function(result)

    result.add_output_transformation_function(function, include_error)
    return result
