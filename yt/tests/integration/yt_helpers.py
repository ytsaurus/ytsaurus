from yt_commands import get, YtError


# This class provides effective means for getting information from YT
# profiling information exported via Orchid. Wrap some calculations into "with"
# section using it as a context manager, and then call `get` method of the
# remaining Profile object to get exactly the slice of given profiling path
# corresponding to the time spend in "with" section.
class ProfileMetric(object):
    def __init__(self, path):
        self.path = path
        self.tags = {}

    def _read_from_cypress(self):
        try:
            entries = get(self.path)
        except YtError:
            return []

        if len(self.tags) == 0:
            return entries
        result = []
        for entry in entries:
            for tag in self.tags:
                if tag in entry["tags"] and self.tags[tag] == entry["tags"][tag]:
                    result.append(entry)
        return result

    def __enter__(self):
        self.len_on_enter = len(self._read_from_cypress())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # False return value makes python re-raise the exception happened inside "with" section.
            return False
        else:
            self.profile = self._read_from_cypress()

    def get(self):
        return self.profile[self.len_on_enter:]

    def total(self):
        return sum(event["value"] for event in self.get())

    def _up_to_moment(self, i):
        return self.profile[i - 1]["value"] if i > 0 else 0

    def differentiate(self):
        return self._up_to_moment(len(self.profile)) - self._up_to_moment(self.len_on_enter)

    @staticmethod
    def at_scheduler(path):
        return ProfileMetric("//sys/scheduler/orchid/profiling/" + path)

    @staticmethod
    def at_node(node, path):
        return ProfileMetric("//sys/nodes/{0}/orchid/profiling/{1}".format(node, path))

    def with_tag(self, tag_name, tag_value):
        if self.len_on_enter is not None:
            raise Exception("Cannot add tag. Profiling metric already used.")
        self.tags[tag_name] = tag_value
        return self
