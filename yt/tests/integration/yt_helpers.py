from yt_commands import get, YtError, print_debug

import time


# This class provides effective means for getting information from YT
# profiling information exported via Orchid. Wrap some calculations into "with"
# section using it as a context manager, and then call `get` method of the
# remaining Profile object to get exactly the slice of given profiling path
# corresponding to the time spend in "with" section.
class ProfileMetric(object):
    def __init__(self, path):
        self.path = path
        self.tags = {}
        self.start_time = None
        self.start_value = None
        self.samples = []

    def __enter__(self):
        start_samples = self._read_from_cypress(verbose=False)
        self.start_value = start_samples[-1]["value"] if start_samples else 0
        # Need start time in mcs.
        self.start_time = int(time.time() * 1e6)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # False return value makes python re-raise the exception happened inside "with" section.
            return False
        else:
            self.update()

    def __len__(self):
        return len(self.samples)

    def __nonzero__(self):
        return bool(self.samples)

    def _read_from_cypress(self, **kwargs):
        try:
            entries = get(self.path, **kwargs)
        except YtError:
            return []

        result = []
        for entry in entries:
            satisfied = True
            for tag in self.tags:
                if tag not in entry["tags"] or self.tags[tag] != entry["tags"][tag]:
                    satisfied = False
            if satisfied:
                result.append(entry)
        return result

    def update(self):
        self.samples = self._read_from_cypress(from_time=self.start_time, verbose=False)
        return self

    def values(self):
        return [sample["value"] for sample in self.samples]

    def apply(self, func):
        return func(self.values())

    def sum(self):
        return self.apply(sum)

    def max(self):
        return self.apply(max)

    def min(self):
        return self.apply(min)

    def last(self, verbose=False):
        last = self.samples[-1]["value"]
        if verbose:
            print_debug("Profile metric \"{}\": last = {}".format(self.path, last))
        return last

    def delta(self, verbose=False):
        delta = self.last() - self.start_value if self.samples else 0
        if verbose:
            print_debug("Profile metric \"{}\": delta = {}".format(self.path, delta))
        return delta

    @staticmethod
    def at_scheduler(path):
        return ProfileMetric("//sys/scheduler/orchid/profiling/" + path)

    @staticmethod
    def at_node(node, path):
        return ProfileMetric("//sys/cluster_nodes/{0}/orchid/profiling/{1}".format(node, path))

    def with_tag(self, tag_name, tag_value):
        if self.start_time is not None:
            raise Exception("Cannot add tag. Profiling metric already used.")
        self.tags[tag_name] = tag_value
        return self

# TODO(eshcherbin): Add ProfileMetricCollection (or similar) helper class, which would handle
# one metric for several values of the same tag (e.g. 2 pools 1 metric).
# This would make some tests neater, more readable and more efficient.
#
# Usage:
# with ProfileMetricCollection.at_scheduler("path/to/metric").by_tag("pool", ["parent", "child"]) as metric:
#     pass
# assert metric["parent"].delta() == metric["child"].delta()
#
# See alternative options in the corresponding PR.
