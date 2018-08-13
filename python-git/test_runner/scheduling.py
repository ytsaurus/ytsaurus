from collections import defaultdict

try:
    xrange
except NameError:  # Python 3
    xrange = range

_test_scheduling_func = None

def split_test_suites(suites, process_count):
    suites_per_process = max(1, len(suites) // process_count)
    suites_keys = list(suites)

    slaves_tasks = []

    for i in xrange(process_count):
        begin_index = i * suites_per_process
        if i == process_count - 1:
            suites_block = suites_keys[begin_index:]
        else:
            suites_block = suites_keys[begin_index:begin_index + suites_per_process]

        slaves_tasks.append([])
        for suite_key in suites_block:
            slaves_tasks[i].extend(suites[suite_key])

    return slaves_tasks

def _default_scheduling_func(test_items, process_count):
    test_suites = defaultdict(list)

    for test_index, test in enumerate(test_items):
        test_suites[(test.cls, test.fspath)].append(test_index)

    return split_test_suites(test_suites, process_count)

def get_scheduling_func():
    if _test_scheduling_func is None:
        return _default_scheduling_func
    return _test_scheduling_func

def set_scheduling_func(func):
    global _test_scheduling_func
    _test_scheduling_func = func

