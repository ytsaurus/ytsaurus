from yt.orm.library.common import wait, YtError

from yt.wrapper import ypath_join


class OrmMicroserviceIterationWatcher:
    def __init__(self, orchid_client, iteration_path):
        self._orchid_client = orchid_client
        self._iteration_path = iteration_path

    def wait_iteration(self, iteration_key, ignore_errors, ignore_error_if=None):
        leader_instance = self._orchid_client.get_leader_instance()
        iteration_path = ypath_join(self._iteration_path, iteration_key)
        wait(lambda: self._orchid_client.exists_at_instance(leader_instance, iteration_path))

        def get_iteration_count():
            error = self._orchid_client.get_at_instance(
                leader_instance, ypath_join(iteration_path, "last_iteration_error")
            )
            error = YtError.from_dict(error)
            if not ignore_errors and error.code != 0 and (ignore_error_if is None or not ignore_error_if(error)):
                raise error
            return self._orchid_client.get_at_instance(
                leader_instance, ypath_join(iteration_path, "/last_iteration_time")
            )

        current_iteration = get_iteration_count()
        wait(lambda: current_iteration < get_iteration_count())
