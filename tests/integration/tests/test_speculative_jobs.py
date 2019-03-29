from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.test_helpers import wait


class TestSpeculativeJobs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    def test_both_jobs_ends_simultaneously(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()

        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 2
        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 2

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 1

    def test_original_faster_than_speculative(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = self.get_sorted_jobs(op)

        release_breakpoint(job_id=original["id"])
        op.track()

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["aborted"]["scheduled"]["speculative_run_lost"] == 1
        assert job_counters["aborted"]["scheduled"]["speculative_run_won"] == 0

    def test_speculative_faster_than_original(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = self.get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["aborted"]["scheduled"]["speculative_run_lost"] == 0
        assert job_counters["aborted"]["scheduled"]["speculative_run_won"] == 1

    def test_speculative_job_fail_fails_whole_operation(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job(
            command="BREAKPOINT;exit 1",
            spec={"max_failed_job_count": 1}
        )
        original, speculative = self.get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track(raise_on_failed=False)
        assert op.get_state() == "failed"

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["failed"] == 1
        assert job_counters["total"] == 1

    def test_speculative_job_fail_but_regular_job_continues(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job(
            command="BREAKPOINT;exit 1",
            spec={"max_failed_job_count": 2}
        )
        original, speculative = self.get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        wait(lambda: get(op.get_path() + "/@progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["failed"] == 1
        assert job_counters["total"] == 1
        op.abort()

    def test_speculative_job_aborts_but_regular_job_succeeds(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = self.get_sorted_jobs(op)

        abort_job(speculative["id"])
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["total"] == 1

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["running"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["completed"] == 1
        assert job_counters["total"] == 1

    def test_regular_job_aborts_but_speculative_job_succeeds(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = self.get_sorted_jobs(op)

        abort_job(original["id"])
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["total"] == 1

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["running"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["completed"] == 1
        assert job_counters["total"] == 1

    def test_map_with_speculative_job(self):
        create_test_tables()
        op = map(
            command=with_breakpoint("BREAKPOINT; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"register_speculative_job_on_job_scheduled": True}},
            dont_track=True)
        wait_breakpoint(job_count=2)
        original, speculative = self.get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 1
        assert read_table("//tmp/t_out") == [{"x": '0'}]

    def run_vanilla_with_one_regular_and_one_speculative_job(self, spec=None, command="BREAKPOINT"):
        spec = spec if spec else {}
        spec["testing"] = {"register_speculative_job_on_job_scheduled": True}
        op = run_test_vanilla(with_breakpoint(command), spec=spec, job_count=1)
        wait_breakpoint(job_count=2)
        wait(lambda: get(op.get_path() + "/@progress/jobs")["running"] == 2)
        return op

    def get_sorted_jobs(self, op):
        jobs = []
        for id, job in op.get_running_jobs().iteritems():
            job["id"] = id
            jobs.append(job)

        return sorted(jobs, key=lambda job: job["start_time"])
