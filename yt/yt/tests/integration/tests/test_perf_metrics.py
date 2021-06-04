import pytest
from flaky import flaky

from yt_env_setup import (
    YTEnvSetup,
    wait
)
from yt_commands import *
from yt_helpers import *

import time

##################################################################

class TestPerfMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "profile_manager": {
            "enabled_perf_events": [
                "cpu_cycles", "instruction_tlb_references",
            ]
        },
    }

    @authors("pogorelov")
    def test_static_config_for_metrics(self):
        profiler = Profiler.at_scheduler()

        print_debug("List of solomon metrics: {}".format(str(profiler.list())))

        wait(lambda: self.is_perf_metric_present(profiler, "cpu_cycles"))
        wait(lambda: self.is_perf_metric_present(profiler, "instruction_tlb_references"))

        first_cpu_cycles = profiler.get("resource_tracker/cpu_cycles", {"thread": "Control"})
        first_instruction_tlb_references = profiler.get("resource_tracker/instruction_tlb_references", {"thread": "Control"})


        wait(lambda: profiler.get(
            "resource_tracker/cpu_cycles", {"thread": "Control"}) != first_cpu_cycles)
        wait(lambda: profiler.get(
            "resource_tracker/instruction_tlb_references", {"thread": "Control"}) != first_instruction_tlb_references)

        assert profiler.get("resource_tracker/cache_misses", {"thread": "Control"}) is None
    
    
    @authors("pogorelov")
    def test_dynamic_config_for_metrics(self):
        profiler = Profiler.at_scheduler()

        assert profiler.get("resource_tracker/instructions", {"thread": "Control"}) is None

        set("//sys/scheduler/config", 
            {
                "profile_manager": {
                    "enabled_perf_events": ["instructions"]
                }
            }
        )

        wait(lambda: self.is_perf_metric_present(profiler, "instructions"))
        
        old_cpu_cycles = profiler.get("resource_tracker/cpu_cycles", {"thread": "Control"})
        old_itlb_refs = profiler.get("resource_tracker/instruction_tlb_references", {"thread": "Control"})
        time.sleep(3)
        assert profiler.get("resource_tracker/cpu_cycles", {"thread": "Control"}) == old_cpu_cycles
        assert profiler.get("resource_tracker/instruction_tlb_references", {"thread": "Control"}) == old_itlb_refs
    

    @authors("pogorelov")
    def test_clear_dynamic_config_for_metrics(self):
        profiler = Profiler.at_scheduler()

        wait(lambda: self.is_perf_metric_present(profiler, "cpu_cycles"))

        assert profiler.get("resource_tracker/cache_references", {"thread": "Control"}) is None
        set("//sys/scheduler/config", 
            {
                "profile_manager": {
                    "enabled_perf_events": ["cache_references"]
                }
            }
        )

        wait(lambda: self.is_perf_metric_present(profiler, "cache_references"))
        old_cpu_cycles = profiler.get("resource_tracker/cpu_cycles", {"thread": "Control"})

        set("//sys/scheduler/config", dict())
        wait(lambda: profiler.get("resource_tracker/cpu_cycles", {"thread": "Control"}) != old_cpu_cycles)
        old_cache_references = profiler.get("resource_tracker/cache_references", {"thread": "Control"})

        time.sleep(3)
        assert profiler.get("resource_tracker/cache_references", {"thread": "Control"}) == old_cache_references


    def is_perf_metric_present(self, profiler, metric):
        metric_value = profiler.get("resource_tracker/" + metric, {"thread": "Control"})
        return metric_value is not None and metric_value > 0
