TEST_SRCS(
    test_broken_gpu_nodes.py
    test_remote_copy.py
    test_scheduler_metering_statistics.py

    test_system_quotas.py
    test_system_quotas_lib.py

    test_chyt_clique_liveness.py

    test_yp_garbage_collector.py
    test_yp_ip4_manager.py
    test_yp_master.py
    test_yp_masters_available.py
    test_yp_schedule_pod.py
)

PEERDIR(
    yp/python/yp/admin
    yp/tests/helpers
    yt/odin/checks/lib/yp_helpers
)

INCLUDE(${ARCADIA_ROOT}/yp/python/ya_programs.make.inc)

DEPENDS(
    yp/server/master/bin
    yt/odin/checks/bin/broken_gpu_nodes
    yt/odin/checks/bin/remote_copy
    yt/odin/checks/bin/scheduler_metering_statistics
    yt/odin/checks/bin/yp_garbage_collector
    yt/odin/checks/bin/yp_ip4_manager
    yt/odin/checks/bin/yp_master
    yt/odin/checks/bin/yp_masters_available
    yt/odin/checks/bin/yp_schedule_pod
)
