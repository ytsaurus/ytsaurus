#!/bin/bash -eux

yt2 create map_node //sys/pools/yamr_import
yt2 create map_node //sys/pools/yamr_import/yamr_copy
yt2 create map_node //sys/pools/yamr_import/yamr_postprocess
yt2 set //sys/pools/yamr_import/yamr_copy/@mode fifo
yt2 set //sys/pools/yamr_import/yamr_copy/@fifo_sort_parameters '[pending_job_count]'
yt2 set //sys/pools/yamr_import/yamr_copy/@resource_limits '{user_slots=200}'
yt2 set //sys/pools/yamr_import/@min_share_ratio 0.05
