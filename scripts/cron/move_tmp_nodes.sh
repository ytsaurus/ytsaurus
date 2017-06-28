#!/bin/bash -eux

MOVE_TMP_NODES="/opt/cron/move_tmp_nodes.py"

MOVE_TMP_NODES --root "/" --trash-dir "//tmp/trash_by_cron" --account "tmp"

