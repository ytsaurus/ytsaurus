#!/bin/bash -eu

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 fs_path fs_type blocks_count mount_path user_name" >&2
    exit 1
fi

VIRTUAL_FS_PATH="$1"
VIRTUAL_FS_TYPE="$2"
BLOCK_COUNT="$3"
MOUNT_PATH="$4"
USER="$5"

dd if=/dev/zero of="${VIRTUAL_FS_PATH}" count=$BLOCK_COUNT
/sbin/mkfs -t $VIRTUAL_FS_TYPE -q "${VIRTUAL_FS_PATH}" -F
mount -o loop,rw,usrquota,grpquota "${VIRTUAL_FS_PATH}" "${MOUNT_PATH}"
quotacheck -cug "${MOUNT_PATH}"
quotaon "${MOUNT_PATH}"
chown "${USER}:${USER}" "${MOUNT_PATH}"
