#!/bin/bash -eu

if [ "$#" -ne 6 ]; then
    echo "Usage: $0 fs_path fs_type block_count block_size mount_path user_name" >&2
    exit 1
fi

VIRTUAL_FS_PATH="$1"
VIRTUAL_FS_TYPE="$2"
BLOCK_COUNT="$3"
BLOCK_SIZE="$4"
MOUNT_PATH="$5"
USER="$6"

dd if=/dev/zero of="${VIRTUAL_FS_PATH}" count=$BLOCK_COUNT bs=$BLOCK_SIZE
/sbin/mkfs -t $VIRTUAL_FS_TYPE -q "${VIRTUAL_FS_PATH}" -F
mount -o loop,rw,usrquota,grpquota "${VIRTUAL_FS_PATH}" "${MOUNT_PATH}"
quotacheck -cug "${MOUNT_PATH}"
quotaon "${MOUNT_PATH}"
chown "${USER}" "${MOUNT_PATH}"
