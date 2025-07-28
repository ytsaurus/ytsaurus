#!/bin/bash

set -e -o pipefail

if [[ "${1}" == "" ]]; then
	echo "[ERROR] please provide a cron script name followed by required arguments"
	exit 2
fi

TASK_NAME="${1}"
PYTHON_TASK_DIR="${YT_CRON_ROOT}/${TASK_NAME}"
BINARY_TASK_FILE="${YT_CRON_ROOT}/binaries/${TASK_NAME}"

if [[ -d "${PYTHON_TASK_DIR}" ]]; then
    echo "[INFO] Running Python task: ${TASK_NAME}"
    source ${VIRTUAL_ENV}/bin/activate
    if [[ -d "${PYTHON_TASK_DIR}/lib" ]]; then
        export PYTHONPATH="${PYTHON_TASK_DIR}/lib"
    fi
    eval python "${@}"
elif [[ -f "${BINARY_TASK_FILE}" && -x "${BINARY_TASK_FILE}" ]]; then
    echo "[INFO] Running Binary task: ${TASK_NAME}"
    "${BINARY_TASK_FILE}" "${@:2}"
else
    echo "[ERROR] Unknown task '${TASK_NAME}':"
    echo "        Not found as a Python task (directory): ${PYTHON_TASK_DIR}"
    echo "        Not found as a Binary task (executable): ${BINARY_TASK_FILE}"
    exit 2
fi
