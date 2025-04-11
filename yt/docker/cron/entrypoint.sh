#!/bin/bash

set -e -o pipefail

source ${VIRTUAL_ENV}/bin/activate

if [[ "${1}" == "" ]]; then
	echo "[ERROR] please provide a cron script name followed by required arguments"
	exit 2
fi

TASK="${YT_CRON_ROOT}/${1}"

if [[ ! -d ${TASK} ]]; then
    echo "[ERROR] unknown script"
    exit 2
fi

if [[ -d ${TASK}/lib ]]; then
    export PYTHONPATH="${TASK}/lib"
fi

eval python ${@}
