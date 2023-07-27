#!/bin/bash -eu
source scheduler_simulator_analysis_config.sh

echo "Jupyter server is running at http://localhost:${LOCAL_JUPYTER_PORT}" >&2
ssh -N -L localhost:${LOCAL_JUPYTER_PORT}:localhost:${BUILD_HOST_JUPYTER_PORT} ${BUILD_HOST_USER}@${BUILD_HOST}
