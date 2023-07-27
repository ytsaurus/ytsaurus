#!/bin/bash -eu
source scheduler_simulator_analysis_config.sh

jupyter notebook --no-browser --port="${BUILD_HOST_JUPYTER_PORT}"
