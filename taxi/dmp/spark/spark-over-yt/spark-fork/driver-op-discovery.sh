#!/usr/bin/env bash

if [ -z "$SPARK_DRIVER_RESOURCE" ]; then
  echo {\"name\": \"driverop\", \"addresses\":[]}
else
  echo {\"name\": \"driverop\", \"addresses\":[\"$(seq -s "\",\"" 1 "$SPARK_DRIVER_RESOURCE")\"]}
fi
