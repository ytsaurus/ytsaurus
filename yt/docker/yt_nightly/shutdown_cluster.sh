#!/bin/bash

ytsaurus_source_path="."
namespace=""

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytsaurus-source-path)
        ytsaurus_source_path=$(realpath "$2")
        shift 2
        ;;
        --namespace)
        namespace="$2"
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        exit 1
        ;;
    esac
done

nsflags=""
if [[ ${namespace} != "" ]]; then
  nsflags="-n ${namespace}"
fi

kubectl delete $nsflags -f ${ytsaurus_source_path}/yt/docker/yt_nightly/cluster.yaml
kubectl delete $nsflags -f ${ytsaurus_source_path}/yt/docker/yt_nightly/tester.yaml

if [[ ${namespace} != "" ]]; then
  kubectl delete namespace ${namespace}
fi
