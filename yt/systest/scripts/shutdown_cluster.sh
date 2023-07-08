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

name_cluster="ytsaurus"
name_tester="tester"
name_systest="systest"

nsflags=""
if [[ ${namespace} != "" ]]; then
  nsflags="-n ${namespace}"
  name_cluster="ytsaurus-${namespace}"
  name_tester="tester-${namespace}"
  name_systest="systest-${namespace}"
fi

helm uninstall ${nsflags} ${name_cluster} ${ytsaurus_source_path}/yt/systest/scripts/cluster.yaml
helm uninstall ${nsflags} ${name_tester} ${ytsaurus_source_path}/yt/systest/scripts/tester.yaml
helm uninstall ${nsflags} ${name_systest} ${ytsaurus_source_path}/yt/systest/scripts/systest.yaml

#kubectl delete $nsflags -f ${ytsaurus_source_path}/yt/systest/scripts/cluster.yaml
#kubectl delete $nsflags -f ${ytsaurus_source_path}/yt/systest/scripts/tester.yaml

if [[ ${namespace} != "" ]]; then
  kubectl delete namespace ${namespace}
fi
