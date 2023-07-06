#!/bin/bash

set -e
set -x

script_name=$0
ytsaurus_source_path="."
namespace=""

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo]
                    [--namespace namespace]
EOF
    exit 1
}

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
        print_usage
        ;;
    esac
done

kubectl cluster-info

nsflags=""
tester_flags=""

if [[ ${namespace} != "" ]]; then
  kubectl create namespace ${namespace}
  nsflags="-n ${namespace}"
  tester_flags="--namespace ${namespace}"
fi

kubectl apply $nsflags -f ${ytsaurus_source_path}/yt/docker/yt_nightly/cluster.yaml

# Wait for Cypress
kubectl apply $nsflags -f ${ytsaurus_source_path}/yt/docker/yt_nightly/tester.yaml
bash ${ytsaurus_source_path}/yt/docker/yt_nightly/wait.sh --name tester ${tester_flags}

kubectl apply $nsflags -f ${ytsaurus_source_path}/yt/docker/yt_nightly/systest.yaml

bash ${ytsaurus_source_path}/yt/docker/yt_nightly/wait.sh --name systest --wait-minutes 60 ${tester_flags}
