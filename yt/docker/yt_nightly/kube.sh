#!/bin/bash

set -e

script_name=$0
ytsaurus_source_path="."
ytsaurus_docker="."

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-docker /path/to/docker/dir]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo]
                    [--ytsaurus-build-path /path/to/ytsaurus.build]
EOF
    exit 1
}

if [[ $# -eq 0 ]]; then
    print_usage
fi

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytsaurus-source-path)
        ytsaurus_source_path=$(realpath "$2")
        shift 2
        ;;
        --ytsaurus-docker)
        ytsaurus_docker=$(realpath "$2")
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

cd ${ytsaurus_docker}

minikube start --vm-driver=docker --force
kubectl cluster-info
helm pull oci://docker.io/ytsaurus/ytop-chart --version 0.1.6 --untar
helm install ytsaurus ytop-chart/
eval $(minikube docker-env)

docker build -t local:current .

kubectl apply -f ${ytsaurus_source_path}/yt/docker/yt_nightly/cluster_v1_minikube_without_yql.yaml
kubectl apply -f ${ytsaurus_source_path}/yt/docker/yt_nightly/tester.yaml

bash ${ytsaurus_source_path}/yt/docker/yt_nightly/wait_tester.sh
