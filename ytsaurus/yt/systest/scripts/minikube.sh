#!/bin/bash

set -e

script_name=$0
ytsaurus_source_path="."
image=""

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo]
                    [--image registry/path/to/image:tag]
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
        --image)
        image="$2"
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

if [[ ${image} == "" ]]; then
echo "  --image is required"
print_usage
fi

minikube start --vm-driver=docker --force
kubectl cluster-info
helm pull oci://docker.io/ytsaurus/ytop-chart --version 0.1.6 --untar
helm install ytsaurus ytop-chart/
eval $(minikube docker-env)

docker pull ${image}

helm install cluster --set YtsaurusImagePath=${image} ${ytsaurus_source_path}/yt/systest/helm/cluster
helm install tester --set YtsaurusImagePath=${image} ${ytsaurus_source_path}/yt/systest/helm/tester

bash ${ytsaurus_source_path}/yt/systest/scripts/wait.sh --name tester
