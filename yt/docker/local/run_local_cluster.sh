#!/bin/bash

die() {
    echo $@ >&2
    exit 1
}

set -e

script_name=$0

# Set options defaults
proxy_port=8000
docker_hostname=localhost
interface_port=8001
yt_version=stable
ui_version=stable
yt_skip_pull=false
ui_skip_pull=false
app_installation=''
local_cypress_dir=''
rpc_proxy_count=0
rpc_proxy_port=8002
node_count=1
queue_agent_count=1
with_auth=false
enable_debug_logging=false
extra_yt_docker_opts=''
yt_fqdn=''
init_operations_archive=false

network_name=yt_local_cluster_network
ui_network=$network_name

ui_container_name=yt.frontend
yt_container_name=yt.backend

ui_proxy_internal=${yt_container_name}:80

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--proxy-port port]
                    [--docker-hostname hostname]
                    [--interface-port port]
                    [--yt-version version]
                    [--ui-version version]
                    [--ui-internal-proxy proxy]
                    [--ui-network docker_network_name]
                    [--yt-skip-pull true|false]
                    [--ui-skip-pull true|false]
                    [--local-cypress-dir dir]
                    [--rpc-proxy-count count]
                    [--rpc-proxy-port port]
                    [--node-count count]
                    [--queue-agent-count count]
                    [--with-auth true|false]
                    [--enable-debug-logging true|false]
                    [--extra-yt-docker-opts opts]
                    [--init-operations-archive]
                    [--stop]

  --proxy-port: Sets the proxy port on docker host (default: $proxy_port)
  --interface-port: Sets the web interface port on docker host (default: $interface_port)
  --docker-hostname: Sets the hostname where docker engine is run (default: $docker_hostname)
  --yt-version: Sets the version of docker container with yt local cluster (default: $yt_version)
  --ui-version: Sets the version of docker container with yt web interface (default: $ui_version)
  --ui-network: Sets the network for ${ui_container_name} docker container (default: $ui_network)
  --ui-proxy-internal: Sets the value for PROXY_INTERNAL environment variable (default: $ui_proxy_internal)
  --yt-skip-pull: Enforces to skip image-pulling step to use locally built version of image (default: $yt_skip_pull)
  --ui-skip-pull: Enforces to skip image-pulling step to use locally built version of image (default: $ui_skip_pull)
  --ui-app-installation: Sets APP_INSTALLATION environment variable for yt.frontend docker container
  --local-cypress-dir: Sets the directory on the docker host to be mapped into local cypress dir inside yt local cluster container (default: $local_cypress_dir)
  --rpc-proxy-count: Sets the number of rpc proxies to start in yt local cluster (default: $rpc_proxy_count)
  --rpc-proxy-port: Sets ports for rpc proxies; number of values should be equal to rpc-proxy-count
  --node-count: Sets the number of cluster nodes to start in yt local cluster (default: $node_count)
  --queue-agent-count: Sets the number of queue agents to start in yt local cluster (default: $queue_agent_count)
  --with-auth: Enables authentication and creates admin user
  --enable-debug-logging: Enable debug logging in backend container (default: $enable_debug_logging)
  --extra-yt-docker-opts: Any extra configuration for backend docker container (default: $extra_yt_docker_opts)
  --init-operations-archive: Initialize operations archive, the option is required to keep more details of operations
  --stop: Run 'docker stop ${ui_container_name} ${yt_container_name}' and exit
EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    --proxy-port)
        proxy_port="$2"
        shift 2
        ;;
    --docker-hostname)
        docker_hostname="$2"
        shift 2
        ;;
    --interface-port)
        interface_port="$2"
        shift 2
        ;;
    --yt-version)
        yt_version="$2"
        shift 2
        ;;
    --ui-version)
        ui_version="$2"
        shift 2
        ;;
    --ui-network)
        ui_network="$2"
        shift 2
        ;;
    --ui-proxy-internal)
        ui_proxy_internal="$2"
        shift 2
        ;;
    --yt-skip-pull)
        yt_skip_pull="$2"
        shift 2
        ;;
    --ui-skip-pull)
        ui_skip_pull="$2"
        shift 2
        ;;
    --ui-app-installation)
        app_installation="$2"
        shift 2
        ;;
    --local-cypress-dir)
        local_cypress_dir="$2"
        shift 2
        ;;
    --rpc-proxy-count)
        rpc_proxy_count="$2"
        shift 2
        ;;
    --rpc-proxy-port)
        rpc_proxy_port="$2"
        shift 2
        ;;
    --node-count)
        node_count="$2"
        shift 2
        ;;
    --queue-agent-count)
        queue_agent_count="$2"
        shift 2
        ;;
    --with-auth)
        with_auth="$2"
        shift 2
        ;;
    --enable-debug-logging)
        enable_debug_logging="$2"
        shift 2
        ;;
    --extra-yt-docker-opts)
        extra_yt_docker_opts="$2"
        shift 2
        ;;
    --init-operations-archive)
        init_operations_archive=true
        shift
        ;;
    --fqdn)
        yt_fqdn="$2"
        shift 2
        ;;

    -h | --help)
        print_usage
        shift
        ;;
    --stop)
        docker stop $ui_container_name $yt_container_name
        exit
        ;;
    *) # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

# Script main part
if [ -z "$(which docker)" ]; then
    url="'unknown'"
    if [ "$(uname)" = "Linux" ]; then
        url="https://docs.docker.com/install/linux/docker-ce/ubuntu/"
    elif [ "$(uname)" = "Darwin" ]; then
        url="https://docs.docker.com/docker-for-mac/install/"
    fi
    die "Docker installation not found on the host machine, yet this script requires docker to run properly. Please \
refer to the instructions at URL $url"
fi

yt_image=ghcr.io/ytsaurus/local:$yt_version
ui_image=ghcr.io/ytsaurus/ui:$ui_version

if [ -n "$local_cypress_dir" ]; then
    if [ ! -d "$local_cypress_dir" ]; then
        echo "When --local-cypress-dir is given it should exist and be a directory" >&2
        local_cypress_dir=""
    else
        local_cypress_dir="-v $local_cypress_dir:/var/lib/yt/local-cypress"
    fi
fi

if [ "$yt_skip_pull" = "false" -o -z "$yt_skip_pull" ]; then
    docker pull $yt_image
fi

if [ "$ui_skip_pull" = "false" -o -z "$ui_skip_pull" ]; then
    docker pull $ui_image
fi

if [ -z "$(docker network ls | grep $network_name)" ]; then
    echo "Creating network $network_name" >&2
    docker network create $network_name
fi

yt_run_params=""
params=""
if [ ${enable_debug_logging} == "true" ]; then
    params="--enable-debug-logging"
    ytBackendTmp=$(readlink -f ~/yt.backend_tmp_$(date +%Y%m%d_%H%M%S))
    yt_run_params="-v ${ytBackendTmp}:/tmp"
    (
        echo
        echo "    Debug logging is enabled, you can find log-files in:"
        echo "        $ytBackendTmp"
        echo
        echo "    Additionally you may want to watch output of the command bellow:"
        echo "        docker logs -f yt.backend"
        echo
    ) >&2
fi

if [ ${with_auth} == "true" ]; then
    params="${params} --enable-auth --create-admin-user"
fi

if [ ${init_operations_archive} == "true" ]; then
    params="${params} --init-operations-archive"
fi

set +e
cluster_container=$(
    docker run -itd \
        --env YT_FORCE_IPV4=1 --env YT_FORCE_IPV6=0 --env YT_USE_HOSTS=0 \
        --network $network_name \
        --name $yt_container_name \
        -p ${proxy_port}:80 \
        -p ${rpc_proxy_port}:${rpc_proxy_port} \
        --rm \
        $local_cypress_dir \
        $extra_yt_docker_opts \
        $yt_run_params \
        $yt_image \
        --fqdn "${yt_fqdn:-${docker_hostname}}" \
        --proxy-config "{coordinator={public_fqdn=\"${docker_hostname}:${proxy_port}\"}}" \
        --rpc-proxy-count ${rpc_proxy_count} \
        --rpc-proxy-port ${rpc_proxy_port} \
        --node-count ${node_count} \
        --queue-agent-count ${queue_agent_count} \
        --address-resolver-config "{enable_ipv4=%true;enable_ipv6=%false;}" \
        --native-client-supported \
        --id primary \
        -c '{name=query-tracker}' -c '{name=yql-agent;config={path="/usr/bin";count=1;artifacts_path="/usr/bin"}}' \
        ${params}
)

if [ "$?" != "0" ]; then
    die "Image $yt_image failed to run. Most likely that was because the port $proxy_port is already busy, \
so you have to provide another port via --proxy-port option."
fi

interface_container=$(
    docker run -itd \
        --network $ui_network \
        --name $ui_container_name \
        -p ${interface_port}:80 \
        -e PROXY=${docker_hostname}:${proxy_port} \
        -e PROXY_INTERNAL=$ui_proxy_internal \
        -e APP_ENV=local \
        -e APP_INSTALLATION=${app_installation} \
        --rm \
        $ui_image
)
if [ "$?" != "0" ]; then
    die "Image $ui_image failed to run. Most likely that was because the port $interface_port is \
already busy, so you have to provide another port via --interface-port option."
fi
set -e

echo -e "\nCongratulations! Local cluster is up and running. To use the cluster web interface, point your browser to \
http://${docker_hostname}:${interface_port}. Or, if you prefer command-line tool 'yt', use it \
like this: 'yt --proxy $docker_hostname:$proxy_port <command>'.\n\nWhen you finish with the local interface,\
stop it by running a command:\n\tdocker stop $yt_container_name $ui_container_name\nor\n\t$0 --stop"
