#!/bin/bash -ex

script_name=$0

image_tag=""
ytsaurus_source_path="."
ytsaurus_build_path="."
yql_build_path="."
output_path="."
image_cr=""
component="ytsaurus"
apt_mirror="http://archive.ubuntu.com/"
install_nvidia_packages="false"

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--component some-component (default: '$component')]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo (default: $ytsaurus_source_path)]
                    [--ytsaurus-build-path /path/to/ytsaurus.build (default: $ytsaurus_build_path)]
                    [--yql-build-path /path/to/yql.build (default: $yql_build_path)]
                    [--output-path /path/to/output (default: $output_path)]
                    [--image-tag some-tag (default: $image_tag)]
                    [--image-cr some-cr/ (default: '$image_cr')]
                    [--apt-mirror http://some.apt.mirror/ (default: '$apt_mirror')]
                    [--install-nvidia-packages true|false (default: '$install_nvidia_packages')]
EOF
    exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --component)
        component="$2"
        shift 2
        ;;
        --ytsaurus-source-path)
        ytsaurus_source_path="$2"
        shift 2
        ;;
        --ytsaurus-build-path)
        ytsaurus_build_path="$2"
        shift 2
        ;;
        --yql-build-path)
        yql_build_path="$2"
        shift 2
        ;;
        --output-path)
        output_path="$2"
        shift 2
        ;;
        --image-tag)
        image_tag="$2"
        shift 2
        ;;
        --image-cr)
        image_cr="$2"
        shift 2
        ;;
        --apt-mirror)
        apt_mirror="$2"
        shift 2
        ;;
        --install-nvidia-packages)
        install_nvidia_packages="$2"
        shift 2
        ;;
        -h|--help)
        print_usage
        shift
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

rm -rf ${output_path}
mkdir -p ${output_path}

dockerfile="${ytsaurus_source_path}/yt/docker/ytsaurus/Dockerfile"
cp ${dockerfile} ${output_path}

mkdir -p ${output_path}/credits

if [[ "${component}" == "ytsaurus" ]]; then

    ytserver_all="${ytsaurus_build_path}/yt/yt/server/all/ytserver-all"
    gpuagent="${ytsaurus_source_path}/yt/yt/gpuagent/gpuagent"
    gpuagent_runner="${ytsaurus_source_path}/yt/docker/ytsaurus/gpuagent_runner.sh"
    init_queue_agent_state="${ytsaurus_build_path}/yt/python/yt/environment/bin/init_queue_agent_state/init_queue_agent_state"
    init_operations_archive="${ytsaurus_build_path}/yt/python/yt/environment/bin/init_operations_archive/init-operations-archive"
    credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/ytsaurus"

    cp ${ytserver_all} ${output_path}
    cp ${gpuagent} ${output_path}
    cp ${gpuagent_runner} ${output_path}
    cp ${init_queue_agent_state} ${output_path}
    cp ${init_operations_archive} ${output_path}

    cp -r ${ytsaurus_build_path}/ytsaurus_python ${output_path}

    cp -r ${credits}/*.CREDITS ${output_path}/credits

elif [[ "${component}" == "chyt" ]]; then

    ytserver_clickhouse="${ytsaurus_build_path}/yt/chyt/server/bin/ytserver-clickhouse"
    clickhouse_trampoline="${ytsaurus_build_path}/yt/chyt/trampoline/clickhouse-trampoline"

    chyt_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/chyt"
    setup_script="${ytsaurus_source_path}/yt/docker/ytsaurus/setup_cluster_for_chyt.sh"

    cp ${ytserver_clickhouse} ${output_path}
    cp ${clickhouse_trampoline} ${output_path}
    cp ${setup_script} ${output_path}

    cp -r ${chyt_credits}/*.CREDITS ${output_path}/credits

elif [[ "${component}" == "query-tracker" ]]; then

    ytserver_all="${ytsaurus_build_path}/yt/yt/server/all/ytserver-all"
    ytserver_yql_agent="${yql_build_path}/yt/yql/agent/bin/ytserver-yql-agent"
    init_query_tracker_state="${ytsaurus_build_path}/yt/python/yt/environment/bin/init_query_tracker_state/init_query_tracker_state"
    mrjob="${yql_build_path}/yt/yql/tools/mrjob/mrjob"
    dq_vanilla_job="${yql_build_path}/yt/yql/dq_vanilla_job/dq_vanilla_job"
    dq_vanilla_job_lite="${yql_build_path}/yt/yql/dq_vanilla_job.lite/dq_vanilla_job.lite"

    ytsaurus_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/ytsaurus"
    qt_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/query-tracker"

    cp ${ytserver_all} ${output_path}
    cp ${ytserver_yql_agent} ${output_path}
    cp ${mrjob} ${output_path}
    cp ${dq_vanilla_job} ${output_path}
    cp ${dq_vanilla_job_lite} ${output_path}
    cp ${init_query_tracker_state} ${output_path}

    cp -r ${yql_build_path}/yql_shared_libraries/yql ${output_path}/yql

    cp -r ${ytsaurus_build_path}/ytsaurus_python ${output_path}

    cp -r ${ytsaurus_credits}/ytserver-all.CREDITS ${output_path}/credits
    cp -r ${ytsaurus_credits}/init_query_tracker_state.CREDITS ${output_path}/credits
    cp -r ${qt_credits}/*.CREDITS ${output_path}/credits

elif [[ "${component}" == "strawberry" ]]; then

    strawberry_controller="${ytsaurus_source_path}/yt/chyt/controller/cmd/chyt-controller/chyt-controller"
    credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/strawberry"

    cp ${strawberry_controller} ${output_path}

    cp -r ${credits}/chyt-controller.CREDITS ${output_path}/credits

elif [[ "${component}" == "local" ]]; then

    ytserver_all="${ytsaurus_build_path}/yt/yt/server/all/ytserver-all"
    ytserver_all_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/ytsaurus"

    ytserver_yql_agent="${yql_build_path}/yt/yql/agent/bin/ytserver-yql-agent"
    init_query_tracker_state="${ytsaurus_build_path}/yt/python/yt/environment/bin/init_query_tracker_state/init_query_tracker_state"
    mrjob="${yql_build_path}/yt/yql/tools/mrjob/mrjob"
    dq_vanilla_job="${yql_build_path}/yt/yql/dq_vanilla_job/dq_vanilla_job"
    dq_vanilla_job_lite="${yql_build_path}/yt/yql/dq_vanilla_job.lite/dq_vanilla_job.lite"

    ytsaurus_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/ytsaurus"
    qt_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/query-tracker"
    configure_file="${ytsaurus_source_path}/yt/docker/local/configure.sh"
    start_file="${ytsaurus_source_path}/yt/docker/local/start.sh"

    cp ${ytserver_all} ${output_path}
    cp -r ${ytsaurus_build_path}/ytsaurus_python ${output_path}

    # YQL/QT files.
    cp ${ytserver_yql_agent} ${output_path}
    cp ${mrjob} ${output_path}
    cp ${dq_vanilla_job} ${output_path}
    cp ${dq_vanilla_job_lite} ${output_path}
    cp ${init_query_tracker_state} ${output_path}
    cp -r ${yql_build_path}/yql_shared_libraries/yql ${output_path}/yql

    # YT local specific files.
    cp ${configure_file} ${output_path}
    cp ${start_file} ${output_path}

    # Credits.
    mkdir -p ${output_path}/credits
    cp -r ${ytserver_all_credits}/*.CREDITS ${output_path}/credits
    cp -r ${qt_credits}/*.CREDITS ${output_path}/credits

elif [[ "${component}" == "job-environment" ]]; then

    echo "Do nothing" > /dev/null

else
    echo "Unknown component: ${component}"
fi

cd ${output_path}
docker build --target ${component} --build-arg APT_MIRROR=${apt_mirror} --build-arg INSTALL_NVIDIA_PACKAGES=${install_nvidia_packages} -t ${image_cr}ytsaurus/${component}:${image_tag} .
