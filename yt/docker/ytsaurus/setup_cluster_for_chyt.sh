#!/usr/bin/env bash

script_name="$0"
make_default=false

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--make-default (default: $make_default)]
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --make-default)
        make_default=true
        shift
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

# Setup yt-clickhouse user.
/usr/bin/yt create user --attributes '{name="yt-clickhouse"}' --ignore-existing
/usr/bin/yt add-member yt-clickhouse superusers || true

# Setup directory with binaries.
/usr/bin/yt create map_node //sys/bin --ignore-existing
/usr/bin/yt create map_node //sys/bin/clickhouse-trampoline --ignore-existing
/usr/bin/yt create map_node //sys/bin/ytserver-clickhouse --ignore-existing

chyt_version=$(/usr/bin/ytserver-clickhouse --version)

if [[ `/usr/bin/yt exists //sys/bin/clickhouse-trampoline/clickhouse-trampoline-${chyt_version}` == 'false' ]]; then
    /usr/bin/yt write-file //sys/bin/clickhouse-trampoline/clickhouse-trampoline-${chyt_version} < /usr/bin/clickhouse-trampoline
    /usr/bin/yt set //sys/bin/clickhouse-trampoline/clickhouse-trampoline-${chyt_version}/@executable %true
fi

if [[ `/usr/bin/yt exists //sys/bin/ytserver-clickhouse/ytserver-clickhouse-${chyt_version}` == 'false' ]]; then
    /usr/bin/yt write-file //sys/bin/ytserver-clickhouse/ytserver-clickhouse-${chyt_version} < /usr/bin/ytserver-clickhouse
    /usr/bin/yt set //sys/bin/ytserver-clickhouse/ytserver-clickhouse-${chyt_version}/@executable %true
fi

if [ "$make_default" = true ]; then
    /usr/bin/yt link --force //sys/bin/clickhouse-trampoline/clickhouse-trampoline-${chyt_version} //sys/bin/clickhouse-trampoline/clickhouse-trampoline
    /usr/bin/yt link --force //sys/bin/ytserver-clickhouse/ytserver-clickhouse-${chyt_version} //sys/bin/ytserver-clickhouse/ytserver-clickhouse
fi

# Setup ACO namespace.
/usr/bin/yt create access_control_object_namespace --attributes '{name=chyt}' --ignore-existing
