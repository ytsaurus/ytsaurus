#!/bin/bash -e

set -o pipefail

set +u
if [ -z "$SANDBOX_TOKEN" ]; then
    echo "Sandbox token should be specified via SANDBOX_TOKEN env variable" && exit 1
fi
set -u

sandbox_request() {
    local method="$1" && shift
    local path="$1" && shift

    touch _curl_out
    http_code=$(curl -X "$method" -sS -k -L -o "_curl_out" -w '%{http_code}' \
                "https://sandbox.yandex-team.ru/api/v1.0/${path}" \
                -H "Content-Type: application/json" \
                -H "Authorization: OAuth $SANDBOX_TOKEN" \
                "$@")

    if [[ $(($http_code / 100)) -ne "2" ]]; then
        echo "Sandbox request failed with error: $(cat _curl_out)"
        exit 1
    fi

    cat _curl_out
    rm -f "_curl_out"
}

upload_to_sandbox() {
    local archive_path="$1" && shift

    task=$(cat <<EOF
{
    "type": "REMOTE_COPY_RESOURCE",
    "context":
    {
        "resource_type": "YT_NIGHTLY_COMPRESSION_SCRIPTS",
        "remote_file_protocol": "http",
        "remote_file_name": "http://locke.yt.yandex.net/api/v3/read_file?path=${archive_path}&disposition=attachment",
        "created_resource_name": "scripts.tar",
        "resource_attrs": "ttl=inf,backup_task=true,commit_hash=$(git rev-parse --verify HEAD)"
    }
}
EOF
)
    task_id=$(echo -ne "$task" | \
              sandbox_request "POST" "task" -d @- | \
              python2 -c 'import sys, json; print json.load(sys.stdin)["id"]')

    echo "Created sandbox task: $task_id"

    task_params=$(cat <<EOF
{
    "description": "Upload nightly compression scripts",
    "notifications": [
        {
            "transport": "email",
            "recipients": ["$(whoami)"],
            "statuses": ["SUCCESS"]
        },
        {
            "transport": "email",
            "recipients": ["$(whoami)"],
            "statuses": ["FAILURE", "TIMEOUT", "EXCEPTION"]
        }
    ],
    "owner": "YT"
}
EOF
)

    echo -ne "$task_params" | sandbox_request "PUT" "task/${task_id}" -d @-

    echo -ne "[$task_id]" | sandbox_request "PUT" "batch/tasks/start" -d @-

    echo "Successfully started sandbox task $task_id. This task id can be used in Nanny now."
}

release_nightly_compression_scripts() {
    local temp_archive_file=$(mktemp --suffix release_nightly_compression.tar)

    tar cvf $temp_archive_file \
        ./nightly_process_watcher.py \
        ./push_nightly_process_watcher_stats.py \
        ./compression/find_tables_to_compress.py \
        ./compression/worker.py

    echo "Packaged all nightly compression files to $temp_archive_file"

    local archive_path="//home/files/$(basename $temp_archive_file)"
    cat $temp_archive_file | yt write-file "$archive_path" --proxy locke
    rm -rf $temp_archive_file

    upload_to_sandbox $archive_path
}

release_nightly_compression_scripts
