#!/bin/bash

if [ $# -lt 1 ] ; then 
    echo "Usage: grep_query.sh <clique-id-or-alias> <query-id> <grep-args> [<grep-args>...]"
    exit 1
fi

clique_id=$1

if [[ "${clique_id:0:1}" == "*" ]] ; then
    echo "Cliqued seems to be identified with alias, resolving it..."
    clique_id=$(yt execute get_operation "{operation_alias=\"${clique_id}\"; attributes=[id]; include_runtime=true}" | python -c "import yt.wrapper.yson as yson; import sys; print yson.loads(sys.stdin.read())['id']")
fi

echo "Clique id: ${clique_id}"

query_id=$2

if [ -e ./$query_id ] ; then
    echo "Directory ${query_id} already exists"
    exit 2
fi

shift 2
grep_args=$*
echo "Grep args: ${grep_args}"

mkdir $query_id

pushd $query_id

echo ${clique_id} >clique_id
echo ${query_id} >query_id 
echo "${grep_args}" >grep_args
jobs=$(yt list-jobs --operation ${clique_id} --job-state running --data-source runtime | python -c 'import yt.wrapper.yson as yson; import sys; out = yson.loads(sys.stdin.read()); print "\n".join([job["id"] for job in out["jobs"]])')
echo -e "Jobs:\n${jobs}"

for job_id in ${jobs}; do 
    echo "Running grep in ${job_id}..."
    # This sucks :(
    # yt run-job-shell ${job_id} --command "fgrep ${query_id} ${grep_args}" >${job_id}
    address=$(yt get //sys/scheduler/orchid/scheduler/jobs/${job_id}/address --format dsv)
    address=${address/:*/}
    echo "Address: ${address}"

    CMD='pwd=$(pgrep -f "./ytserver-clickhouse" -n | xargs sudo pwdx | cut -f2 -d" "); pushd ${pwd}'
    CMD="${CMD}; echo START; fgrep ${query_id} ${grep_args}; popd >/dev/null 2>&1; echo END"

    (echo $CMD | ssh ${address} -o StrictHostKeyChecking=no -q -o LogLevel=Error | sed "0,/START/d" >${job_id}) &
done

while true; do
  wait -n || {
    code="$?"
    ([[ $code = "127" ]] && exit 0 || exit "$code")
    break
  }
done;

popd
