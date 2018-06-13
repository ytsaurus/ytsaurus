#!/bin/bash

CLUSTER="${1}"
VERSION="${2}"
READONLY="${3}"
if [ "${CLUSTER}" == "" ]; then echo "Usage: ${0} cluster version [--set-read-only]"; exit 1; fi
if [ "${VERSION}" == "" ]; then echo "Usage: ${0} cluster version [--set-read-only]"; exit 1; fi
if [[ "${READONLY}" != "" && "${READONLY}" != "--set-read-only" ]]; then
    echo "Usage: ${0} cluster version [--set-read-only]"
    exit 1
fi

MASTERS=""
PROXIES=""
SCHEDULERS=""
CONTROLLERAGENTS=""
NODES=""
RPCPROXIES=""

case $CLUSTER in 
    zeno)
    MASTERS="m0[1-5]i.zeno.yt.yandex.net"
    NODES="n00[01-64]i.zeno.yt.yandex.net"
    ;;
    seneca-man)
    MASTERS="m0[1-5]i.seneca-man.yt.yandex.net"
    NODES="n00[01-74]i.seneca-man.yt.yandex.net"
    ;;
    seneca-myt)
    MASTERS="m0[1-5]-myt.seneca-myt.yt.yandex.net"
    NODES="n00[01-79]-myt.seneca-myt.yt.yandex.net"
    ;;
    seneca-sas)
    MASTERS="m0[1-5]h.seneca-sas.yt.yandex.net"
    NODES="n00[01-90]h.seneca-sas.yt.yandex.net"
    ;;
    perelman)
    MASTERS="m0[1-3]i.perelman.yt.yandex.net"
    NODES="n0[002-426]i.perelman.yt.yandex.net"
    PROXIES="c0[1-2]i.perelman.yt.yandex.net"
    ;;
    bohr)
    MASTERS="m0[1-3]-sas.bohr.yt.yandex.net"
    NODES="n0[001-241]-sas.bohr.yt.yandex.net"
    PROXIES="c0[1-2]-sas.bohr.yt.yandex.net"
    ;;
    vanga)
    MASTERS="m02i.vanga.yt.yandex.net,m03f.vanga.yt.yandex.net,m04h.vanga.yt.yandex.net"
    NODES="n0015i.vanga.yt.yandex.net,n0016i.vanga.yt.yandex.net,n0017i.vanga.yt.yandex.net,n0018i.vanga.yt.yandex.net,n0019i.vanga.yt.yandex.net,n0020i.vanga.yt.yandex.net,n0021f.vanga.yt.yandex.net,n0022f.vanga.yt.yandex.net,n0023f.vanga.yt.yandex.net,n0024f.vanga.yt.yandex.net,n0029h.vanga.yt.yandex.net,n0030h.vanga.yt.yandex.net,n0031h.vanga.yt.yandex.net,n0032h.vanga.yt.yandex.net,n0033h.vanga.yt.yandex.net,n0034h.vanga.yt.yandex.net"
    ;;
    pythia)
    MASTERS="m01-sas.pythia.yt.yandex.net,m02-myt.pythia.yt.yandex.net,m03-man.pythia.yt.yandex.net"
    NODES="n0001-sas.pythia.yt.yandex.net,n0002-sas.pythia.yt.yandex.net,n0003-sas.pythia.yt.yandex.net,n0004-sas.pythia.yt.yandex.net,n0005-myt.pythia.yt.yandex.net,n0006-myt.pythia.yt.yandex.net,n0007-myt.pythia.yt.yandex.net,n0008-myt.pythia.yt.yandex.net,n0009-man.pythia.yt.yandex.net,n0010-man.pythia.yt.yandex.net,n0011-man.pythia.yt.yandex.net,n0012-man.pythia.yt.yandex.net"
    ;;
    markov)
    MASTERS="m01-man.markov.yt.yandex.net,m02-sas.markov.yt.yandex.net,m03-myt.markov.yt.yandex.net"
    NODES="n0001-man.markov.yt.yandex.net,n0002-man.markov.yt.yandex.net,n0003-man.markov.yt.yandex.net,n0004-man.markov.yt.yandex.net,n0005-sas.markov.yt.yandex.net,n0006-sas.markov.yt.yandex.net,n0007-sas.markov.yt.yandex.net,n0008-sas.markov.yt.yandex.net,n0009-myt.markov.yt.yandex.net,n0010-myt.markov.yt.yandex.net,n0011-myt.markov.yt.yandex.net,n0012-myt.markov.yt.yandex.net,n0013-myt.markov.yt.yandex.net"
    ;;
    hume)
    MASTERS="m[0-2][1-3]-man.hume.yt.yandex.net"
    SCHEDULERS="s0[1-2]-man.hume.yt.yandex.net"
    PROXIES="c01-man.hume.yt.yandex.net,c02-man.hume.yt.yandex.net,n0001-man.hume.yt.yandex.net,n0003-man.hume.yt.yandex.net,n0006-man.hume.yt.yandex.net,n0010-man.hume.yt.yandex.net,n0024-man.hume.yt.yandex.net,n0040-man.hume.yt.yandex.net,n0043-man.hume.yt.yandex.net,n0044-man.hume.yt.yandex.net,n0059-man.hume.yt.yandex.net,n0060-man.hume.yt.yandex.net,n0079-man.hume.yt.yandex.net,n0080-man.hume.yt.yandex.net,n0099-man.hume.yt.yandex.net,n0114-man.hume.yt.yandex.net,n0116-man.hume.yt.yandex.net,n0120-man.hume.yt.yandex.net,n0122-man.hume.yt.yandex.net,n0133-man.hume.yt.yandex.net,n0134-man.hume.yt.yandex.net,n0142-man.hume.yt.yandex.net"
    ;;
    freud)
    MASTERS="m0[1-3]i.freud.yt.yandex.net"
    NODES="n0[001-773]i.freud.yt.yandex.net"
    SCHEDULERS="s0[1-2]i.freud.yt.yandex.net"
    PROXIES="c0[1-2]i.freud.yt.yandex.net"
    ;;
    yp-sas)
    MASTERS="m0[1-5].yp-sas.yt.yandex.net"
    NODES="m0[1-5].yp-sas.yt.yandex.net"
    ;;
    *)
    echo "Error: unknown cluster \"${CLUSTER}\""
    exit 1
esac

if [ "${PROXIES}" == "" ]; then PROXIES="${MASTERS}"; fi
if [ "${SCHEDULERS}" == "" ]; then SCHEDULERS="${MASTERS}"; fi
if [ "${CONTROLLERAGENTS}" == "" ]; then CONTROLLERAGENTS="${SCHEDULERS}"; fi
if [ "${RPCPROXIES}" == "" ]; then RPCPROXIES="${NODES}"; fi

echo "Updating ${CLUSTER} to version ${VERSION} ${READONLY}"
echo "Masters: ${MASTERS}"
echo "HTTP Proxies: ${PROXIES}"
echo "Schedulers: ${SCHEDULERS}"
echo "Controller agents: ${CONTROLLERAGENTS}"
echo "Nodes: ${NODES}"
echo "RPC Proxies: ${RPCPROXIES}"

function checkversion {
    if [[ $# != 2  || "$1" == "" ]]; then return 1; fi
    out=$(pdsh -R ssh -w "$1" "$2 --version" 2>/dev/null | grep -v ${VERSION} | sort)
    echo "["`date`"]" "Other versions on $1: " $out
    if [ "${out}" = "" ]; then return 1; else return 0; fi
}
function waitversion {
    while checkversion $@; do sleep 1; done 
}
function update {
    if [[ $# != 2  || "$1" == "" ]]; then return 1; fi
    echo "Updating $1"
    pdsh -R ssh -w "$1" "$2"
}
function buildsnapshot {
    if [[ $# == 0  || "$1" == "" ]]; then return 1; fi
    master=$(pdsh -R ssh -w "$1" -q |grep -A 1 "Target nodes" | tail -n 1 | tr "," "\n" |head -n 1)
    cells=$(pdsh -R ssh -N -w "${master}" "python -c \"import yt.yson as y; f=open('/etc/ytserver_master.conf', 'r');c=y.load(f); cells=[c['primary_master']['cell_id']]; cells+=[m['cell_id'] for m in c['secondary_masters']]; print ' '.join(cells)\"")
    echo "Building snapshots ${2} on ${master} for cells ${cells}"
    if [ "${cells}" == "" ]; then return 1; fi
    for cell in ${cells}; do
        pdsh -R ssh -S -w "${master}" "sudo yt-admin build-snapshot ${2} --cell-id ${cell}"
        if [ $? -ne 0 ]; then return 1; fi
    done
    return 0
}

function checkconfig {
    if [[ $# != 1  || "$1" == "" ]]; then return 1; fi
    out=$(pdsh -R ssh -w "$1" "grep \"enable_in_memory_balancer\" /etc/ytserver_master.conf" 2>/dev/null | wc -l)
    echo "Good config on $1: " $out
    if [ "${out}" == "5" ]; then return 1; else return 0; fi
}
function waitconfig {
    while checkconfig $@; do sleep 1; done 
}
#waitconfig $MASTERS


waitversion $MASTERS ytserver-master
waitversion $PROXIES ytserver-master
waitversion $SCHEDULERS ytserver-scheduler
waitversion $CONTROLLERAGENTS ytserver-controller-agent
waitversion $NODES ytserver-node
waitversion $RPCPROXIES ytserver-proxy

buildsnapshot "${MASTERS}" "${READONLY}"
if [ $? -eq 0 ]; then echo "Snapshot built"; else echo "Snapshot failed"; exit; fi

command="restart"
update $MASTERS "sudo sv restart yt_master"
update $PROXIES "sudo sv ${command} yt_http_proxy"
update $SCHEDULERS "sudo sv ${command} yt_scheduler"
update $CONTROLLERAGENTS "sudo sv ${command} yt_controller_agent"
update $NODES "sudo sv ${command} yt_node"
update $RPCPROXIES "sudo sv ${command} yt_proxy"

#update $NODES "sudo rm -f /var/lock/yt-is-crashing ; sudo sv restart yt_node"

