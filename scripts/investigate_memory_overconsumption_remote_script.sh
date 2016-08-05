#!/bin/bash

if ! [ -e /yt ]; then
    echo "This is script file to be run on nodes; use investigate_memory_overconsumption.sh instead.";
    exit 1
fi

pushd /yt/logs
sudo mkdir overconsumption_investigation
lst=$(ls | grep log | grep -v debug | grep -v error)
for f in $lst; do
	bash -c "
	echo $f;
	if zfgrep $1 $f; then
		echo $f>~/NODE_ID
	fi" >/dev/null 2>/dev/null &
done
while true; do
	node_log=$(cat ~/NODE_ID 2>/dev/null || echo NO)
	echo $node_log
	if [[ "$node_log" != "NO" ]]; then
		break;
	fi
	sleep 1
done

node_log=$(echo $node_log | sed -s "s/\.log/\.debug\.log/g")
echo $node_log
sudo cp $node_log overconsumption_investigation
slot=$(zfgrep $1 $node_log | egrep -o "[^ ,]*slot[^ ,]*" | head -n 1)
echo $slot
pushd $slot
lst=$(ls | grep log | grep -v debug | grep -v error)
for f in $lst; do
	bash -c "
	echo $f;
	if zfgrep $1 $f; then
		echo $f>~/JOBPROXY_ID
	fi" >/dev/null 2>/dev/null &
done
while true; do
	jobproxy_log=$(cat ~/JOBPROXY_ID 2>/dev/null || echo NO)
	echo $jobproxy_log
	if [[ "$jobproxy_log" != "NO" ]]; then
		break;
	fi
	sleep 1
done

jobproxy_log=$(echo $jobproxy_log | sed -s "s/\.log/\.debug\.log/g")
echo $jobproxy_log
sudo cp $jobproxy_log /yt/logs/overconsumption_investigation/

zfgrep $1 $jobproxy_log | grep exceeded | tail -n 1 >~/refcounted_tracker
sed -i 's/\\n/\
/g' ~/refcounted_tracker
cat ~/refcounted_tracker
echo $(whoami)@$(hostname):$(pwd) $ ls -l
ls -l /yt/logs/overconsumption_investigation
