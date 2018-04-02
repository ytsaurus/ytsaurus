#!/usr/bin/python

from time import sleep
from random import randint
from random import random
from random import choice
from subprocess import call
from subprocess import check_output
import datetime
import argparse

MAX_DOWN_NODES = 2

def start_node(address):
	print "*** Starting {0}".format(address)
	call(["ssh", address, "sudo sv start yt_node"])

def stop_node(address):
	print "*** Stopping {0}".format(address)
	call(["ssh", address, "sudo sv stop yt_node"])

def restart_node(address):
	print "*** Restarting {0}".format(address)
	call(["ssh", address, "sudo sv restart yt_node"])

def restart_scheduler(address):
	print "*** Restarting {0}".format(address)
	call(["ssh", address, "sudo sv restart yt_scheduler"])

def restart_controller_agent(address):
	print "*** Restarting {0}".format(address)
	call(["ssh", address, "sudo sv restart yt_controller_agent"])

def get_addresses(cluster, role):
    output = check_output(["curl", "-qs", "http://c.yandex-team.ru/api/groups2hosts/yt_{0}_{1}".format(cluster, role)])
    return output.strip().split()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Randomly restart nodes and possibly schedulers.")
    parser.add_argument("--restart-nodes", action="store_true", default=False, help="Restart nodes")
    parser.add_argument("--restart-schedulers", action="store_true", default=False, help="Restart schedulers")
    parser.add_argument("--restart-controller-agents", action="store_true", default=False, help="Restart controller agents")
    parser.add_argument("--sleep-time", type=float, default=2, help="Sleep time in seconds (float)")
    parser.add_argument("cluster", type=str, help="Lowercase cluster name")

    args = parser.parse_args()
    cluster = args.cluster
    restart_nodes = args.restart_nodes
    restart_schedulers = args.restart_schedulers
    sleep_time = args.sleep_time

    up_nodes = set(get_addresses(cluster, "nodes") + get_addresses(cluster, "data_proxy"))
    down_nodes = set()
    schedulers = set(get_addresses(cluster, "schedulers"))
    #controller_agents = set(get_addresses(cluster, "controller_agents"))
    controller_agents = schedulers

    print datetime.datetime.now().isoformat()
    while True:
        r = randint(0, 16)
        if r < 15:
            mode = r // 5
        elif r == 15:
            mode = 3
        else:
            mode = 4
        if mode == 0:
            if not restart_nodes:
                continue
            # Stop random node
            if len(down_nodes) >= MAX_DOWN_NODES:
                continue
            node = choice(list(up_nodes))
            stop_node(node)
            down_nodes.add(node)
            up_nodes.remove(node)
        elif mode == 1:
            if not restart_nodes:
                continue
            # Start random node
            if len(down_nodes) == 0:
                continue
            node = choice(list(down_nodes))
            start_node(node)
            down_nodes.remove(node)
            up_nodes.add(node)
        elif mode == 2:
            if not restart_nodes:
                continue
            # Restart random node
            node = choice(list(up_nodes))
            restart_node(node)
        elif mode == 3:
            if not restart_scheduler:
                continue
            scheduler = choice(list(schedulers))
            restart_scheduler(scheduler)
        elif mode == 4:
            if not restart_controller_agent:
                continue
            controller_agent = choice(list(controller_agents))
            restart_controller_agent(controller_agent)

        sleep(sleep_time * (0.75 + 0.5 * random()))
        print datetime.datetime.now().isoformat()


