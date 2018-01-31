#! /usr/bin/python

import argparse

from collections import defaultdict

import yt.wrapper as yt
from yt.wrapper.config import get_config

from multiprocessing import Pool, Queue, Manager
from sys import exit, stderr

STEP = 50
POOL_SIZE = 4

def make_batch_request(queue, orchid_addresses, proxy=None):
    addresses = []
    requests = []

    for orchid_address in orchid_addresses:
        requests.append({'command' : 'get', 'parameters' : {'path' : '{}/orchid/service/version'.format(orchid_address)}})
        addresses.append(str(orchid_address).split('/')[-1])

    config = get_config(None)
    config["proxy"]["request_timeout"] = 120 * 1000
    client = yt.YtClient(proxy=proxy, config=config)
    batch_rsp = client.execute_batch(requests)

    machines_with_version = []
    errors = []
    for rsp, address in zip(batch_rsp, addresses):
        if 'output' in rsp:
            machines_with_version.append((address, rsp['output']))
        else:
            machines_with_version.append((address, 'error'))
            errors = rsp['error']

    queue.put((machines_with_version, errors))

def iter_component(orchid_addresses, proxy=None):
    manager = Manager()
    result_queue = manager.Queue()
    pool = Pool(POOL_SIZE)

    count = 0
    for start in xrange(0, len(orchid_addresses), STEP):
        pool.apply_async(make_batch_request, (result_queue, orchid_addresses[start:start + STEP]), {"proxy": proxy})
        count += 1
    pool.close()

    while count > 0:
        machines_with_version, err = result_queue.get()
        count -= 1
        for address, version in machines_with_version:
            yield address, version

    pool.join()

def iter_nodes(proxy=None):
    client = yt.YtClient(proxy=proxy, config=get_config(None))
    nodes = client.search(
        "//sys/nodes",
        depth_bound = 1,
        attributes=["state"],
        map_node_order=None,
        object_filter=lambda x: "orchid" in x and x.attributes.get('state', 'offline') == 'online')
    nodes = list(nodes)
    return iter_component(nodes, proxy)

def iter_masters(proxy=None):
    client = yt.YtClient(proxy=proxy, config=get_config(None))
    primary_masters = client.search(
        "//sys/primary_masters",
        depth_bound = 1,
        attributes=["state"],
        map_node_order=None,
        object_filter=lambda x: "orchid" in x)
    secondary_masters = client.search(
        "//sys/secondary_masters",
        depth_bound = 2,
        attributes=["state"],
        map_node_order=None,
        object_filter=lambda x: "orchid" in x)
    masters = list(primary_masters) + list(secondary_masters)
    return iter_component(masters, proxy)

def iter_schedulers(proxy=None):
    client = yt.YtClient(proxy=proxy, config=get_config(None))
    schedulers = client.search(
        "//sys/scheduler/instances",
        depth_bound = 1,
        attributes=["state"],
        map_node_order=None,
        object_filter=lambda x: "orchid" in x)
    schedulers = list(schedulers)
    return iter_component(schedulers, proxy)

def iter_rpc_proxies(proxy=None):
    client = yt.YtClient(proxy=proxy, config=get_config(None))
    rpc_proxies = client.search(
        "//sys/rpc_proxies",
        depth_bound = 1,
        attributes=["state"],
        map_node_order=None,
        object_filter=lambda x: "orchid" in x)
    rpc_proxies = list(rpc_proxies)
    return iter_component(rpc_proxies, proxy)

def print_component(iterator, verbose=False, version_filter=None):
    if version_filter:
        print "=== {} ===".format(version_filter)

    machines_by_version = defaultdict(list)
    for address, version in iterator:
        machines_by_version[version].append(address)
        if version_filter and version == version_filter:
            print address

    if verbose:
        for version, addresses in machines_by_version.iteritems():
            print "=== {} ===".format(version)
            for address in addresses:
                print address
    else:
        for version, addresses in machines_by_version.iteritems():
            print "=== {} === {} ===".format(version, len(addresses))

def main():
    parser = argparse.ArgumentParser(description="Script that fetches machine version statistics from cluster")
    parser.add_argument("--proxy", action="store", default=None, help="By default script uses the cluster mentioned YT_PROXY")
    parser.add_argument("--verbose", action="store_true", help="Print addresses of all machines that have each possible version")
    parser.add_argument("--filter", action="store", default=None, help="Print only those machines that have specified verison. Acts as if `--verbose` is set")
    parser.add_argument("component", help="Component to print versions, possible values are: nodes, masters, schedulers, rpc_proxies")
    args = parser.parse_args()

    iterator = None
    if args.component == "nodes":
        iterator = iter_nodes(proxy=args.proxy)
    elif args.component == "masters":
        iterator = iter_masters(proxy=args.proxy)
    elif args.component == "schedulers":
        iterator = iter_schedulers(proxy=args.proxy)
    elif args.component == "rpc_proxies":
        iterator = iter_rpc_proxies(proxy=args.proxy)
    else:
        print >>stderr, "Invalid component, possible values are: nodes, masters, schedulers, rpc_proxies"
        return 1

    print_component(iterator, verbose=args.verbose, version_filter=args.filter)
    return 0

if __name__ == "__main__":
    exit(main())
