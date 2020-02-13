#!/usr/bin/env python2

import argparse
import json
import subprocess
import time

import yt.wrapper as yt

from yt.wrapper.http_helpers import get_token
from yt.packages import requests


class BuildSnapshotError(RuntimeError):
    pass


def build_snapshot(proxy, cell, set_read_only):
    cell_id = cell['cell_id']
    print('Started building snapshot for {0}'.format(cell_id))

    url = 'http://' + proxy + "/api/v4/build_snapshot"

    params = {
        "cell_id": cell_id,
        "set_read_only": set_read_only,
        "wait_for_snapshot_completion": False
    }

    headers = {
        'X-YT-Parameters': json.dumps(params),
        'Authorization': "OAuth " + get_token(),
    }

    r = requests.post(url, headers=headers)
    if r.status_code == 200:
        cell['expected_snapshot_id'] = json.loads(r.text)['snapshot_id']
    else:
        print(r.text)


def verify_proxy(proxy):
    url = 'http://' + proxy + "/api/v4"
    try:
        r = requests.get(url)
    except requests.exceptions.SSLError as e:
        raise BuildSnapshotError(
            "got SSL error for {url} ({err}) try to restart script from dev server".format(
                url=url,
                err=str(e),
            ))
    if r.status_code != 200:
        raise BuildSnapshotError("unexpected HTTP code: {} for {}".format(r.status_code, url))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="proxy", required=True)
    parser.add_argument("--read-only", help="read_only", action="store_true", default=False)
    args = parser.parse_args()

    yt.config["proxy"]["url"] = args.proxy

    read_only = args.read_only
    proxy = yt.config["proxy"]["url"]

    verify_proxy(proxy)

    cells = {}
    sys_preffix = '//sys/'
    cluster_connection = yt.get(sys_preffix + '@cluster_connection')

    for m in [cluster_connection['primary_master']] + cluster_connection['secondary_masters']:
        cells[m['addresses'][0]] = {'cell_id': m['cell_id']}

    secondary_masters = yt.list(sys_preffix + "secondary_masters")
    paths = [sys_preffix + "primary_masters"] + \
        [sys_preffix + "secondary_masters/{}".format(id) for id in secondary_masters]

    for path in paths:
        master = yt.list(path)[0]
        if master in cells:
            monitoring_path = path + '/' + master + '/orchid/monitoring/hydra'
            cells[master]['monitoring_path'] = monitoring_path

    expected_snapshots_count = len(cells)

    while True:
        ready_snapshot_count = 0
        for address, cell in cells.iteritems():
            if 'snapshot_id' in cell:
                ready_snapshot_count += 1
                continue

            expected_snapshot_id = cell.get('expected_snapshot_id', -2)

            monitoring_path = cell['monitoring_path']
            monitoring = yt.get(monitoring_path)

            version = monitoring['logged_version']
            prev_snapshot_id, tail = [int(part) for part in version.split(':')]

            snapshot_id = monitoring['last_snapshot_id']

            if monitoring['building_snapshot']:
                continue

            if ((tail == 0 or not read_only) and snapshot_id > 0 and
                    prev_snapshot_id == snapshot_id and expected_snapshot_id == snapshot_id
                    and monitoring['read_only'] == read_only):
                cell['snapshot_id'] = snapshot_id
                print("Built {0} snapshot for {1} cell ({2})".format(snapshot_id,
                    cell['cell_id'], address))
                ready_snapshot_count += 1
            elif not monitoring['read_only']:
                build_snapshot(proxy, cell, read_only)

        if ready_snapshot_count == expected_snapshots_count:
            return
        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except BuildSnapshotError as e:
        print "build_snapshot.py:", str(e)
        exit(1)
