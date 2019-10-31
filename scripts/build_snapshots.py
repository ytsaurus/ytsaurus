import yt.wrapper as yt
from yt.wrapper.http_helpers import get_token

import argparse
import json
import subprocess
import time
import yt.packages.requests as requests


def build_snapshot(proxy, cell, set_read_only):
    cell_id = cell['cell_id']
    print('Started building snapshot for {0}'.format(cell_id))

    url = 'https://' + proxy + "/api/v4/build_snapshot"

    params = {
        "cell_id": cell_id,
        "set_read_only": set_read_only,
        "wait_for_completion": False
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="proxy", required=True)
    parser.add_argument("--read-only", help="read_only", action="store_true", default=False)
    args = parser.parse_args()

    yt.config["proxy"]["url"] = args.proxy
    read_only = args.read_only

    proxy = yt.config["proxy"]["url"]
    pref = '//sys/'

    cells = {}
    all_ = yt.get(pref + '@cluster_connection')

    for m in [all_['primary_master']] + all_['secondary_masters']:
        cells[m['addresses'][0]] = {'cell_id': m['cell_id']}

    secondary_masters = yt.list(pref + "secondary_masters")
    paths = [pref + "primary_masters"] + \
        [pref + "secondary_masters/{}".format(id) for id in secondary_masters]

    for path in paths:
        master = yt.list(path)[0]
        if master in cells:
            monitoring_path = path + '/' + master + '/orchid/monitoring/hydra'
            cells[master]['monitoring_path'] = monitoring_path

    total = len(cells)

    while True:
        cnt = 0
        for address, cell in cells.iteritems():
            if 'snapshot_id' in cell:
                cnt += 1
                continue

            expected_snapshot_id = cell.get('expected_snapshot_id', -2)

            monitoring_path = cell['monitoring_path']
            res = yt.get(monitoring_path)
            version = res['logged_version']
            prev_snapshot_id, mut_cnt = int(version.split(':')[0]), int(version.split(':')[1])
            snapshot_id = res['last_snapshot_id']
            if res['building_snapshot']:
                continue

            if ((mut_cnt == 0 or not read_only) and snapshot_id > 0 and
                    prev_snapshot_id == snapshot_id and expected_snapshot_id == snapshot_id
                    and res['read_only'] == read_only):
                cell['snapshot_id'] = snapshot_id
                print("Built {0} snapshot for {1} cell ({2})".format(snapshot_id,
                    cell['cell_id'], address))
                cnt += 1
            elif not res['read_only']:
                build_snapshot(proxy, cell, read_only)

        if cnt == total:
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
