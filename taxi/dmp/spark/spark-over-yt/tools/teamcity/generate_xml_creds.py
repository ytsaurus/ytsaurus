#!/usr/bin/env/ python3

import sys
import xml.etree.ElementTree as et


def main(src, dst):
    lines = src.readlines()
    username = ''
    password = ''
    for line in lines:
        key, value, *_ = line.split('=')

        key = key.strip()
        value = value.strip()

        if key == 'user':
            username = value
        elif key == 'password':
            password = value

    if not username or not password:
        raise RuntimeError('Username or password not present in artifactory deploy creds')

    settings = et.Element('settings')

    servers = et.Element('servers')
    settings.append(servers)

    server = et.Element('server')
    servers.append(server)

    id_ = et.Element('id')
    id_.text = 'yandex-spark'
    server.append(id_)

    uname = et.Element('username')
    uname.text = username
    server.append(uname)

    pwd = et.Element('password')
    pwd.text = password
    server.append(pwd)

    tree = et.ElementTree(settings)

    tree.write(dst)


if __name__ == '__main__':
    source_filename = sys.argv[1]
    dest_filename = sys.argv[2]

    with open(source_filename, 'r') as src:
        with open(dest_filename, 'wb') as dst:
            main(src, dst)
