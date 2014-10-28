#!/bin/bash

set -ex

version=$1
version="%yt.version%"
[[ -z "$version" ]] && echo "Please, specify package version." && exit 1

sandbox=$(mktemp -d)
trap "rm -rf ${sandbox}" 0 1 2 3 15

changes_out="yandex-yt_${version}_amd64.changes"
changes_url="http://dist.yandex.net/yt-precise/unstable/${changes_out}"
changes_url=${changes_url/+/\%2b}

curl -sL "${changes_url}" > "${sandbox}/${changes_out}"

cat "${sandbox}/${changes_out}" \
  | awk '/^Files:/ { f = 1 } /^$/ { f = 0 } { if (f > 0) { if (f++ > 1) { print } } }' \
  | awk '{ print $5 }' \
  | while read deb_out; do
  deb_url="http://dist.yandex.net/yt-precise/unstable/amd64/${deb_out}"
  deb_url=${deb_url/+/\%2b}
  curl -sSL "${deb_url}" > "${sandbox}/${deb_out}"
done

chmod -R a+rwx "${sandbox}"

sudo -H -u teamcity dupload --to yabs-precise --nomail --noarchive "${sandbox}/${changes_out}"

echo "##teamcity[buildStatus text='{build.status.text}; Package: ${version}']" >&2
