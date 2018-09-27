#!/bin/bash

VERSION="$(dpkg-parsechangelog | grep Version | awk '{print $2}')"
dch -r "VERSION" "'Resigned by teamcity'"
