#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 0
#% SETUP_TIMEOUT = 3

echo '{do = set; path = "/list"; value = [1;2;"some string"]}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/+"; value = 100}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/-"; value = 200}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/-"; value = 500}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/2+"; value = 1000}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/3"; value = 777}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = remove; path = "/list/4"}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = remove; path = "/list/4"}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = remove; path = "/list/0"}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/+"; value = last}' | ytdriver 
echo '{do = get; path = "/list"}' | ytdriver

echo '{do = set; path = "/list/-"; value = first}' | ytdriver
echo '{do = get; path = "/list"}' | ytdriver
