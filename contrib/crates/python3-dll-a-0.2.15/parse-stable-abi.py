#!/usr/bin/env python3
# Parses Python Stable ABI symbol definitions from the manifest in the CPython repository located at https://github.com/python/cpython/blob/main/Misc/stable_abi.toml
# and produces a definition file following the format described at https://docs.microsoft.com/en-us/cpp/build/reference/module-definition-dot-def-files.
import sys
import tomli

stable_abi = tomli.load(sys.stdin.buffer)

print("LIBRARY python3.dll")
print("EXPORTS")

count = 0

for function in stable_abi["function"].keys():
    print(function)
    count += 1

for data in stable_abi["data"].keys():
    print(f"{data} DATA")
    count += 1

assert count >= 859
