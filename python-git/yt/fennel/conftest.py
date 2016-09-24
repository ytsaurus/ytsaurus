import sys

if sys.version_info[:2] <= (2, 6):
    collect_ignore = ["test_new_fennel.py"]
