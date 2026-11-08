# IOTracker evidence

`evidence.json`: reduced YTADMINREQ-62112 exports with verbatim component-suffix
rejection, eleven-match INFO control and IOAggregate/IORaw config. Capture IDs
and full-export SHA-256 hashes identify originals.

`synthetic` is invented; no IOTracker stream was read. Configured filenames
prove neither retention nor content.

Replay from the repository root:

```sh
python3 -B -m unittest discover -s yt/yt/tools/logslice/unittests/py -p test_iotracker_py.py
```
