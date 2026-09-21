This script is designed to accurately cleanup temporary objects on cluster.

Usage example:

```
./clear_tmp --account "tmp" --max-age-days 5
```

Options:
- ...
- `--dont-prune-white-list alice` - list of owners which can use `dont_prune` settings (by default - all owners)
