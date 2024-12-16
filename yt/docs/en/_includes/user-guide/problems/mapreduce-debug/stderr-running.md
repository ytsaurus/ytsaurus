## Getting the current stderr of a running job

As an operation runs, there may be situations where there's still one incomplete job left while the rest have already finished. In {{product-name}}, it's possible to get the stderr that the job has written up to a given moment.

You can do that with a special mode implemented in the `yt` command: `yt get-job-stderr`.

```bash
yt get-job-stderr --operation-id 35060d89-f9328e09-3f403e8-6f4eb4b5 --job-id fe270b54-a938652-3fc0384-2144
```

Stderr in the output is truncated according to the same rules that apply when you save files in Cypress: the first few megabytes plus the last few megabytes are shaved off.
