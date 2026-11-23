# File parsing example

The example parses files from a blob storage. Files arrive in a queue as URLs. One parse takes
from a second to over an hour. Flow partitions by key, so every file belongs to a partition, and
the partitions are spread over the workers. A short parse behind a long one in the same partition
waits for it, while other workers may be idle. This is head-of-line blocking of long parses in
key-partitioned jobs. The example balances the parses between workers without a global queue.

## The scheme

1. `Reader` reads the input queue and writes every file to the internal queue `parse_queue` with
   `attempt = 0` and `first_seen_at` (stream `files`, sink `queue`). `first_seen_at` is the write
   time of the input row in milliseconds, which the queue source adds as the column `write_time_ms`
   (`write_time_column`); unlike the current time, it stays the same when the source replays the row.
2. `RequestReader` reads `parse_queue` into the stream `requests`. `Parser` is a process function
   computation keyed by `(url, attempt)`, so every attempt of a file lands in a pseudo-random partition.
3. `Parser` sets a swift timer (`parse_timer`) per request. The timer adds a task to the async
   processor `parse`. Its start callback is a method of the worker resource `ParseExecutor`: one
   instance per worker, shared by all partitions of that worker.
4. The executor has `slot_count` slots. It starts the waiting task with the oldest `first_seen_at`
   first. A task that does not get a slot within `max_queue_wait` completes with a rejected result
   and never starts.
5. The result handler writes a row to `parse_output` on success (stream and sink `parsed`). On
   a rejected result it sends the request back to `parse_queue` with `attempt + 1` (stream
   `retries`, sink `retry`). If `attempt + 1 >= max_attempts`, or on an error, it writes a row to
   `failed_output` (stream and sink `failed`).

```
input_queue -> Reader -> parse_queue -> RequestReader -> Parser -> parse_output
                            ^                              |   \-> failed_output
                            |                              |
                            +---- retry, attempt + 1 ------+
                                  (rejected by the executor)
```

A retried request gets a new key, so it may land in another partition, possibly on another worker
with free slots; it can also land in the same one.

Duplicates are absorbed per key `(url, attempt)` only. A new input row for a url has `attempt = 0`,
so it is absorbed only while attempt 0 of that url is in flight. If the file has already moved to a
later attempt, or its result is written, the new row is parsed separately, and both give a result.
A file with a negative size fails to parse and goes to `failed_output` at once.

## Why messages do not pile up in the queue

Requests do not wait in the queue: `Parser` settles them in swift timers and async tasks, so the
queue is read at the full speed. The executor of a worker sees all the tasks of that worker, in
all its partitions, and picks the oldest one for a free slot. For this, a task waiting in the
executor must not be limited by the async processor. It counts as running, and the default
`max_running_tasks` of the processor `parse` is 100 per job. Raise it in the dynamic spec above
the number of requests that may wait in a job.

## Parameters

| Parameter | Where | Default | Meaning |
| --- | --- | --- | --- |
| `slot_count` | `parameters` of resource `ParseExecutor` | `2` | Parses running at once on a worker. |
| `max_queue_wait` | `parameters` of resource `ParseExecutor` | `"5s"` | How long a task waits for a slot before it is rejected. |
| `parse_time_per_size_unit` | `parameters` of resource `ParseExecutor` | `"100ms"` | Duration of the stub parse per unit of file size. |
| `max_attempts` | `processing_function_parameters` of computation `Parser` | `100` | Attempts after which a rejected file goes to `failed_output`. |

Durations are YSON strings like `"5s"`; an integer means milliseconds. The dynamic spec of `Parser`
sets `async_processors = {parse = {max_running_tasks = 10000}}` and `desired_partition_count = 8`.

## Limitations

- A parse is bound to its job. A job restart, a partition move or a split cancels the parse, and
  it starts from scratch.
- The order is per worker only: the oldest `first_seen_at` goes first. There is no global queue.
- The result goes through the Flow output within one epoch, so the example suits small results.
- The parse itself is a stub: it works for `size * parse_time_per_size_unit` and returns
  `row_count = size`.

## Test

```bash
ya make -A yt/yt/flow/examples/cpp/file_parsing/test
```
