# Chunk auto-merge on the master server side

For static tables, {{product-name}} supports automatic chunk merge on the master server side. Unlike the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation, this method doesn't use the computing resources of the pool.

To enable automatic chunk merge, set the `@chunk_merger_mode` table attribute to one of the values below:

- **shallow**: Merge chunks at the metadata level, without recompression. This approach works only if the chunks are "similar" enough (have the same sort order, schema, compression codec, etc.). The resulting chunk may also have too many [blocks](../../../../user-guide/storage/chunks.md#chunk-size) at a certain point.
- **deep**: Merge chunks with full recompression (by reading all input chunks and writing to a new one). This mode is much more expensive than the previous one, but it allows you to merge chunks with different characteristics if they end up in the same table. You can also use this mode to remove small blocks or chunks in older formats.
- **auto** (recommended): Merge chunks in shallow mode with rollback to deep mode in case of failure.

 We recommend using `auto` by default (other modes are allowed as well, but you should switch to them only if you have a good idea of why you need to do that):

```bash
$ yt set //home/dir_with_tables/table_i_want_to_merge/@chunk_merger_mode auto
```

If you need to merge all tables in a particular subtree, you can set the attribute for the entire directory:

```bash
$ yt set //home/dir_with_tables/@chunk_merger_mode auto
```

{% note info "Note" %}

This mechanism works only for static tables. Dynamic tables have their own chunk merge mechanisms. Setting the `chunk_merger_mode` attribute for them won't do anything.

{% endnote %}

Please note that `chunk_merger_mode` is an [inheritable attribute](../../../../user-guide/storage/chunks.md#common). Once it is set, all new tables in the directory will inherit its value, but nothing will change for the old ones, meaning you should go over all the tables when enabling this attribute.

Chunks are merged when the attribute is set and every time a write is made to the table (on additional writes, the system will attempt to remerge only the end of the table for optimization purposes).

{% if audience == "internal" %}
You should also note that automatic merge won't work for a `tmp` account (but is allowed by default for all other accounts).

{% else %}

## Merge attributes on the account

Every account contains attributes for controling the chunk merge mechanism:
- `allow_using_chunk_merger`: Whether it is allowed to merge chunks for tables of this account.
- `chunk_merger_node_traversal_concurrency`: The number of tables for which merge jobs can be scheduled concurrently.
- `merge_job_rate_limit`: The number of jobs that can be scheduled per second.

The default settings allow automatic chunk merge for every account.
{% endif %}

## About troubleshooting

Tables have several system attributes for finding out the current chunk merge status. These attributes aren't displayed in the web interface but can be queried via the CLI.

- `@chunk_merger_status`: The table status in the merge pipeline, can take the `not_in_merge_pipeline`, `awaiting_merge`, and `in_merge_pipeline` values. It may take a significant amount of time for the merger to halt all activities on the table after the chunk merge trigger activates. If you've set `chunk_merger_mode` for the table, but the chunks in it are still small, look at `chunk_merger_status` and wait for it to change to `not_in_merge_pipeline`.

- `@chunk_merger_traversal_info/chunk_count`: The number of chunks already processed by the merger and which won't be merged any further. You can use this to understand which chunks won't be merged.

Suppose a merge was performed on a table and left it with 1000 chunks, and now you want to append data to the end of the table. There's no point in going over the chunks from the very beginning after each write: if they weren't merged before, this won't be needed now. However, you may need to merge a newly added chunk with some of the last chunks from the old ones. If `@chunk_merger_traversal_info/chunk_count` is, let's say, 980, then only the last 20 chunks will be considered in the future to become merged with new chunks after future writes.

Automatic chunk merge doesn't acquire locks on tables, so chunk merge is unnoticed and does not interfere with other processes. This, however, has a negative side effect: such merges may occasionally fail (for example, if the table was completely overwritten with new data during the merge). The system automatically retries failed merges if possible (this may not be possible if the table was deleted or became dynamic). However, if the table is often overwritten with new data, this process can take a long time to complete.

{% if audience == "public" %}

## Metrics in Prometheus

The following time series are used in Prometheus to collect chunk merge metrics:
- `yt.chunk_server.chunk_merger_account_queue_size`: Account-based chunk merge queues. If the account queue is empty, make sure that the correct attribute is set for all required tables. Also make sure that the account settings allow table merges: the `allow_using_chunk_merger` attribute is set to `true` and the `chunk_merger_node_traversal_concurrency` and `merge_job_rate_limit` attributes aren't set to 0.

- `yt.chunk_server.chunk_merger_chunk_replacements_succeeded`: The number of successful merges.

- `yt.chunk_server.chunk_merger_chunk_replacements_failed`: The number of failed merges.

Having a certain number of failed merges is normal, and you can't get rid of them completely. But if there are more failed merges than successful ones, this is something that you want to take note of. One possible reason could be that chunk merge is enabled for tables that are constantly overwritten from scratch with new data or for tables with a short lifetime. In this case, it's best to disable automatic merge for such tables.

{% else %}

You can see account-based chunk merge queues on this [graph](https://monitoring.yandex-team.ru/projects/yt/dashboards/monkq434ofbsaj1lfjvt/view/graph/4iotctsws/queries?from=now-30m&to=now&p.cluster=hahn&p.cell_tag=-&p.cell_id=%2A&p.container=-&p.account=-&refresh=60000). If your account queue is empty, make sure that the correct attribute is set for all required tables (and if it is set, but the queue is still always empty and the tables don't merge, create a ticket in YTADMINREQ).

You can see the number of successful and failed merges on another [graph](https://monitoring.yandex-team.ru/projects/yt/dashboards/monkq434ofbsaj1lfjvt/view/graph/ykbmokv3d/queries?cluster=hahn&p.cell_tag=-&p.cell_id=%2A&p.container=-&p.account=-&refresh=60000). Having a certain number of failed merges is normal, and you can't get rid of them completely. But if there are more failed merges than successful ones, this is something that you want to take note of. One possible reason could be that chink merge is enabled for tables that are constantly overwritten from scratch with new data or for tables with a short lifetime. In this case, it's best to disable automatic merge for such tables.

{% endif %}
