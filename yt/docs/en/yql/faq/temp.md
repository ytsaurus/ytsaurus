# Temporary data

## Directory for temporary data

Using the `yt.TmpFolder` pragma, the user can set a directory for hosting temporary tables and files. This might be needed in the following cases:
* There is no access to the `//tmp` directory of the {{product-name}} cluster.{% if audience == "internal" %} This is typically the case with external employees or robots.{% endif %}
* The query is processing tables with sensitive data. By default, temporary tables and files are saved to //tmp where any cluster user can access them. To avoid disclosure of sensitive information, protect the temporary file directory used in your queries by an ACL.

## Control of TmpFolder directory growth

When you use the `yt.TmpFolder` pragma in your query, the specified directory begins to accumulate data gradually. The following subdirectories are created in it:

* query_cache: [Query cache](../syntax/pragma#querycache).
* tmp: Temporary tables that exist only while the query is running.
* new_cache: {{product-name}} API file cache:
    * YQL executables.
    * Custom and system UDFs.
    * Files attached to the query.
    * Tables used in MapJoin (if transmitted not in a {{product-name}}-native manner).

The query_cache subdirectory and its contents' time-to-live are managed by the [yt.QueryCacheMode](../syntax/pragma#querycache) and [yt.QueryCacheTtl](../syntax/pragma#ytquerycachettl) pragmas. For `yt.QueryCacheMode="disable"`, the query cache is fully disabled and this subdirectory isn't created or used. The `yt.QueryCacheTtl` pragma enables you to limit the tables' TTL in the query cache.

Once the query is completed, the query's temporary tables are cleared automatically from the tmp subdirectory. Exceptions are the queries with the `yt.ReleaseTempData="never"` and tables delivered in query results as "Full result". You can manage time-to-live for these tables using the [yt.TempTablesTtl](../syntax/pragma#yttemptablesttl) pragma. You can override the path to temporary tables using the [yt.TablesTmpFolder](../syntax/pragma#yttablestmpfolder) pragma.

For objects inside new_cache, time-to-live is managed by the [yt.FileCacheTtl](../syntax/pragma#ytfilecachettl) pragma. To exclude executable YQL and UDF files from this directory, use the [yt.BinaryTmpFolder](../syntax/pragma#ytbinarytmpfolder) pragma to set up a separate directory and TTL for them (see [yt.BinaryExpirationInterval](../syntax/pragma#ytbinaryexpirationinterval)). You can also move your tables used in MapJoin to a separate directory by the [yt.TableContentTmpFolder](../syntax/pragma#yt.tablecontenttmpfolder) pragma. Keep in mind, however, that such tables can include sensitive data.

{% if audience == "internal" %}
If you don't use TTL, you can set up a script to periodically clean your temporary directory. To learn about the cleanup procedure and get a link to the cleanup script, see the [section]({{yt-docs-root}}/user-guide/storage/regular-system-processes#tmp_cleaning).
{% endif %}
