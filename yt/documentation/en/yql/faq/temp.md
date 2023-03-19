---
vcsPath: yql/docs_yfm/docs/ru/yql-product/faq/temp.md
sourcePath: yql-product/faq/temp.md
---
# Temporary data

## Directory for temporary data

Using the `yt.TmpFolder` pragma, the user can set a directory for hosting temporary tables and files. This might be needed in the following cases:
* No access to the `//tmp` directory of the {{product-name}} cluster. Usually, those are external employees or robots.
* The query is processing tables with sensitive data. Temporary tables and files are saved to //tmp where any Yandex employee can access them. To avoid disclosure of sensitive information, protect the temporary file directory used in your queries by an ACL.

## Control of TmpFolder directory growth

When you use the `yt.TmpFolder` pragma in your query, the specified directory begins to accumulate data gradually. The following subdirectories are created in it:

* query_cache: [Query cache](../syntax/pragma#querycache)
* tmp: Temporary tables that only exist while the query is running
* new_cache: {{product-name}} API file cache:
   * YQL executable files
   * Custom and system UDFs
   * Files attached to the query
   * Tables used in MapJoin, unless transmitted in a {{product-name}}-native manner

The query_cache directory and its contents time-to-live are managed by the [yt.QueryCacheMode](../syntax/pragma#querycache) and [yt.QueryCacheTtl](../syntax/pragma#ytquerycachettl) pragmas. For `yt.QueryCacheMode="disable"`, the query cache is fully disabled and this subdirectory isn't created or used. The `yt.QueryCacheTtl` pragma enables you to limit the lifetime of tables in the query cache.

Once the query is completed, the query's temporary tables are cleared automatically from the tmp subdirectory. Exceptions are the queries with the `yt.ReleaseTempData="never"` and tables delivered in query results as "Full result". You can manage time-to-live for these tables using the [yt.TempTablesTtl](../syntax/pragma#yttemptablesttl) pragma. You can override the path to temporary tables using the [yt.TablesTmpFolder](../syntax/pragma#yttablestmpfolder) pragma.

For objects inside new_cache, time-to-live is managed by the [yt.FileCacheTtl](../syntax/pragma#ytfilecachettl) pragma. To exclude executable YQL and UDF files from this directory, use the [yt.BinaryTmpFolder](../syntax/pragma#ytbinarytmpfolder) pragma to set up a separate directory and TTL for them (see [yt.BinaryExpirationInterval](../syntax/pragma#ytbinaryexpirationinterval)). You can also move your tables used in MapJoin to a separate directory by the [yt.TableContentTmpFolder](../syntax/pragma#yt.tablecontenttmpfolder) pragma. Keep in mind, however, that such tables can include sensitive data.

If you don't use TTL, you can set up a script to clean your temporary directory periodically.
