
------
#### **Q: How do I add a column to a {{product-name}} table?**

**A:** You need to retrieve the current table schema:

`yt get //home/maps-nmaps/production/pedestrian/address_tasks/@schema --format '<format=text>yson'`

And replace it with a new one:
```bash
yt alter-table //home/maps-nmaps/testing/pedestrian/address_tasks --schema '<"unique_keys"=%false;"strict"=%true;>[{"name"="timestamp";"required"=%false;"type"="uint64";"sort_order"="ascending";};{"name"="lon";"required"=%false;"type"="double";};{"name"="lat";"required"=%false;"type"="double";};{"name"="buildingId";"required"=%false;"type"="string";};{"name"="taskId";"required"=%false;"type"="string";};{"name"="pedestrianType";"required"=%false;"type"="string";};{"name"="buildingPerimeter";"required"=%false;"type"="double";};{"name"="buildingShape";"required"=%false;"type"="string";};]'
```

------
#### **Q: I get the following error when I attempt to change a table schema: Changing "strict" from "false" to "true" is not allowed. What should I do?**

**A:** You cannot change the schema of a non-empty table from weak to strict since the entire table must be read to validate this action to make sure that the data match the new schema. It is easiest to create a new table and copy data from the old one via a read+write or by launching a [Merge](../../../user-guide/data-processing/operations/merge.md) transaction.

------
#### **Q: How do I authorize when communicating with YT through the console?**

**A:** You have to save the required user's token to a file called `~/.yt/token`.

------
#### **Q: Can I reduce the replication factor for temporary tables in the native C++ wrapper?**

**A:** No, the wrapper does not include this capability.

------
#### **Q: The python wrapper produces error "ImportError: Bad magic number in ./modules/yt/__init__.pyc". What should I do?**

**A:** This error results from a python version mismatch between the client running the script and your cluster. To run, use the same python version as on the cluster. You can retrieve the current version as follows:

```bash
yt vanilla --tasks '{master = {job_count = 1; command = "python --version >&2"}}'
Python 2.7.3
```

Python versions on different cluster nodes may differ. It is better to use your own proto layer to run jobs.

------
#### **Q: What is the overhead for reading small table ranges and small tables?**

**A:** Small ranges do not create overhead since only the relevant blocks are read from disk. At the same time, all static table reads require requests for metadata to the master server. In this case, communication with the master server is the bottleneck. Therefore, we recommend using a smaller number of queries (larger parts) to read static tables minimizing master server load. Or restructuring your process to read from dynamic tables.

------
#### **Q: Can hex numbers be stored efficiently in keys instead of strings?**

**A:** They can using the [YSON](../../../user-guide/storage/formats.md#yson) or the [JSON](../../../user-guide/storage/formats.md#json) format.

------
#### **Q: What are the logging levels in the console client and how can I select them?**

**A:** You can select logging levels via the `YT_LOG_LEVEL` environment variable with `INFO` being the default. You can change the logging level using the `export YT_LOG_LEVEL=WARNING` command. The following logging levels are available:

- `INFO`: to display transaction execution progress and other useful information.
- `WARNING`: to display warnings. For instance, a query could not be run and is being resubmitted, or a transaction input table is empty. Errors of these types are not critical, and the client can continue running.
- `ERROR`: all errors that cause the client to fail. Errors of this type result in an exception. The exception is handled, and the client exits returning a non-zero code.

------
#### **Q: How does the clearing of temporary data work on clusters?**

**A:** Most {{product-name}} clusters regularly (twice or more a day) run the `//tmp` cleaning script that finds and deletes tmp data that have not been used in a long time or use up a certain portion of the account's quota. For a detailed description of the cleaning process, please see the System processes section. When writing data to `//tmp`, users have to keep this regular cleaning in mind.

------
#### **Q: When reading a small table using the read command, the client freezes up in a repeat query. What could be the reason?**

**A:** One of the possible common reasons is too many (small) chunks in a table. We recommend running the [Merge](../../../user-guide/data-processing/operations/merge.md) operation with the `--spec '{force_transform=true}'` option. When such tables come up in operation output, the console client issues a warning containing, among other things, the command you can run to increase the size of the table's chunks. You can also specify the `auto_merge_output={action=merge}` option to have a merge occur automatically.

------
#### **Q: An operation returns error "Account "tmp" is over disk space (or chunk) limit". What is going on?**

**A:** Your cluster has run out of storage space for temporary data (tmp account), or the account has too many chunks. This account is accessed by all the cluster users which may cause it to become full. You have to keep this in mind, and if using up the tmp quota is critical to your processes, we recommend using a different directory in your own account for temporary data. Some APIs use `//tmp` as the default path for storing temporary data. If that is the case, you must reconfigure these to use subdirectories in your project's directory tree.

------
#### **Q: An operation fails with error "Account "intermediate" is over disk space (or chunk) limit". What is going on?**

**A**: Your cluster has run out of storage space for intermediate data (`intermediate` account), or the account has too many chunks.
Unless you specified `intermediate_data_account` (see [Operation settings](../../../user-guide/data-processing/operations/operations-options.md), [Sort](../../../user-guide/data-processing/operations/sort.md), [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md)), you are sharing this account with everybody else. To preempt this problem, set `intermediate_data_account`.

------
#### **Q: Is reading a table (or file) a consistent operation in {{product-name}}? What will happen if I am reading a table while deleting it at the same time?**

**A:** On the one hand it is. That is to say that if a read successfully completes, the read will return exactly the data contained in the table or file at the start of the operation. On the other hand, the read may terminate if you delete the table at that time. To avoid this, you need to create a transaction and have it take out a snapshot lock on the table or file. When you are using the python API, including the python CLI, with retry activated for reads, this lock is taken out automatically.

------
#### **Q: When I start a client, I get "Cannot determine backend type: either driver config or proxy url should be specified". What should I do?**

**A:** Check to see whether the `YT_PROXY=<cluster-name>` environment variable is set.

------

{% if audience == public %}

#### **Q: What do I do if a query from the python wrapper fails with "gaierror(-2, 'Name or service not known')"?**

**A:** If you are getting an error that looks like
`has failed with error <class 'yt.packages.requests.exceptions.ConnectionError'>, message: '('Connection aborted.', gaierror(-2, 'Name or service not known'))'`, or
"Name or service not known", it is a DNS error that indicates that the requested DNS record is not found.
Given {{product-name}} and reliable DNS, this most probably means that your service is attempting to resolve a record for a {{product-name}} host but {{product-name}} is an ipv6 only service, hence this is not working. You service should work properly with an ipv6 network. In addition, the YT_FORCE_IPV4 environment variable may be set switching yt.wrapper to the ipv4 only mode. It must be eliminated.
To view the {{product-name}} environment variable, run the following command in your terminal:
`env | grep YT_`
To remove the YT_FORCE_IPV4 variable from the environment:
`unset YT_FORCE_IPV4`

{% endif %}

------
#### **Q: What do I do if I get error "Account "..." is over disk space limit (node count limit, etc)"?**

**A:** This message is an indication that the account is out of one of its quotas. The system has quotas for all kinds of resources. For more information on the types of quotas and for forms to change quotas, see the [Quotas](../../../user-guide/storage/quotas.md) section.

------
#### **Q: How do I discover who is taking up space in an account or all the nodes with a specific account?**

**A:** The short answer is `yt find / --name "*" --account <account_name>`

A more detailed answer:
1. Look in the recycle bin (`//tmp/trash/by-account/<account_name>`). To do this, follow the specified path in the web interface's Navigation section.
2. Use `yt find` to look for your account's tables in `//tmp` and your associated project directories. Please note that `yt find` does not look in directories to which you do not have access.
3. Contact the system administrator.

------
#### **Q: How do I change a table's account?**

**A:** `yt set //path/to/table/@account my-account`

------
#### **Q: How do I find what part of my resources is being used by a directory together with all the tables, files, and so on it contains?**

**A:** `yt get //path/to/dir/@recursive_resource_usage` или выбрать **Show all resources** в разделе **Navigation** в веб-интерфейсе.

------
#### **Q: While working with the system, I am getting "Transaction has expired or was aborted". What does it mean and how should I deal with it?**

**A:** When you create a master transaction, you specify a **timeout**, and a user undertakes to ping the transaction at least once during the specified time interval. If the interval between the moment of creation or the most recent ping is greater than the timeout, the system will terminate the transaction.  There may be several things causing this behavior:

1. Network connection issues between client machine and cluster.
2. Client-side issues, such as failure to ping. Pings are either not being sent from the code, or the pinging code is not being called. When using python for instance, this might happen, if there is a long-term GIL lock taken out inside a program to work with some native libraries.
3. There is some maintenance underway on the cluster.
4. There are known access issues.
5. The transaction was explicitly aborted by the client which you can ascertain by reviewing server logs.

Otherwise, client debug logs are required for a review of the issue. The server side only knows that there are no pings; therefore, you must make sure to activate debug logging and collect the logs. By setting `YT_LOG_LEVEL=debug`, for instance, which is suitable for most supported APIs.

-----
#### **Q: The operation page displays "owners" field in spec ignored as it was specified simultaneously with "acl"". What does it mean?** { #ownersinspecignored }

**A:** This message means that the operation spec includes both the deprecated "owners" field and the "acl" field with preference given to the latter, which means that "owners" was ignored.

------
#### **Q: How do I automatically rotate nodes deleting those that are older than a certain age?**

**A:** You should use the `expiration_time` attribute. For more information, see [Metainformation tree.](../../../user-guide/storage/cypress.md#TTL)

------
#### **Q: How do I automatically delete nodes that have not been in use longer than a specified period of time?**

**A:** You should use the `expiration_timeout` attribute. For more information, see [Metainformation tree.](../../../user-guide/storage/cypress.md#TTL)

------
#### **Q: A running operation displays warning "Detected excessive disk IO in <job_type> jobs. IO throttling was activated». What does it mean?** { #excessivediskusage }

**A:** The operation's jobs are performing many input/output transactions against the local drive. To minimize the negative impact of such behavior on the cluster, jobs were restricted via the [blkio cgroup throttling](https://www.kernel.org/doc/Documentation/cgroup-v1/blkio-controller.txt) mechanism. For possible reasons of such behavior, please see the examples in the [Job statistics](../../../user-guide/problems/jobstatistics.md#excessive_io) section.


------
#### **Q: Which paths does the web interface correct automatically with the Enable path autocorrection setting enabled?** { #enablepathautocorrection }

**A:** The web interface does not specify which path errors have been corrected.
For instance, a path looking like `//home/user/tables/` is always invalid. When the path is displayed in the web interface, the unescaped slash at the end of the path will be stripped.

------
#### **Q: How do I find out whether a table from the web interface has successfully downloaded, and if not, look at the error?** { #web_interface_table_download }

**A:** If there is an error, it will be written to the file being downloaded. An error may occur at any time during a download. You need to check the end of the file. Example error:

```json
==================================================================" "# "" ""#
{
    "code" = 1;
    "message" = "Missing key column \"key\" in record";
    "attributes" = {
        "fid" = 18446479488923730601u;
        "tid" = 9489286656218008974u;
        "datetime" = "2017-08-30T15:49:38.645508Z";
        "pid" = 864684;
        "host" = "<cluster_name>";
    };
}
==================================================================" "# "" ""#
```

------
#### **Q: You are attempting locally to run a program that communicates via RPC in {{product-name}} and get back "Domain name not found»**

**A:** In a log, you may also encounter `W Dns DNS resolve failed (HostName: your-local-host-name)`. The error occurs when resolving the name of the local host that is not listed in global DNS. The thing is that the {{product-name}} RPC client uses IPv6 by default and disables IPv4. That is why the line  `127.0.1.1  your-local-host-name` in the local `/etc/hosts` file does not work. If you add `::1  your-local-host-name` to the above file, it should solve your problem.

------
#### **Q: How do I copy a specified range of rows rather than the entire table?**

**A:** In the current implementation, the `Copy` operation does not support the copying of ranges but you can use the [Merge](../../../user-guide/data-processing/operations/merge.md) command which will run quickly. Using the `ordered` mode will keep the data sorted in simple situations (when there is a single range, for instance). Example command:

```bash
yt merge --src '_path/to/src/table[#100:#500]' --dst _path/to/dst/table --mode ordered
```

------
#### **Q: How do I find out who is processing my tables?**

**A:** To accomplish this, you can analyze the master server access log.

------
#### **Q: How do I recover deleted data?**

**A:** If the delete used the UI, and the `Delete permanently` option was not selected, you can look for the tables in the recycle bin under the relevant account folder.
If the delete used `yt remove` or similar API calls, recovery is **not possible**.

------
#### **Q: Error "Operations of type "remote-copy" must have small enough specified resource limits in some of ancestor pools"**

**A:** [RemoteCopy](../../../user-guide/data-processing/operations/remote-copy.md) operations create load in the cross DC network.
To limit the load, an artificial load limit was introduced: RemoteCopy operations must run in a pool with the `user_slots` limit not exceeding `2000`.
If the plan is only to run RemoteCopy in the pool, it is sufficient to set this limit for the pool
`yt set //sys/pools/..../your_pool/@resource_limits '{user_slots=2000}'`.
