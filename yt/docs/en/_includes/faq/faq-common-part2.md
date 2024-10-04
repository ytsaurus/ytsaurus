
------
#### **Q: What do I do if I get error "Account "..." is over disk space limit (node count limit, etc)"?**

**A:** This message is an indication that the account is out of one of its quotas. The system has quotas for all kinds of resources. For more information on the types of quotas and for forms to change quotas, see the [Quotas](../../user-guide/storage/quotas.md) section.

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

**A:** You should use the `expiration_time` attribute. For more information, see [Metainformation tree.](../../user-guide/storage/cypress.md#TTL)

------
#### **Q: How do I automatically delete nodes that have not been in use longer than a specified period of time?**

**A:** You should use the `expiration_timeout` attribute. For more information, see [Metainformation tree.](../../user-guide/storage/cypress.md#TTL)

------
#### **Q: A running operation displays warning "Detected excessive disk IO in <job_type> jobs. IO throttling was activated». What does it mean?** { #excessivediskusage }

**A:** The operation's jobs are performing many input/output transactions against the local drive. To minimize the negative impact of such behavior on the cluster, jobs were restricted via the [blkio cgroup throttling](https://www.kernel.org/doc/Documentation/cgroup-v1/blkio-controller.txt) mechanism. For possible reasons of such behavior, please see the examples in the [Job statistics](../../user-guide/problems/jobstatistics.md#excessive_io) section.


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

**A:** In the current implementation, the `Copy` operation does not support the copying of ranges but you can use the [Merge](../../user-guide/data-processing/operations/merge.md) command which will run quickly. Using the `ordered` mode will keep the data sorted in simple situations (when there is a single range, for instance). Example command:

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

**A:** [RemoteCopy](../../user-guide/data-processing/operations/remote-copy.md) operations create load in the cross DC network.
To limit the load, an artificial load limit was introduced: RemoteCopy operations must run in a pool with the `user_slots` limit not exceeding `2000`.
If the plan is only to run RemoteCopy in the pool, it is sufficient to set this limit for the pool
`yt set //sys/pools/..../your_pool/@resource_limits '{user_slots=2000}'`.
