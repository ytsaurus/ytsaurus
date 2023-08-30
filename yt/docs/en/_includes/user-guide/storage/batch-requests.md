# Batch processing of requests

This section contains information about batch processing of requests (batch requests), with usage examples for the [Python](../../../user-guide/storage/batch-requests.md#python_api), [C++](../../../user-guide/storage/batch-requests.md#c++_api), and [Java](../../../user-guide/storage/batch-requests.md#java_api) APIs.

When working with the {{product-name}} system, each command generates a separate request to the master server. A request has its own cost, often higher than the execution of a command. Therefore, combining several commands into a single request can significantly speed up processes that send many easy commands to [Cypress](../../../user-guide/storage/cypress.md) and are waiting for a response most of the time. This request is called a **batch request**. The Cypress master server will execute the commands from the batch request in random order and return all the obtained results. Errors that occur during the execution of individual commands do not affect other commands.

## Concurrency

A batch request on the Cypress master server is executed as a set of independent commands, the specific command execution order is not guaranteed. All commands are counted in the user command quota. A batch request has the `concurrency` parameter which regulates the concurrency of commands. The parameter value limits the number of concurrently running commands, thus enabling you not to exceed the allowable number of concurrent requests to the master server for a user. Otherwise, the `User 'user' has exceeded its request queue size limit` error will be returned in response to the request. The default value of the parameter is 50 (concurrently running commands) and the user has a limit of 100, which helps avoid errors involving exceeding the limit. Exceeding the limit is not a fatal error. If it occurs, we recommend sending a request again. If you use high-level SDKs, the request will be sent again automatically.

## Python API { #python_api }

In the Python API, there is a `create_batch_client()` method that enables you to create a client to combine multiple commands into a single batch request (`client.create_batch_client()`) if you have a simple client (`client = yt.YtClient(<cluster-name>)`). A usage example is shown in Listing 1:

```python
import yt.wrapper as yt

if __name__ == "__main__":
    client = yt.YtClient(<cluster-name>)

    batch_client = client.create_batch_client()
    list_rsp = batch_client.list("/")
    exists_rsp = batch_client.exists("/")
    batch_client.commit_batch()

    print list_rsp.get_result()
    print exists_rsp.get_result()

#['cooked_logs', 'home', 'logs', 'projects', 'statbox', 'sys', 'tmp', 'user_sessions', 'userdata', 'userfeat', 'userstats']
#true
```

As a result of executing the request using batch_client, a special object of the `BatchResponse` type, which has the structure shown below, is returned.

```python
class BatchResponse(object):
    ...

    def get_result(self):
        ...

    def get_error(self):
        ...

    def is_ok(self):
        ...
```

You cannot specify the [format](../../../user-guide/storage/formats.md) of the returned data by a separate command via `batch_client`. The reason is that the format of the returned data is the same for the entire batch request.

## C++ API { #c++_api }

In the C++ API, to execute a batch request, you need to:

- Create an object of the `TBatchRequest` type.
- Use it to specify commands (the `TBatchRequest` methods return `TFuture` to the appropriate type).
- Pass it to the `ExecuteBatch` method of the client.
- Get the results from the previously obtained `TFuture`.

However, if some commands were completed with an error, you can find this out by calling `future.GetValue` or `future.HasException` (i.e. calling `ExecuteBatch` will not return an error). A usage example is given below.

```c++
#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/helpers.h>

using namespace NYT;

int main()
{
    auto client = CreateClient(<cluster-name>);

    TBatchRequest batchRequest;
    auto listRsp = batchRequest.List("/", TListOptions().MaxSize(5));
    auto existsRsp = batchRequest.Exists("//tmp");

    client->ExecuteBatch(batchRequest);

    for (auto item : listRsp.GetValue()) {
        Cerr << NodeToYsonString(item) << Endl;
    }
    Cerr << NodeToYsonString(existsRsp.GetValue()) << Endl;

    return 0;
}

/* Output of program:
"tmp"
"projects"
"logs"
"userfeat"
"cooked_logs"
%true
*/
```

## Java API { #java_api }

In the Java API, there is an `executeBatch` method that enables you to create an object of the `BatchRequest` type to execute multiple commands within a single batch request if you have a Cypress client `yt.cypress()`. All commands sent via `BatchRequest` return objects of the `future` type, which will be executed after calling the `execute` method.

```java
BatchRequest request = yt.cypress().executeBatch(transactionId, pingAncestorTransactions, Option.empty());
ListF<CompletableFuture<YTreeNode>> futures = Cf.arrayList();

for (YPath path : spec.getInputTables()) {
	futures.add(request.get(transactionId, pingAncestorTransactions, path.attribute("sorted"), Cf.set()));
}

request.execute().join();

boolean result = futures.forAll(x -> {
	YTreeNode node = x.join();
	return node.isBooleanNode() && node.boolValue();
});
```
