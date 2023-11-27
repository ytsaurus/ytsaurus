# Пакетная обработка запросов

В данном разделе содержится информация о пакетной обработке запросов (batch-запросах), приведены примеры использования для [Python](../../../user-guide/storage/batch-requests.md#python_api), [C++](../../../user-guide/storage/batch-requests.md#c_plus_plus_api) и [Java](../../../user-guide/storage/batch-requests.md#java_api) API.

При работе с системой {{product-name}} каждая команда порождает отдельный запрос к мастер-серверу. Запрос имеет свою стоимость, зачастую более высокую, чем выполнение команды. Поэтому объединение нескольких команд в один запрос может существенно ускорить процессы, которые посылают много легких команд к [Кипарису](../../../user-guide/storage/cypress.md), и основную часть времени находятся в ожидании ответа. Такой запрос называется **Batch-запросом**. Мастер-сервер Кипариса выполнит команды из batch-запроса в произвольном порядке и вернет все полученные результаты. Ошибки, возникающие при выполнении отдельных команд, не влияют на другие команды. 

## Concurrency

Batch-запрос на мастере Кипариса выполняется как набор независимых команд, конкретный порядок в выполнении команд не гарантируется. Все команды учитываются в квоте команд пользователя. У Batch-запроса существует параметр `concurrency`, который регулирует параллельность исполнения команд. Значение параметра ограничивает количество одновременно выполняющихся команд, тем самым позволяет не превышать допустимое число одновременных обращений к мастеру для пользователя, в противном случае в ответ на запрос вернётся ошибка `User 'user' has exceeded its request queue size limit`. По умолчанию параметр имеет значение 50 (одновременно выполняющихся команд), а ограничение у пользователя равно 100, что позволит избегать ошибок с превышением ограничения. Превышение ограничения представляет собой не фатальную ошибку, в случае ее возникновения рекомендуется делать перезапрос. При использовании высокоуровневых SDK перезапрос будет делаться автоматически.

## Python API { #python_api }

В python API доступен метод `create_batch_client()`, который позволяет, имея простого клиента (`client = yt.YtClient(<cluster-name>)`) , создать клиента для объединения нескольких команд в один batch-запрос (`client.create_batch_client()`). Пример использования представлен в листинге 1:

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

В результате выполнения запроса с помощью batch_client в ответ возвращается специальный объект типа `BatchResponse`, который обладает следующей структурой, представленной ниже.

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

Указать [формат](../../../user-guide/storage/formats.md) возвращаемых данных отдельной команды через `batch_client` нельзя. Причина в том, что формат возвращаемых данных един для всего batch-запроса.

## C++ API { #c_plus_plus_api }

В С++ API для выполнения batch-запроса необходимо:

- создать объект типа `TBatchRequest`;
- с его помощью задать команды (методы `TBatchRequest` возвращают `TFuture` на соответствующий тип);
- передать его в метод `ExecuteBatch` у клиента;
- достать результаты из полученных ранее `TFuture`.

При этом, если какие-то команды завершились с ошибкой, узнать об этом можно, вызвав `future.GetValue` или `future.HasException` (т.е. вызов `ExecuteBatch` не будет возвращать ошибку). Пример использования представлен ниже.

```c++
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/common/helpers.h>

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

В Java API доступен метод `executeBatch`, который позволяет, имея клиента для работы с Кипарисом `yt.cypress()`, создать объект типа `BatchRequest` для выполнения нескольких команд в рамках одного batch-запроса. Все команды, отправленные через `BatchRequest`, возвращают объекты типа `future`, которые будут выполнены после вызова метода `execute`.

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
