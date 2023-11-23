# RPC-прокси

В данном разделе приведены примеры работы с YT через RPC-прокси для разных языков программирования: [C++](#c_plus_plus), [Java](#java), [Python](#python).

## С++ { #c_plus_plus }

Пример на [Github](https://github.com/ytsaurus/ytsaurus/blob/main/yt/examples/rpc_proxy_sample/main.cpp)

<!--[Интерфейс клиента](https://a.yandex-team.ru/arc/trunk/arcadia/yt/yt/client/api/client.h)
[Создание подключения](https://a.yandex-team.ru/arc/trunk/arcadia/yt/yt/client/api/rpc_proxy/connection.h)
[Пример](https://a.yandex-team.ru/arc/trunk/arcadia/yt/examples/rpc_proxy_sample/main.cpp)-->

В примере реализована программа, через которую можно работать с динамическими таблицами в интерактивном режиме. 

Формат вызова:

```bash
./rpc_proxy_sample --config config.yson --user <user> --token $(cat ~/.yt/token) 2> /dev/null
```

Работа с Кипарисом:

```bash
list //sys
get //sys
```

Чтение данных средствами языка запросов (SelectRows):

```sql
select timestamp, host, rack, utc_time, data FROM [//home/dev/autorestart_nodes_copy] LIMIT 10
```

Запрос строк по ключу (LookupRows):

```bash
ulookup //home/dev/autorestart_nodes_copy timestamp;host;rack;utc_time;data <id=0>1486113922563016;<id=1>"s04-sas.cluster-name";<id=2>"SAS2.4.3-13" <id=0>1486113924172063;<id=1>"s04-sas.cluster-name";<id=2>"SAS2.4.3-13" <id=0>1486113992045484;<id=1>"s04-sas.cluster-name";<id=2>"SAS2.4.3-13" <id=0>1486113992591731;<id=1>"s04-sas.cluster-name";<id=2>"SAS2.4.3-13"  <id=0>1486113997734536;<id=1>"n4137-sas.cluster-name";<id=2>"SAS2.4.3-13"
```

Вставка строк (aka WriteRows):

```bash
upsert //home/dev/autorestart_nodes_copy timestamp;host;rack;utc_time;data <id=0>123;<id=1>"host123";<id=2>"rack123";<id=3>"utc_time1";<id=4>"data1" <id=0>567;<id=1>"host567";<id=2>"rack567";<id=3>"utc_time2";<id=4>"data2"
```



Удаление данных (DeleteRows):

```bash
delete //home/dev/autorestart_nodes_copy timestamp;host;rack <id=0>123;<id=1>"host123";<id=2>"rack123"
```

Пример работы с транзакциями:

```c++
if (ValidateSignature("delete", {"path", "columns", "..."}, tokens)) {
    auto path = tokens[1];
    TPrepareRows prepareRows(tokens);

    auto tx = Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get();
    if (!ValidateResult(tx)) return;

    tx.Value()->DeleteRows(path, prepareRows.NameTable, prepareRows.Rows);
    auto result = tx.Value()->Commit().Get();
    if (!ValidateResult(result)) return;

    Cout << "Committed" << Endl;
}
```

## Java { #java }

Исходные коды представленных примеров находятся в [GitHub](https://github.com/ytsaurus/ytsaurus/tree/main/yt/java/ytsaurus-client/src/test/java/tech/ytsaurus/client).

Создание клиента:

```java
   public static YtClient createYtClient(BusConnector connector, String user, String token)
    {
        return new YtClient(connector, "cluster-name", RpcCredentials(user, token));
    }
```

Реализация Select запроса:

```java
package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

public class SelectRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsExample.class);

    public static void main(String[] args) {
        ExamplesUtil.runExampleWithBalancing(client -> {
            long t0 = System.nanoTime();
            UnversionedRowset rowset = client.selectRows(
                    "timestamp, host, rack, utc_time, data FROM [//home/dev/autorestart_nodes_copy] LIMIT 10")
                    .join();
            long t1 = System.nanoTime();
            logger.info("Request time: {}", (t1 - t0) / 1000000.0);
            logger.info("Result schema:");
            for (ColumnSchema column : rowset.getSchema().getColumns()) {
                logger.info("    {}", column.getName());
            }
            for (UnversionedRow row : rowset.getRows()) {
                logger.info("Row:");
                for (UnversionedValue value : row.getValues()) {
                    logger.info("    value: {}", value);
                }
            }
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }
        });
    }
}
```

Работа с Кипарисом:

```java
package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNodeExample {
    private static final Logger logger = LoggerFactory.getLogger(GetNodeExample.class);

    public static void main(String[] args) {
        ExamplesUtil.runExample(client -> {
            logger.info("Table dynamic: {}", client.getNode("//home/dev/autorestart_nodes_copy/@dynamic").join());
            logger.info("Table schema: {}", client.getNode("//home/dev/autorestart_nodes_copy/@schema").join());
        });
    }
}
```

Работа с Кипарисом:

```java
package ru.yandex.yt.ytclient.examples;

import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;

public class CypressExample {

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();

            OptionSpec<String> proxyOpt = parser.accepts("proxy", "proxy (see //sys/rpc_proxies)")
                    .withRequiredArg().ofType(String.class);

            OptionSet option = parser.parse(args);

            String [] hosts = null;

            if (option.hasArgument(proxyOpt)) {
                String line = option.valueOf(proxyOpt);
                hosts = line.split(",");
            } else {
                parser.printHelpOn(System.out);
                System.exit(1);
            }

            Random rnd = new Random();
            String host = hosts[rnd.nextInt(hosts.length)];

            ExamplesUtil.runExample(client -> {
                try {
                    ApiServiceTransactionOptions transactionOptions =
                            new ApiServiceTransactionOptions(ETransactionType.MASTER);

                    String node = "//tmp/test-node-cypress-example";
                    ApiServiceTransaction t = client.startTransaction(transactionOptions).get();


                    t.existsNode(node).thenAccept(result -> {
                        try {
                            if (result) {
                                t.removeNode(node);
                            }
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }).get();

                    YtGuid guid = t.createNode(node, ObjectType.Table).get();
                    /*
                    Map<String, YTreeNode> data = new HashMap<String, YTreeNode>();
                    data.put("k1", new YTreeInt64Node(10, new HashMap<>()));
                    data.put("k2", new YTreeInt64Node(31337, new HashMap<>()));
                    data.put("str", new YTreeStringNode("stroka"));
                    t.setNode(node, new YTreeMapNode(data)).get();
                    */
                    t.commit();

                    ApiServiceTransaction t2 = client.startTransaction(transactionOptions).get();
                    t2.linkNode(node, node + "-link");
                    t2.moveNode(node, node + "-moved");
                    t2.commit();

                    client.removeNode(node + "-link");

                } catch (Throwable e) {
                    System.out.println(e);
                    e.printStackTrace();
                    System.exit(-1);
                }
            }, ExamplesUtil.getUser(), ExamplesUtil.getToken(), host);

            System.exit(0);
        } catch (Throwable e) {
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
```

Выполнение LookupRows:

```java
package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

public class LookupRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(LookupRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("timestamp", ColumnValueType.INT64)
                .addKey("host", ColumnValueType.STRING)
                .addKey("rack", ColumnValueType.STRING)
                .addValue("utc_time", ColumnValueType.STRING)
                .addValue("data", ColumnValueType.STRING)
                .build();
        ExamplesUtil.runExampleWithBalancing(client -> {
            long t0 = System.nanoTime();
            LookupRowsRequest request = new LookupRowsRequest("//home/dev/autorestart_nodes_copy", schema.toLookup())
                    .addFilter(1486113922563016L, "s04-sas.cluster-name", "SAS2.4.3-13")
                    .addFilter(1486113924172063L, "s04-sas.cluster-name", "SAS2.4.3-13")
                    .addFilter(1486113992045484L, "s04-sas.cluster-name", "SAS2.4.3-13")
                    .addFilter(1486113992591731L, "s04-sas.cluster-name", "SAS2.4.3-13")
                    .addFilter(1486113997734536L, "n4137-sas.cluster-name", "SAS2.4.3-13")
                    .addLookupColumns("utc_time", "data");
            long t1 = System.nanoTime();
            UnversionedRowset rowset = client.lookupRows(request).join();
            long t2 = System.nanoTime();
            logger.info("Request time: {}ms + {}ms", (t1 - t0) / 1000000.0, (t2 - t1) / 1000000.0);
            logger.info("Result schema:");
            for (ColumnSchema column : rowset.getSchema().getColumns()) {
                logger.info("    {}", column.getName());
            }
            for (UnversionedRow row : rowset.getRows()) {
                logger.info("Row:");
                for (UnversionedValue value : row.getValues()) {
                    logger.info("    value: {}", value);
                }
            }
            for (YTreeMapNode row : rowset.getYTreeRows()) {
                logger.info("Row: {}", row);
            }
        });
    }
}
```

Выполнение ModifyRows:

```java
package ru.yandex.yt.ytclient.examples;

import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.proxy.ModifyRowsRequest;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.ytree.YTreeBuilder;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

public class ModifyRowsExample {
    private static final Logger logger = LoggerFactory.getLogger(ModifyRowsExample.class);

    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .addKey("timestamp", ColumnValueType.INT64)
                .addKey("host", ColumnValueType.STRING)
                .addKey("rack", ColumnValueType.STRING)
                .addValue("utc_time", ColumnValueType.STRING)
                .addValue("data", ColumnValueType.STRING)
                .build();
        ExamplesUtil.runExample(client -> {
            ApiServiceTransactionOptions transactionOptions =
                    new ApiServiceTransactionOptions(ETransactionType.MASTER);
            try (ApiServiceTransaction transaction = client.startTransaction(transactionOptions).join()) {
                logger.info("Transaction started: {} (timestamp={}, ping={})",
                        transaction.getId(),
                        transaction.getStartTimestamp(),
                        transaction.isPing());

                transaction.ping().join();
                logger.info("Transaction ping succeeded!");

                ModifyRowsRequest request =
                        new ModifyRowsRequest("//home/dev/autorestart_nodes_copy", schema)
                                .addInsert(Arrays.asList(10, "myhost1", "myrack1", "utc_time1", "data1"))
                                .addInsert(Arrays.asList(11, "myhost2", "myrack2", "utc_time2", "data2"))
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("timestamp").value(1486190036109192L)
                                        .key("host").value("n0344-sas.cluster-name")
                                        .key("rack").value("SAS2.4.3-15")
                                        .key("data").value("XXX " + UUID.randomUUID().toString())
                                        .buildMap()
                                        .mapValue())
                                .addUpdate(new YTreeBuilder()
                                        .beginMap()
                                        .key("timestamp").value(1486190037953802L)
                                        .key("host").value("s03-sas.cluster-name")
                                        .key("rack").value("SAS2.4.3-15")
                                        .key("data").value("XXX " + UUID.randomUUID().toString())
                                        .buildMap()
                                        .mapValue());
                long t0 = System.nanoTime();
                transaction.modifyRows(request).join();
                long t1 = System.nanoTime();

                logger.info("Request time: {}ms", (t1 - t0) / 1000000.0);

                t0 = System.nanoTime();

                LookupRowsRequest lookup = new LookupRowsRequest("//home/dev/autorestart_nodes_copy", schema.toLookup())
                        .addFilter(1486190036109192L, "n0344-sas.cluster-name", "SAS2.4.3-15")
                        .addFilter(1486190037953802L, "s03-sas.cluster-name", "SAS2.4.3-15")
                        .addLookupColumns("timestamp", "data");


                t1 = System.nanoTime();
                UnversionedRowset rowset = client.lookupRows(lookup).join();
                long t2 = System.nanoTime();
                logger.info("Request time: {}ms + {}ms", (t1 - t0) / 1000000.0, (t2 - t1) / 1000000.0);
                logger.info("Result schema:");
                for (ColumnSchema column : rowset.getSchema().getColumns()) {
                    logger.info("    {}", column.getName());
                }
                for (UnversionedRow row : rowset.getRows()) {
                    logger.info("Row:");
                    for (UnversionedValue value : row.getValues()) {
                        logger.info("    value: {}", value);
                    }
                }
                for (YTreeMapNode row : rowset.getYTreeRows()) {
                    logger.info("Row: {}", row);
                }

                transaction.commit().join();
                logger.info("Transaction committed!");
            }
        });
    }
}
```

## Python { #python }

Для работы через Python необходимо установить пакет с биндингами, pypi-пакет называется ytsaurus-rpc-driver. После этого необходимо указать в качестве бэкенда rpc.

{% if audience == "internal" %}
В Аркадии нужно добавить PEERDIR на yt/python/client_with_rpc (вместо yt/python/client).
{% endif %}

Работа с динамическими таблицами:

```python
import yt.wrapper as yt

def main():
    client = yt.YtClient("cluster-name", config={"backend": "rpc"})
    schema = [
        {"name": "x", "type": "int64", "sort_order": "ascending"},
        {"name": "y", "type": "int64"},
        {"name": "z", "type": "int64"}
    ]

    table = "//home/ignat/dynamic_table"
    client.create("table", table, attributes={"schema": schema, "dynamic": True})
    client.mount_table(table, sync=True)
    client.insert_rows(table, [{"x": 0, "y": 99}])

    for iter in xrange(5):
        with client.Transaction(type="tablet"):
            rows = list(client.lookup_rows(table, [{"x": 0}]))
            if len(rows) == 1 and rows[0]["y"] <= 100:
                rows[0]["y"] += 1
                client.insert_rows(table, rows)

    print list(client.select_rows("* from [{}]".format(table)))

if __name__ == "__main__":
    main() 
```

Для того, чтобы клиент ходил к проксям определённой роли (например, `my_role`), нужно указать в конфиге опцию `proxy_role`:

```python
client = yt.YtClient("cluster-name", config={"backend": "rpc", "driver_config": {"proxy_role": "my_role"}})
```

Мы также советуем включать дебажные логи клиентской библиотеки. Такие логи сильно помогут при возникновении проблем. Помимо обычного способа через переменную окружения `YT_LOG_LEVEL=debug` можно явно настроить логирование внутри rpc proxy client. Для этого нужно передать конфиг логирования через параметр `driver_logging_config`:

```python
logging_config = {
    'rules': [{'min_level': 'debug', 'writers': ['file']}],
    'writers': {'file': {'file_name': logging_file_name, 'type': 'file'}}
}
client = yt.YtClient("cluster-name", config={"backend": "rpc", "driver_logging_config": logging_config})
```
