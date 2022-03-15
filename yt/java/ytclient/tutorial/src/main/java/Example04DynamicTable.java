import java.util.List;
import java.util.Map;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.type_info.TiType;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.proxy.ModifyRowsRequest;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.TransactionType;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnSortOrder;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public class Example04DynamicTable {
    public static void main(String[] args) {
        //
        // Программа принимает аргументами имя кластера и путь к таблице, с которой она будет работать (таблица не должна существовать).
        //
        // По умолчанию у пользователей нет прав монтировать динамические таблицы, и перед запуском программы необходимо получить
        // такие права (права на монтирование таблиц) на каком-нибудь кластере YT.
        if (args.length != 2) {
            throw new IllegalArgumentException("Incorrect arguments count");
        }
        String cluster = args[0];
        String path = args[1];

        TableSchema schema = TableSchema.builder()
                .add(
                        ColumnSchema.builder("key", TiType.int64())
                                .setSortOrder(ColumnSortOrder.ASCENDING)
                                .build()
                )
                .add(
                        ColumnSchema.builder("value", TiType.string())
                                .build()
                )
                .build();

        YtClient client = YtClient.builder()
                .setCluster(cluster)
                .build();

        try (client) {
            // ВАЖНО: при создании динамической таблицы нам нужно
            //  - указать атрибут dynamic: true
            //  - указать схему
            // Это нужно сделать обязательно в вызове createNode. Т.е. не получится сначала создать таблицу,
            // а потом проставить эти атрибуты.
            CreateNode createNode = new CreateNode(path,
                    ObjectType.Table,
                    Map.of(
                            "dynamic", YTree.booleanNode(true),
                            "schema", schema.toYTree()
                    ));
            client.createNode(createNode).join();

            // Для того чтобы начать работу с динамической таблицей, её необходимо "подмонтировать".
            //
            // Часто создание / монтирование / размонтирование таблиц делается отдельными административными скриптами,
            // а приложение просто расчитывает, что таблицы существуют и уже подмонтированы.
            //
            // Мы для полноты примера подмонтируем таблицу, а в конце её размонтируем.
            client.mountTable(path).join();

            // Заполняем таблицу.
            try (ApiServiceTransaction transaction = client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Вставлять значения в динтаблицу можно с помощью modifyRows.
                transaction.modifyRows(
                        new ModifyRowsRequest(path, schema)
                                .addInsert(List.of(1, "a"))
                                .addInsert(List.of(2, "b"))
                                .addInsert(List.of(3, "c"))
                ).join();
                transaction.commit().join();
            }

            // Читаем таблицу.
            try (ApiServiceTransaction transaction = client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Получать значения из динтаблицы можно с помощью lookupRows,
                // возвращает UnversionedRows из найденных строчек.
                UnversionedRowset rowset = transaction.lookupRows(
                        new LookupRowsRequest(path, schema.toLookup()).addFilter(1)).join();

                System.out.println("====== LOOKUP RESULT ======");
                for (UnversionedRow row : rowset.getRows()) {
                    System.out.println(row.toYTreeMap(schema, true));
                }
                System.out.println("====== END LOOKUP RESULT ======");

                transaction.commit().join();
            }

            // Размонтируем таблицу.
            client.unmountTable(path).join();
        }
    }
}
