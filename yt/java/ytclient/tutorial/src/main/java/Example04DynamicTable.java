import java.util.List;
import java.util.Map;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.ObjectType;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.type_info.TiType;
import tech.ytsaurus.ysontree.YTree;

import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnSortOrder;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class Example04DynamicTable {
    private Example04DynamicTable() {
    }

    public static void main(String[] args) {
        //
        // Программа принимает аргументами имя кластера и путь к таблице, с которой она будет работать (таблица не
        // должна существовать).
        //
        // По умолчанию у пользователей нет прав монтировать динамические таблицы, и перед запуском программы
        // необходимо получить такие права (права на монтирование таблиц) на каком-нибудь кластере YT.
        if (args.length != 2) {
            throw new IllegalArgumentException("Incorrect arguments count");
        }
        String cluster = args[0];
        String path = args[1];

        TableSchema schema = TableSchema.builderWithUniqueKeys()
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
            CreateNode createNode = CreateNode.builder()
                    .setPath(YPath.simple(path))
                    .setType(ObjectType.Table)
                    .setAttributes(Map.of(
                            "dynamic", YTree.booleanNode(true),
                            "schema", schema.toYTree()
                    ))
                    .build();

            client.createNode(createNode).join();

            // Для того чтобы начать работу с динамической таблицей, её необходимо "подмонтировать".
            //
            // Часто создание / монтирование / размонтирование таблиц делается отдельными административными скриптами,
            // а приложение просто расчитывает, что таблицы существуют и уже подмонтированы.
            //
            // Мы для полноты примера подмонтируем таблицу, а в конце её размонтируем.
            client.mountTable(path).join();

            // Заполняем таблицу.
            try (ApiServiceTransaction transaction =
                         client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Вставлять значения в динтаблицу можно с помощью modifyRows.
                transaction.modifyRows(
                        ModifyRowsRequest.builder()
                                .setPath(path)
                                .setSchema(schema)
                                .addInsert(List.of(1, "a"))
                                .addInsert(List.of(2, "b"))
                                .addInsert(List.of(3, "c"))
                                .build()).join();
                transaction.commit().join();
            }

            // Читаем таблицу.
            try (ApiServiceTransaction transaction =
                         client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Получать значения из динтаблицы можно с помощью lookupRows,
                // возвращает UnversionedRows из найденных строчек.
                UnversionedRowset rowset = transaction.lookupRows(
                        LookupRowsRequest.builder()
                                .setPath(path)
                                .setSchema(schema.toLookup())
                                .addFilter(1)
                                .build()).join();

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
