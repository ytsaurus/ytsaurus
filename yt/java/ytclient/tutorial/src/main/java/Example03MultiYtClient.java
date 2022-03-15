import ru.yandex.type_info.TiType;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.proxy.MultiYtClient;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnSortOrder;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public class Example03MultiYtClient {
    public static void main(String[] args) {
        // Схема динамической таблицы, которую будем читать
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

        // Путь, по которому лежит динамическая таблица. Эта таблица есть на двух кластерах - на hume и freud.
        String path = "//home/dev/tutorial/dynamic-table";

        // Создаем клиент, который в первую очередь будет делать запросы в hume,
        // а если там будут ошибки/долгие ответы, то пойдет в freud.
        MultiYtClient client = MultiYtClient.builder()
                .addCluster("freud")
                .addPreferredCluster("hume")
                .build();

        try (client) {
            // Читаем строчку с key=2.
            UnversionedRowset rows = client.lookupRows(
                    new LookupRowsRequest(path, schema.toLookup()).addFilter(2)
            ).join();

            System.out.println("====== LOOKUP RESULT ======");
            for (UnversionedRow row : rows.getRows()) {
                System.out.println(row.toYTreeMap(schema, true));
            }
            System.out.println("====== END LOOKUP RESULT ======");
        }
    }
}
