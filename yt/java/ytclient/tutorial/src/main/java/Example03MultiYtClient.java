import tech.ytsaurus.client.MultiYtClient;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.type_info.TiType;


public class Example03MultiYtClient {
    private Example03MultiYtClient() {
    }

    public static void main(String[] args) {
        // Схема динамической таблицы, которую будем читать
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
                    LookupRowsRequest.builder()
                            .setPath(path)
                            .setSchema(schema.toLookup())
                            .addFilter(2)
                            .build()
            ).join();

            System.out.println("====== LOOKUP RESULT ======");
            for (UnversionedRow row : rows.getRows()) {
                System.out.println(row.toYTreeMap(schema, true));
            }
            System.out.println("====== END LOOKUP RESULT ======");
        }
    }
}
