import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YtDecimal;
import ru.yandex.type_info.TiType;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class Example05Decimal {
    @YTreeObject
    static class TableRow {
        public String field;
        // У таблицы обязательно должна быть схема, в который указаны precision и scale.
        public BigDecimal value;

        public TableRow(String field, BigDecimal value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s\", %s)", field, value.toString());
        }
    }

    @YTreeObject
    static class TableRowAnnotated {
        public String field;

        // Можно использовать без схемы. Тогда для сериализации/десериализации будут использованы указанные в аннотации precision и scale.
        @YtDecimal(precision = 7, scale = 3)
        public BigDecimal value;

        public TableRowAnnotated(String field, BigDecimal value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRowAnnotated(\"%s\", %s)", field, value.toString());
        }
    }

    private static <T> void writeRead(YtClient client, YPath path, List<T> data, Class<T> rowClass) throws Exception {
        // Создаем writer.
        var writer = client.writeTable(
                new WriteTable<>(path, rowClass).setNeedRetries(true)
        ).join();

        // Пишем данные в таблицу.
        try {
            writer.readyEvent().join();
            writer.write(data);
        } finally {
            writer.close().join();
        }

        // Создаем reader.
        var reader = client.readTable(
                new ReadTable<>(path, rowClass)
        ).join();

        // Читаем всю таблицу.
        List<T> result = new ArrayList<>();
        try {
            while (reader.canRead()) {
                reader.readyEvent().join();
                List<T> cur;
                while ((cur = reader.read()) != null) {
                    result.addAll(cur);
                }
                reader.readyEvent().join();
            }
        } finally {
            reader.readyEvent().join();
            reader.close().join();
        }

        System.out.println("====== READ ROWS ======");
        for (T row : result) {
            System.out.println(row);
        }
        System.out.println("====== END READ ROWS ======");
    }

    public static void main(String[] args) throws Exception {
        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        YtClient client = YtClient.builder()
                .setCluster("hume")
                .build();

        // У колонок типа decimal есть два параметра - precision и scale.
        // Бинарное представление decimal поля зависит от этих параметров.
        // Есть два варианта:
        //   - писать/читать в уже существующую таблицу со схемой, в которой хранятся precision и scale
        //   - разметить decimal поля с помощью @YtDecimal, в которой указаны precision и scale.

        try (client) {
            {
                YPath path = YPath.simple("//tmp/" + System.getProperty("user.name") + "-decimal");
                client.createNode(new CreateNode(path.toString(), ObjectType.Table, Map.of(
                        "schema", schema.toYTree()
                )).setIgnoreExisting(true)).join();

                List<TableRow> data = List.of(
                        new TableRow("first", BigDecimal.valueOf(123.45)),
                        new TableRow("second", BigDecimal.valueOf(4.123))
                );

                // Пишем в уже созданную таблицу (с заданной схемой).
                writeRead(client, path, data, TableRow.class);
            }

            {
                YPath path = YPath.simple("//tmp/" + System.getProperty("user.name") + "-decimal" + UUID.randomUUID());

                List<TableRowAnnotated> data = List.of(
                        new TableRowAnnotated("first", BigDecimal.valueOf(123.45)),
                        new TableRowAnnotated("second", BigDecimal.valueOf(4.123))
                );

                // Пишем в несуществующую таблицу без схемы, precision и scale берем из аннотации @YtDecimal.
                writeRead(client, path, data, TableRowAnnotated.class);
            }
        }
    }
}
