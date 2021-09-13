import java.util.ArrayList;
import java.util.List;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.TableReader;
import ru.yandex.yt.ytclient.proxy.TableWriter;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;

public class Example02ReadWriteTable {
    @YTreeObject
    static class TableRow {
        public String english;
        public String russian;

        public TableRow(String english, String russian) {
            this.english = english;
            this.russian = russian;
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s, %s\")", english, russian);
        }
    }

    public static void main(String[] args) {
        YtClient client = YtClient.builder()
                .setCluster("freud")
                .build();

        try (client) {
            // Таблица лежит в `//tmp` и содержит имя текущего пользователя
            // Имя пользователя нужно на тот случай, если два человека одновременно запустят этот пример,
            // мы не хотим, чтобы они столкнулись на одной выходной таблице.
            YPath table = YPath.simple("//tmp/" + System.getProperty("user.name") + "-read-write");

            // Записываем таблицу.
            {
                // Создаем writer.
                TableWriter<TableRow> writer = client.writeTable(
                        new WriteTable<>(table, YTreeObjectSerializerFactory.forClass(TableRow.class))).join();

                try {
                    while (true) {
                        // Необходимо дождаться readyEvent перед тем, как пробовать делать запись.
                        writer.readyEvent().join();

                        // Если вернулось false, то необходимо дождаться readyEvent перед тем, как пробовать еще раз.
                        boolean accepted = writer.write(List.of(
                                new TableRow("one", "один"),
                                new TableRow("two", "два"))
                        );

                        if (accepted) {
                            break;
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    // Дожидается завершения записи. Может выбросить исключение, если что-то пошло не так.
                    writer.close().join();
                }
            }

            // Читаем всю таблицу.
            {
                // Создаем reader.
                TableReader<TableRow> reader = client.readTable(
                        new ReadTable<>(table, YTreeObjectSerializerFactory.forClass(TableRow.class))).join();

                List<TableRow> rows = new ArrayList<>();

                try {
                    // Будем читать, пока можем.
                    while (reader.canRead()) {
                        // Ждем, пока можно будет продолжить чтение.
                        reader.readyEvent().join();

                        List<TableRow> currentRows;
                        while ((currentRows = reader.read()) != null) {
                            rows.addAll(currentRows);
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to read");
                } finally {
                    reader.close().join();
                }

                for (TableRow row : rows) {
                    System.out.println("russian: " + row.russian + "; english: " + row.english);
                }
            }
        }
    }
}
