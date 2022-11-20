import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;

public class Example02ReadWriteTable {
    private Example02ReadWriteTable() {
    }

    @YTreeObject
    static class TableRow {
        private String english;
        private String russian;

        TableRow(String english, String russian) {
            this.english = english;
            this.russian = russian;
        }

        public String getEnglish() {
            return english;
        }

        public void setEnglish(String english) {
            this.english = english;
        }

        public String getRussian() {
            return russian;
        }

        public void setRussian(String russian) {
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

            // Создаем writer.
            TableWriter<TableRow> writer = client.writeTable(
                    WriteTable.<TableRow>builder()
                            .setPath(table)
                            .setSerializationContext(
                                    new WriteTable.SerializationContext<>(
                                            MappedRowSerializer.forClass(
                                                    YTreeObjectSerializerFactory.forClass(TableRow.class)
                                            ))
                            )
                            .build()).join();

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


            // Читаем всю таблицу.

            // Создаем reader.
            TableReader<TableRow> reader = client.readTable(
                    ReadTable.<TableRow>builder()
                            .setPath(table)
                            .setSerializationContext(
                                    new ReadTable.SerializationContext<>(
                                            YTreeObjectSerializerFactory.forClass(TableRow.class))
                            )
                            .build()).join();

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
