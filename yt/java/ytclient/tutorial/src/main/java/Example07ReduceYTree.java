import java.util.Iterator;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.yt.ytclient.operations.Operation;
import ru.yandex.yt.ytclient.operations.ReduceSpec;
import ru.yandex.yt.ytclient.operations.ReducerSpec;
import ru.yandex.yt.ytclient.operations.ReducerWithKey;
import ru.yandex.yt.ytclient.operations.SortSpec;
import ru.yandex.yt.ytclient.operations.Statistics;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.request.ReduceOperation;
import ru.yandex.yt.ytclient.request.SortOperation;

public class Example07ReduceYTree {
    private Example07ReduceYTree() {
    }

    // Класс редьюсера должен реализовывать соответствующий интерфейс.
    // В качестве аргументов дженерика указывается класс для представления входного и выходного объектов.
    // В данном случае это универсальный YTreeMapNode, который позволяет работать с произвольной таблицей.
    public static class SimpleReducer implements ReducerWithKey<YTreeMapNode, YTreeMapNode, String> {
        @Override
        public String key(YTreeMapNode entry) {
            return entry.getString("name");
        }

        @Override
        public void reduce(String key, Iterator<YTreeMapNode> input, Yield<YTreeMapNode> yield, Statistics statistics) {
            // В reduce приходят все записи с общим reduce ключом, т.е. в нашем случае с общим полем `name'.

            int count = 0;
            while (input.hasNext()) {
                input.next();
                ++count;
            }

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(key)
                    .key("count").value(count)
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    public static void main(String[] args) {
        YtClient client = YtClient.builder()
                .setCluster("freud")
                .build();

        YPath outputTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-emails");
        YPath sortedTmpTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-tmp");

        try (client) {
            // Сортируем входную таблицу по `name`.
            client.sort(SortOperation.builder()
                    .setSpec(SortSpec.builder()
                            .setInputTables(YPath.simple("//home/tutorial/staff_unsorted"))
                            .setOutputTable(sortedTmpTable)
                            .setSortBy("name")
                            .build())
                    .build()).join();

            Operation op = client.reduce(ReduceOperation.builder()
                    .setSpec(ReduceSpec.builder()
                            .setInputTables(sortedTmpTable)
                            .setOutputTables(outputTable)
                            .setReduceBy("name")
                            .setReducerSpec(new ReducerSpec(new SimpleReducer()))
                            .build())
                    .build()
            ).join();

            System.err.println("Operation was finished (OperationId: " + op.getId() + ")");
            System.err.println("Status: " + op.getStatus().join());
        }

        System.err.println(
                "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" + outputTable
        );
    }
}
