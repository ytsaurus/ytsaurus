import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.operations.MapSpec;
import tech.ytsaurus.client.operations.Mapper;
import tech.ytsaurus.client.operations.MapperSpec;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.Statistics;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;


public class Example06MapYTree {
    private Example06MapYTree() {
    }

    // Класс маппера должен реализовывать соответствующий интерфейс.
    // В качестве аргументов дженерика указывается класс для представления входного и выходного объектов.
    // В данном случае это универсальный YTreeMapNode, который позволяет работать с произвольной таблицей.
    public static class SimpleMapper implements Mapper<YTreeMapNode, YTreeMapNode> {
        @Override
        public void map(YTreeMapNode entry, Yield<YTreeMapNode> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getString("name");
            String email = entry.getString("login").concat("@yandex-team.ru");

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(name)
                    .key("email").value(email)
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    public static void main(String[] args) {
        YtClient client = YtClient.builder()
                .setCluster("freud")
                .build();

        // Выходная таблица лежит в `//tmp` и содержит имя текущего пользователя
        // Имя пользователя нужно на тот случай, если два человека одновременно запустят этот пример,
        // мы не хотим, чтобы они столкнулись на одной выходной таблице.
        YPath outputTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-emails");

        try (client) {
           Operation op = client.map(
                    MapOperation.builder()
                            .setSpec(MapSpec.builder()
                                    .setInputTables(YPath.simple("//home/tutorial/staff_unsorted").withRange(0, 2))
                                    .setOutputTables(outputTable)
                                    .setMapperSpec(new MapperSpec(new SimpleMapper()))
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
