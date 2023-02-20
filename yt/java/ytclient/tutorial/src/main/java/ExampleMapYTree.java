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
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;



public class ExampleMapYTree {
    private ExampleMapYTree() {
    }

    // The mapper class must implement the appropriate interface.
    // Generic type arguments are the classes to represent the input and output objects.
    // In this case, it's a universal YTreeMapNode that allows you to work with an arbitrary table.
    @NonNullApi
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

        // The output table is located in `//tmp` and contains the name of the current user.
        // The username is necessary in case two people run this example at the same time
        // so that they use different output tables.
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
