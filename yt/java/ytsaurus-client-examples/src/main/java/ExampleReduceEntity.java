import java.util.Iterator;

import javax.persistence.Entity;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.ReduceSpec;
import tech.ytsaurus.client.operations.ReducerSpec;
import tech.ytsaurus.client.operations.ReducerWithKey;
import tech.ytsaurus.client.operations.SortSpec;
import tech.ytsaurus.client.operations.Statistics;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;

public class ExampleReduceEntity {
    private ExampleReduceEntity() {
    }

    @Entity
    static class InputStaffInfo {
        private String login;
        private String name;
        private long uid;

        public String getLogin() {
            return login;
        }

        public String getName() {
            return name;
        }

        public long getUid() {
            return uid;
        }
    }

    @Entity
    static class OutputStaffInfo {
        private String name;
        private int count;

        OutputStaffInfo() {
        }

        OutputStaffInfo(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    // The reducer class must implement the appropriate interface.
    // Generic type arguments are the classes to represent the input and output objects.
    // The third generic type argument in the ReducerWithKey interface specifies the type of the key.
    @NonNullApi
    public static class SimpleReducer implements ReducerWithKey<InputStaffInfo, OutputStaffInfo, String> {
        @Override
        public String key(InputStaffInfo entry) {
            return entry.getName();
        }

        @Override
        public void reduce(String key, Iterator<InputStaffInfo> input, Yield<OutputStaffInfo> yield,
                           Statistics statistics) {
            // All rows with a common reduce key come to reduce, i.e. in this case with a common field `name`.

            int count = 0;
            while (input.hasNext()) {
                input.next();
                ++count;
            }

            yield.yield(new OutputStaffInfo(key, count));
        }
    }

    public static void main(String[] args) {
        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster("freud")
                .build();

        YPath outputTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-emails");
        YPath sortedTmpTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-tmp");

        try (client) {
            // Sort the input table by `name`.
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
                "Output table: " + outputTable
        );
    }
}
