import javax.persistence.Entity;

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

public class ExampleMapEntity {
    private ExampleMapEntity() {
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
        private String email;
        private String name;

        OutputStaffInfo() {
        }

        OutputStaffInfo(String email, String name) {
            this.email = email;
            this.name = name;
        }
    }

    // The mapper class must implement the appropriate interface.
    // Generic type arguments are the classes to represent the input and output objects.
    @NonNullApi
    public static class SimpleMapper implements Mapper<InputStaffInfo, OutputStaffInfo> {
        @Override
        public void map(InputStaffInfo entry, Yield<OutputStaffInfo> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getName();
            String email = entry.getLogin().concat("@yandex-team.ru");

            yield.yield(new OutputStaffInfo(name, email));
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
