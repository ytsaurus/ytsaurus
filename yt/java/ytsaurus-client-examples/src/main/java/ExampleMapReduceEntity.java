import java.util.Iterator;

import javax.persistence.Column;
import javax.persistence.Entity;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.operations.MapReduceSpec;
import tech.ytsaurus.client.operations.Mapper;
import tech.ytsaurus.client.operations.MapperSpec;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.ReducerSpec;
import tech.ytsaurus.client.operations.ReducerWithKey;
import tech.ytsaurus.client.operations.Statistics;
import tech.ytsaurus.client.request.MapReduceOperation;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;

public class ExampleMapReduceEntity {

    private ExampleMapReduceEntity() {
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
    static class StaffNameLength {
        private String name;
        @Column(name = "name_length")
        private int nameLength;

        StaffNameLength() {
        }

        StaffNameLength(String name, int nameLength) {
            this.name = name;
            this.nameLength = nameLength;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getNameLength() {
            return nameLength;
        }

        public void setNameLength(int nameLength) {
            this.nameLength = nameLength;
        }
    }

    @Entity
    static class OutputStaffInfo {
        private String name;
        @Column(name = "sum_name_length")
        private int sumNameLength;

        OutputStaffInfo() {
        }

        OutputStaffInfo(String name, int sumNameLength) {
            this.name = name;
            this.sumNameLength = sumNameLength;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSumNameLength() {
            return sumNameLength;
        }

        public void setSumNameLength(int sumNameLength) {
            this.sumNameLength = sumNameLength;
        }
    }

    @NonNullApi
    public static class SimpleMapper implements Mapper<InputStaffInfo, StaffNameLength> {
        @Override
        public void map(InputStaffInfo entry, Yield<StaffNameLength> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getName();
            yield.yield(new StaffNameLength(name, name.length()));
        }
    }

    @NonNullApi
    public static class SimpleReducer implements ReducerWithKey<StaffNameLength, OutputStaffInfo, String> {
        @Override
        public String key(StaffNameLength entry) {
            return entry.getName();
        }

        @Override
        public void reduce(String key, Iterator<StaffNameLength> input, Yield<OutputStaffInfo> yield,
                           Statistics statistics) {
            // All rows with a common reduce key come to reduce, i.e. in this case with a common field `name`.

            int sumNameLength = 0;
            while (input.hasNext()) {
                StaffNameLength staffNameLength = input.next();
                sumNameLength += staffNameLength.getNameLength();
            }

            yield.yield(new OutputStaffInfo(key, sumNameLength));
        }
    }

    public static void main(String[] args) {
        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster("freud")
                .build();

        YPath inputTable = YPath.simple("//home/tutorial/staff_unsorted");
        YPath outputTable = YPath.simple("//tmp/" + System.getProperty("user.name") + "-tutorial-emails");

        try (client) {
            Operation op = client.mapReduce(MapReduceOperation.builder()
                    .setSpec(MapReduceSpec.builder()
                            .setInputTables(inputTable)
                            .setOutputTables(outputTable)
                            .setReduceBy("name")
                            .setSortBy("name")
                            .setReducerSpec(new ReducerSpec(new SimpleReducer()))
                            .setMapperSpec(new MapperSpec(new SimpleMapper()))
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
