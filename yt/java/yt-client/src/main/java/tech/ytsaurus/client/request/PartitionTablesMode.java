package tech.ytsaurus.client.request;

public enum PartitionTablesMode {
    Sorted("sorted", 0),
    Ordered("ordered", 1),
    Unordered("unordered", 2);

    private final String name;
    private final int protoValue;

    PartitionTablesMode(String name, int protoValue) {
        this.name = name;
        this.protoValue = protoValue;
    }

    public String getName() {
        return name;
    }

    public int getProtoValue() {
        return protoValue;
    }
}
