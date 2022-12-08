package tech.ytsaurus.client;

class StashedMessage<DataType> {
    DataType data;
    long offset;

    StashedMessage(DataType data, long offset) {
        this.data = data;
        this.offset = offset;
    }
}
