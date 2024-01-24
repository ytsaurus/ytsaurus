package tech.ytsaurus.client.request;

import java.util.List;

import tech.ytsaurus.rpcproxy.TReqRegisterQueueConsumer;

public class RegistrationPartitions {
    private final List<Integer> items;

    public RegistrationPartitions(List<Integer> items) {
        this.items = items;
    }

    public List<Integer> getPartitions() {
        return items;
    }

    public TReqRegisterQueueConsumer.TRegistrationPartitions toProto() {
        return TReqRegisterQueueConsumer.TRegistrationPartitions.newBuilder()
                .addAllItems(items)
                .build();
    }
}
