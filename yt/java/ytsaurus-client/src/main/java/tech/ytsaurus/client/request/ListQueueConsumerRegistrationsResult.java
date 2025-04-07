package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.RichYPath;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TRspListQueueConsumerRegistrations;

/**
 * Immutable list of queue consumer registrations.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#listQueueConsumerRegistrations(ListQueueConsumerRegistrations)
 */
public class ListQueueConsumerRegistrationsResult {
    private final List<QueueConsumerRegistration> queueConsumerRegistrations;

    public ListQueueConsumerRegistrationsResult(TRspListQueueConsumerRegistrations rsp) {
        this.queueConsumerRegistrations = rsp.getRegistrationsList().stream()
                .map(QueueConsumerRegistration::new)
                .collect(Collectors.toUnmodifiableList());
    }

    public List<QueueConsumerRegistration> getQueueConsumerRegistrations() {
        return queueConsumerRegistrations;
    }

    public static class QueueConsumerRegistration {
        private final YPath queuePath;
        private final YPath consumerPath;
        private final boolean vital;
        @Nullable
        private final RegistrationPartitions partitions;

        public QueueConsumerRegistration(TRspListQueueConsumerRegistrations.TQueueConsumerRegistration protoValue) {
            this.queuePath = RichYPath.fromString(protoValue.getQueuePath().toStringUtf8());
            this.consumerPath = RichYPath.fromString(protoValue.getConsumerPath().toStringUtf8());
            this.vital = protoValue.getVital();
            this.partitions = protoValue.hasPartitions()
                    ? new RegistrationPartitions(protoValue.getPartitions().getItemsList())
                    : null;
        }

        public YPath getQueuePath() {
            return queuePath;
        }

        public YPath getConsumerPath() {
            return consumerPath;
        }

        public boolean isVital() {
            return vital;
        }

        public Optional<RegistrationPartitions> getPartitions() {
            return Optional.ofNullable(partitions);
        }
    }
}
