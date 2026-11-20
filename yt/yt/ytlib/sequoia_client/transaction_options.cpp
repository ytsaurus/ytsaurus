#include "transaction_options.h"

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TSequoiaTransactionFeatures& features, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{UseSharedWriteLockForCypressTransactions: %v, CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator: %v, "
        "UseCoarseGrainedOrderingTags: %v, UseFineGrainedOrderingTags: %v}",
        features.UseSharedWriteLocksForCypressTransactions,
        features.CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator,
        features.UseCoarseGrainedOrderingTags,
        features.UseFineGrainedOrderingTags);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

namespace NYT::NSequoiaClient::NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TSequoiaTransactionFeatures* protoFeatures, const NSequoiaClient::TSequoiaTransactionFeatures& features)
{
    if (features.UseSharedWriteLocksForCypressTransactions) {
        protoFeatures->set_use_shared_write_locks_for_cypress_transactions(*features.UseSharedWriteLocksForCypressTransactions);
    }
    if (features.CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator) {
        protoFeatures->set_coordinate_cypress_transaction_replication_on_cypress_transaction_coordinator(
            *features.CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator);
    }
    if (features.UseCoarseGrainedOrderingTags) {
        protoFeatures->set_use_coarse_grained_ordering_tags(*features.UseCoarseGrainedOrderingTags);
    }
    if (features.UseFineGrainedOrderingTags) {
        protoFeatures->set_use_fine_grained_ordering_tags(*features.UseFineGrainedOrderingTags);
    }
}

void FromProto(NSequoiaClient::TSequoiaTransactionFeatures* features, const TSequoiaTransactionFeatures& protoFeatures)
{
    if (protoFeatures.has_use_shared_write_locks_for_cypress_transactions()) {
        features->UseSharedWriteLocksForCypressTransactions = protoFeatures.use_shared_write_locks_for_cypress_transactions();
    }
    if (protoFeatures.has_coordinate_cypress_transaction_replication_on_cypress_transaction_coordinator()) {
        features->CoordinateCypressTransactionReplicationOnCypressTransactionCoordinator =
            protoFeatures.coordinate_cypress_transaction_replication_on_cypress_transaction_coordinator();
    }
    if (protoFeatures.has_use_coarse_grained_ordering_tags()) {
        features->UseCoarseGrainedOrderingTags = protoFeatures.use_coarse_grained_ordering_tags();
    }
    if (protoFeatures.has_use_fine_grained_ordering_tags()) {
        features->UseFineGrainedOrderingTags = protoFeatures.use_fine_grained_ordering_tags();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient::NProto
