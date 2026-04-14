#include "transaction_options.h"

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

namespace NYT::NSequoiaClient::NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TSequoiaTransactionFeatures* protoFeatures, const NSequoiaClient::TSequoiaTransactionFeatures& features)
{
    if (features.UseSharedWriteLocksForCypressTransactions) {
        protoFeatures->set_use_shared_write_locks_for_cypress_transactions(*features.UseSharedWriteLocksForCypressTransactions);
    }
}

void FromProto(NSequoiaClient::TSequoiaTransactionFeatures* features, const TSequoiaTransactionFeatures& protoFeatures)
{
    if (protoFeatures.has_use_shared_write_locks_for_cypress_transactions()) {
        features->UseSharedWriteLocksForCypressTransactions = protoFeatures.use_shared_write_locks_for_cypress_transactions();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient::NProto
