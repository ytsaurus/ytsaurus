#pragma once

#include "public.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

TMasterInstanceTag InstanceTagFromTransactionId(TTransactionId transactionId);

TClusterTag ClusterTagFromId(TTransactionId id);

TTransactionId GenerateTransactionId(
    TMasterInstanceTag instanceTag,
    TClusterTag clusterTag,
    TTimestamp timestamp);

bool IsValidInstanceTag(TMasterInstanceTag tag);

void ForEachValidInstanceTag(std::function<void(TMasterInstanceTag)> callback);

TString ValidInstanceTagRange();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
