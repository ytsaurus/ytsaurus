#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

class TFailedMessages
    : public TRefCounted
{
public:
    TFailedMessages(
        TContextPtr context,
        TStreamSpecsPtr streamSpecs,
        std::string workerId,
        TFlowCoreTarget flowCoreTarget);

    void Write(
        const IRetryableTransactionPtr& transaction,
        const TComputationId& computationId,
        const TPartitionId& partitionId,
        TSystemTimestamp systemTimestamp,
        const std::vector<TFailedMessage>& messages);

private:
    const TContextPtr Context_;
    const TStreamSpecsPtr StreamSpecs_;
    const std::string WorkerId_;
    const TFlowCoreTarget FlowCoreTarget_;
    const NYPath::TYPath TablePath_;
};

DEFINE_REFCOUNTED_TYPE(TFailedMessages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
