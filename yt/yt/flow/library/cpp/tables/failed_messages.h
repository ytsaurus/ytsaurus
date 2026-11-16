#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/public.h>

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
        TFlowCoreTarget flowCoreTarget,
        NConcurrency::IThroughputThrottlerPtr writeThrottler,
        IStatusErrorStatePtr writeQuotaErrorState);

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
    const NConcurrency::IThroughputThrottlerPtr WriteThrottler_;
    const IStatusErrorStatePtr WriteQuotaErrorState_;
    const NYPath::TYPath TablePath_;
    const NProfiling::TCounter CapturedRows_;
};

DEFINE_REFCOUNTED_TYPE(TFailedMessages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
