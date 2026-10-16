#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

//! A single output message ready to be distributed to a downstream computation.
struct TDistributorOutputMessage
{
    TComputationId ComputationId;
    TOutputMessageConstPtr Message;
    TOnDistributedCallback OnDistributed;
};

////////////////////////////////////////////////////////////////////////////////

struct IMessageDistributor
    : public TRefCounted
{
    //! Distributes all messages in one shot, taking internal locks once.
    virtual void DistributeOutputMessages(
        TJobId fromJobId,
        std::deque<TDistributorOutputMessage>&& messages) = 0;

    virtual void Reconfigure(TPipelineSpecPtr pipelineSpec, TDynamicMessageDistributorSpecPtr dynamicSpec) = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;

    virtual TMessageDistributorStatusPtr GetStatus() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageDistributor);

////////////////////////////////////////////////////////////////////////////////

IMessageDistributorPtr CreateMessageDistributor(
    IJobDirectoryPtr jobDirectory,
    NRpc::IChannelFactoryPtr channelFactory,
    TStreamSpecStoragePtr streamSpecStorage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
