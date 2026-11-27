#pragma once

#include "public.h"

#include "ordered_source_computation_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TTransformOrderedSourceComputation
    : public TOrderedSourceComputationBase
{
private:
    static void ValidateSpec(const TComputationSpec& spec);

public:
    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

    using TOrderedSourceComputationBase::TOrderedSourceComputationBase;

public:
    static constexpr auto InputAcknowledgementPolicy = EInputAcknowledgementPolicy::PersistAfterEpoch;

protected:
    TMessageId GetMaxPersistedMessageIdExclusive();

    bool HasPersistedKeyedOutput() const override;

private:
    struct THeldRead
    {
        std::vector<TSourceMessageBatchCookie> BatchCookies;
        std::vector<TMessage> OutputMessages;
        std::vector<bool> OutputMessagesDistribute;
        TSystemTimestamp EventTimestamp;
    };

    std::optional<THeldRead> HeldRead_;

    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;
    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
