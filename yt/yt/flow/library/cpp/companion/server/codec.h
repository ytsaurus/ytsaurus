#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Decoded ProcessBatch request: the whole epoch input of one job.
struct TBatchInput
{
    std::vector<TInputMessageConstPtr> Messages;
    std::vector<TInputTimerConstPtr> Timers;
    std::vector<TInputVisitConstPtr> Visits;
    THashMap<std::string, NCompanion::TStateHolder<std::string>> InternalStates;
    THashMap<std::string, NCompanion::TStateHolder<TPayload>> ExternalStates;
    THashMap<std::string, NCompanion::TStateHolder<TPayload>> JoinedExternalStates;
    THashMap<TStreamId, TSystemTimestamp> Watermarks;
};

//! |messageStreamSpecs| must be the specs message payloads were serialized against:
//! the request-level stream override when present, the job streams otherwise.
//! |keySchema| is the job's group-by schema; keys do not carry a schema on the wire.
TBatchInput ParseProcessBatchRequest(
    const NProto::NCompanion::TReqProcessBatch& request,
    const TStreamSpecsPtr& messageStreamSpecs,
    const NTableClient::TTableSchemaPtr& keySchema);

////////////////////////////////////////////////////////////////////////////////

//! One output group: messages/timers attributed to the given parent entities.
struct TOutputGroup
{
    std::vector<TMessage> Messages;
    //! Aligned with |Messages|.
    std::vector<bool> Distribute;
    std::vector<NCompanion::TNewTimer> Timers;
    std::vector<TMessageId> ParentIds;
};

void SerializeProcessBatchResponse(
    NProto::NCompanion::TResponseData* data,
    const std::vector<TOutputGroup>& groups,
    const std::vector<NCompanion::TStateHolder<std::string>>& internalStates,
    const std::vector<NCompanion::TStateHolder<TPayload>>& externalStates,
    const TStreamSpecsPtr& messageStreamSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
