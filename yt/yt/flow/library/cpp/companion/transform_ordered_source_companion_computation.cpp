#include "transform_ordered_source_companion_computation.h"

#include "companion_model.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TTransformOrderedSourceCompanionParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("internal_states", &TThis::InternalStates)
        .Default();
}

void TTransformOrderedSourceCompanionDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TTransformOrderedSourceCompanionComputation::TTransformOrderedSourceCompanionComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TCompanionComputationBaseAdapter<TTransformOrderedSourceComputation>(std::move(context), std::move(dynamicContext))
{ }

void TTransformOrderedSourceCompanionComputation::DoInit(IJobInitContextPtr initContext)
{
    YT_TLOG_DEBUG("DoInit started");
    InitCompanionJob();

    if (GetParameters()->InternalStates) {
        for (const auto& stateName : *GetParameters()->InternalStates) {
            YT_TLOG_DEBUG("Initializing KeyStateClient")
                .With("StateName", stateName);
            initContext->InitClient<TCompanionState>(InternalStateClients_[stateName], stateName);
        }
    }

    for (const auto& stateName : GetKeys(GetSpec()->ExternalStateJoiners)) {
        YT_TLOG_DEBUG("Initializing ExternalStateJoiner")
            .With("StateName", stateName);
        initContext->InitExternalStateClient(ExternalStateJoiners_[stateName], stateName);
    }
    YT_TLOG_DEBUG("DoInit finished");
}

void TTransformOrderedSourceCompanionComputation::DoProcess(
    IInputContextPtr input,
    IOutputCollectorPtr output)
{
    YT_VERIFY(input->GetTimers().empty());
    YT_VERIFY(input->GetVisits().empty());

    YT_TLOG_DEBUG("Starting DoProcess")
        .With("MessagesSize", input->GetMessages().size());
    if (input->GetMessages().empty()) {
        YT_TLOG_DEBUG("Empty inputs. Returning.");
        return;
    }

    auto request = CreateCompanionRequest<TCompanionProcessRequest>();
    request->Messages.reserve(input->GetMessages().size());

    THashMap<TMessageId, TInputMessageConstPtr> messageMap;
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> sourceStreamsSchemas;
    THashMap<std::string, THashMap<TKey, TStateAccessor<TCompanionState>>> internalStateMap;

    auto addInternalStatesForKey = [&] (const TKey& key) {
        for (const auto& [stateName, stateClient] : InternalStateClients_) {
            if (internalStateMap[stateName].contains(key)) {
                continue;
            }
            auto& state = (internalStateMap[stateName][key] = stateClient.GetState(key));
            if (state->Payload) {
                GetOrInsert(
                    request->InternalStates,
                    stateName,
                    [&] {
                        return TStateHolder<std::string>{.StateName = stateName};
                    })
                    .StateItems.push_back({
                        .Key = key,
                        .State = *state->Payload,
                    });
            }
        }
    };

    for (const auto& message : input->GetMessages()) {
        request->Messages.push_back(message);
        messageMap[message->MessageId] = message;
        sourceStreamsSchemas[message->StreamId] = message->PayloadSchema;
        addInternalStatesForKey(message->Key);
    }

    AddJoinedExternalStates(request, ExternalStateJoiners_, input);

    request->OverrideStreamSpecs = CreateLocalStreamSpecs(
        sourceStreamsSchemas,
        GetSpec()->OutputStreamIds,
        GetContext()->StreamSpecStorage->GetStreamSpecs());

    for (const auto& streamId : GetKeys(GetSpec()->SourceStreams)) {
        request->Watermarks.push_back(TStreamWatermark{
            .StreamId = streamId,
            .Watermark = GetEpochEventWatermark(streamId),
        });
    }

    auto response = DoProcessWithCompanion(request);

    for (auto& group : response->Groups) {
        THROW_ERROR_EXCEPTION_UNLESS(group.Timers.empty(), "Timers are not supported in source computation");
        THROW_ERROR_EXCEPTION_UNLESS(
            group.Distribute.empty() || group.Distribute.size() == group.Messages.size(),
            "Distribute flags count must match output messages count");

        std::vector<TInputMessageConstPtr> groupParents;
        groupParents.reserve(group.ParentIds.size());
        for (const auto& parentId : group.ParentIds) {
            auto it = messageMap.find(parentId);
            if (it == messageMap.end()) {
                THROW_ERROR_EXCEPTION("Parent message not found")
                    .With("parent_id", parentId);
            }
            groupParents.push_back(it->second);
        }

        auto groupOutput = output->SetParents(groupParents, {}, {});
        for (int index = 0; index < std::ssize(group.Messages); ++index) {
            const bool distribute = group.Distribute.empty() || group.Distribute[index];
            groupOutput->AddMessage(std::move(group.Messages[index]), distribute);
        }
    }

    for (const auto& state : response->InternalStates) {
        auto stateMapIt = internalStateMap.find(state.StateName);
        if (stateMapIt == internalStateMap.end()) {
            THROW_ERROR_EXCEPTION("Internal state is not found for state name")
                .With("state_name", state.StateName);
        }

        for (const auto& stateItem : state.StateItems) {
            auto stateIt = stateMapIt->second.find(stateItem.Key);
            if (stateIt == stateMapIt->second.end()) {
                THROW_ERROR_EXCEPTION("Internal state is not found for key")
                    .With("state_name", state.StateName)
                    .With("key", stateItem.Key);
            }
            if (stateItem.Reset) {
                stateIt->second.Clear();
            } else {
                stateIt->second->Payload = stateItem.State;
            }
        }
    }

    THROW_ERROR_EXCEPTION_UNLESS(
        response->ExternalStates.empty(),
        "TTransformOrderedSourceCompanionComputation does not support external state managers");

    YT_TLOG_DEBUG("DoProcess finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
