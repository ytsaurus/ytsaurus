#include "transform_companion_computation.h"
#include "companion_model.h"

#include <yt/yt/flow/library/cpp/common/external_metrics_reporter.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("internal_states", &TThis::InternalStates)
        .Default();
}

void TCompanionDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompanionState::Register(TRegistrar registrar)
{
    registrar.Parameter("payload", &TThis::Payload)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTransformCompanionComputation::TTransformCompanionComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TCompanionComputationBaseAdapter<TTransformComputation>(std::move(context), std::move(dynamicContext))
{ }

void TTransformCompanionComputation::DoInit(IJobInitContextPtr initContext)
{
    YT_LOG_DEBUG("DoInit started");
    FetchAndValidateCompanionInfo();

    PutJobInfoToCompanionWithReconfigure();

    // Init internal states clients.
    if (GetParameters()->InternalStates) {
        for (const auto& stateName : GetParameters()->InternalStates.value()) {
            YT_LOG_DEBUG("Initializing KeyStateClient (StateName: %v)", stateName);
            initContext->InitClient<TCompanionState>(InternalStateClients_[stateName], stateName);
        }
    }
    // Init external state clients — names come from the spec's
    // ``external_state_managers`` block.
    for (const auto& stateName : GetKeys(GetSpec()->ExternalStateManagers)) {
        YT_LOG_DEBUG("Initializing ExternalStateClient (StateName: %v)", stateName);
        initContext->InitExternalStateClient(ExternalStateClients_[stateName], stateName);
    }
    // Init read-only external state joiners — names come from the spec's
    // ``external_state_joiners`` block.
    for (const auto& stateName : GetKeys(GetSpec()->ExternalStateJoiners)) {
        YT_LOG_DEBUG("Initializing ExternalStateJoiner (StateName: %v)", stateName);
        initContext->InitExternalStateClient(ExternalStateJoiners_[stateName], stateName);
    }
    YT_LOG_DEBUG("DoInit finished");
}

void TTransformCompanionComputation::DoProcess(
    IInputContextPtr input,
    IOutputCollectorPtr output)
{
    YT_LOG_DEBUG("Starting DoProcess (MessagesSize: %v, TimersSize: %v, VisitsSize: %v)",
        input->GetMessages().size(),
        input->GetTimers().size(),
        input->GetVisits().size());
    if (input->GetMessages().empty() && input->GetTimers().empty() && input->GetVisits().empty()) {
        YT_LOG_DEBUG("Empty inputs. Returning.");
        return;
    }
    auto request = CreateCompanionRequest<TCompanionProcessRequest>();
    request->Messages.reserve(input->GetMessages().size());

    // Temporary store of messages and timers for parent messages and timers tracking.
    // Companion sends back only IDs of parent messages and timers.
    THashMap<TMessageId, TInputMessageConstPtr> messageMap;
    THashMap<TMessageId, TInputTimerConstPtr> timerMap;
    THashMap<TMessageId, TInputVisitConstPtr> visitMap;

    // Map of Map to internal state accessors for the current epoch. Updated by companion response.
    THashMap<std::string, THashMap<TKey, TStateAccessor<TCompanionState>>> internalStateMap;
    // Map of Map to pointer to simple external states. Updated by companion response.
    // The states themselves are owned by the external state manager for the epoch.
    THashMap<std::string, THashMap<TKey, TSimpleExternalState*>> externalStateMap;
    // Keys already added per joiner. Joined state is read-only, so no accessors are kept.
    THashMap<std::string, THashSet<TKey>> joinedStateKeys;

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
                        .State = state->Payload.value(),
                    });
            }
        }
    };

    auto addExternalStatesForKey = [&] (const TKey& key) {
        for (const auto& [stateName, stateClient] : ExternalStateClients_) {
            if (externalStateMap[stateName].contains(key)) {
                continue;
            }
            auto stateHandle = stateClient.GetState(key);
            auto* extState = stateHandle.Get();
            externalStateMap[stateName][key] = extState;
            if (extState) {
                GetOrInsert(
                    request->ExternalStates,
                    stateName,
                    [&] {
                        return TStateHolder<TPayload>{
                            .StateName = stateName,
                            .Schema = extState->Schema,
                        };
                    })
                    .StateItems.push_back({
                        .Key = key,
                        .State = extState->Payload,
                    });
            }
        }
    };

    auto addJoinedStatesForKey = [&] (const TKey& key) {
        for (const auto& [stateName, stateClient] : ExternalStateJoiners_) {
            if (!joinedStateKeys[stateName].insert(key).second) {
                continue;
            }
            auto stateHandle = stateClient.GetState(key);
            const auto* joinedState = stateHandle.Get();
            if (joinedState) {
                GetOrInsert(
                    request->JoinedExternalStates,
                    stateName,
                    [&] {
                        return TStateHolder<TPayload>{
                            .StateName = stateName,
                            .Schema = joinedState->Schema,
                        };
                    })
                    .StateItems.push_back({
                        .Key = key,
                        .State = joinedState->Payload,
                    });
            }
        }
    };

    // Process messages.
    for (const auto& message : input->GetMessages()) {
        request->Messages.push_back(message);
        messageMap[message->MessageId] = message;
        // Internal states.
        addInternalStatesForKey(message->Key);
        // External states.
        addExternalStatesForKey(message->Key);
        // Read-only joined external states.
        addJoinedStatesForKey(message->Key);
    }

    // Process timers.
    request->Timers.reserve(input->GetTimers().size());
    for (const auto& timer : input->GetTimers()) {
        timerMap[timer->MessageId] = timer;
        request->Timers.push_back(timer);
        // Internal states.
        addInternalStatesForKey(timer->Key);
        // External states.
        addExternalStatesForKey(timer->Key);
        // Read-only joined external states.
        addJoinedStatesForKey(timer->Key);
    }

    // Process visits.
    request->Visits.reserve(input->GetVisits().size());
    for (const auto& visit : input->GetVisits()) {
        visitMap[visit->MessageId] = visit;
        request->Visits.push_back(visit);
        addInternalStatesForKey(visit->Key);
        addExternalStatesForKey(visit->Key);
        addJoinedStatesForKey(visit->Key);
    }

    for (const auto& streamId : Concatenate(GetSpec()->InputStreamIds, GetKeys(GetSpec()->TimerStreams))) {
        auto streamWatermark = GetEpochEventWatermark(streamId);
        request->Watermarks.push_back(TStreamWatermark{
            .StreamId = streamId,
            .Watermark = streamWatermark});
    }

    // Request to Companion.
    auto response = DoProcessWithCompanion(request);

    // Process result groups.
    for (auto& group : response->Groups) {
        std::vector<TInputMessageConstPtr> groupParents;
        std::vector<TInputTimerConstPtr> groupTimerParents;
        std::vector<TInputVisitConstPtr> groupVisitParents;

        for (const auto& parentId : group.ParentIds) {
            if (auto it = messageMap.find(parentId); it != messageMap.end()) {
                groupParents.push_back(it->second);
            } else if (auto it = timerMap.find(parentId); it != timerMap.end()) {
                groupTimerParents.push_back(it->second);
            } else if (auto it = visitMap.find(parentId); it != visitMap.end()) {
                groupVisitParents.push_back(it->second);
            } else {
                THROW_ERROR_EXCEPTION("Parent message, timer or visit not found")
                    << TErrorAttribute("parent_id", parentId);
            }
        }

        auto groupOutput = output->SetParents(groupParents, groupTimerParents, groupVisitParents);
        for (auto& message : group.Messages) {
            groupOutput->AddMessage(std::move(message));
        }
        for (const auto& companionTimer : group.Timers) {
            if (companionTimer.StreamId) {
                groupOutput->AddTimer(*companionTimer.StreamId,
                    companionTimer.TriggerTimestamp,
                    companionTimer.EventTimestamp);
            } else {
                groupOutput->AddTimer(companionTimer.TriggerTimestamp, companionTimer.EventTimestamp);
            }
        }
    }
    // Set States.
    for (const auto& state : response->InternalStates) {
        auto stateMapIt = internalStateMap.find(state.StateName);
        if (stateMapIt == internalStateMap.end()) {
            THROW_ERROR_EXCEPTION("Internal state is not found for state name")
                << TErrorAttribute("state_name", state.StateName);
        }

        for (const auto& stateItem : state.StateItems) {
            auto stateIt = stateMapIt->second.find(stateItem.Key);
            if (stateIt == internalStateMap[state.StateName].end()) {
                THROW_ERROR_EXCEPTION("Internal state is not found for key")
                    << TErrorAttribute("state_name", state.StateName)
                    << TErrorAttribute("key", stateItem.Key);
            }
            if (stateItem.Reset) {
                stateIt->second.Clear();
            } else {
                stateIt->second->Payload = stateItem.State;
            }
        }
    }
    // Set External States.
    for (const auto& state : response->ExternalStates) {
        auto stateMapIt = externalStateMap.find(state.StateName);
        if (stateMapIt == externalStateMap.end()) {
            THROW_ERROR_EXCEPTION("External state is not found for state name")
                << TErrorAttribute("state_name", state.StateName);
        }

        for (const auto& stateItem : state.StateItems) {
            auto stateIt = externalStateMap[state.StateName].find(stateItem.Key);
            if (stateIt == externalStateMap[state.StateName].end()) {
                THROW_ERROR_EXCEPTION("External state is not found for key")
                    << TErrorAttribute("state_name", state.StateName)
                    << TErrorAttribute("key", stateItem.Key);
            }
            if (stateItem.Reset) {
                stateIt->second->Clear();
            } else {
                stateIt->second->Payload = stateItem.State;
            }
        }
    }

    YT_LOG_DEBUG("DoProcess finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
