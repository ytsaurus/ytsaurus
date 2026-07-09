#include "state_manager.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

namespace {

IYsonSerializable& AsSerializable(const IStateHolderPtr& state)
{
    auto* serializable = dynamic_cast<IYsonSerializable*>(state.Get());
    YT_VERIFY(serializable);
    return *serializable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStateManager::TStateManager(TJobManagerStatePtr remoteState)
    : RemoteState_(std::move(remoteState))
{ }

IInitContextPtr TStateManager::CreateContext(const TComputationId& computationId, std::string prefix)
{
    return New<TInitContext>(MakeStrong(this), computationId, prefix);
}

void TStateManager::Sync()
{
    for (auto& [computationId, states] : States_) {
        std::vector<std::string> badNames;
        for (auto& [name, state] : states) {
            if (auto strongState = state.Lock()) {
                if (auto serialized = AsSerializable(strongState).Serialize()) {
                    RemoteState_->Computations[computationId][name] = *serialized;
                } else {
                    RemoteState_->Computations[computationId].erase(name);
                }
            } else {
                badNames.push_back(name);
            }
        }
        for (const auto& badName : badNames) {
            states.erase(badName);
        }
    }

    // Reclaim persisted state that no live holder backs any more - a removed computation or source,
    // or a source whose identity changed. Without this every such change would strand its blob in
    // the persisted state forever. Relies on all controller state being registered eagerly, in Init,
    // before the first Sync of an incarnation.
    for (auto& [computationId, remoteNames] : RemoteState_->Computations) {
        auto liveIt = States_.find(computationId);
        EraseNodesIf(remoteNames, [&] (const auto& item) {
            return liveIt == States_.end() || !liveIt->second.contains(item.first);
        });
    }
    EraseNodesIf(RemoteState_->Computations, [] (const auto& item) {
        return item.second.empty();
    });
}

IStateHolderPtr TStateManager::CreateState(const TComputationId& computationId, const std::string& name, std::function<IStateHolderPtr()> ctor)
{
    YT_VERIFY(!States_[computationId].contains(name) || States_[computationId][name].IsExpired());
    auto state = ctor();
    if (auto iter = RemoteState_->Computations[computationId].find(name); iter != RemoteState_->Computations[computationId].end()) {
        AsSerializable(state).Deserialize(iter->second);
    }
    States_[computationId][name] = state;
    return state;
}

////////////////////////////////////////////////////////////////////////////////

TMutableStateProvider::TMutableStateProvider(IStateHolderPtr state)
    : State_(std::move(state))
{ }

IStateHolderPtr TMutableStateProvider::GetState()
{
    return State_;
}

////////////////////////////////////////////////////////////////////////////////

TInitContext::TInitContext(
    TStateManagerPtr stateManager,
    TComputationId computationId,
    std::string prefix)
    : StateManager_(std::move(stateManager))
    , ComputationId_(std::move(computationId))
    , Prefix_(std::move(prefix))
{ }

IInitContextPtr TInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TInitContext>(StateManager_, ComputationId_, ExtendStateNamePrefix(Prefix_, prefix));
}

const std::string& TInitContext::GetPrefix() const
{
    return Prefix_;
}

TFuture<IMutableStateProviderPtr> TInitContext::CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const
{
    return MakeFuture<IMutableStateProviderPtr>(New<TMutableStateProvider>(StateManager_->CreateState(ComputationId_, Prefix_, ctor)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
