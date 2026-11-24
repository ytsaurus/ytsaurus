#ifndef STATE_ACCESS_INL_H_
    #error "Direct inclusion of this file is not allowed, include state_access.h"
    // For the sake of sane code completion.
    #include "state_access.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TState, class TSnapshot>
TYdbStateBridge<TState, TSnapshot>::TYdbStateBridge(std::string name)
    : Name_(std::move(name))
{ }

template <class TState, class TSnapshot>
TYdbStateAccess<TSnapshot> TYdbStateBridge<TState, TSnapshot>::MakeAccess()
{
    TYdbStateAccess<TSnapshot> access;
    access.Init = BIND(&TYdbStateBridge::Init, MakeStrong(this));
    access.Read = BIND(&TYdbStateBridge::Read, MakeStrong(this));
    access.Write = BIND(&TYdbStateBridge::Write, MakeStrong(this));
    if constexpr (std::is_same_v<TSnapshot, TYdbSinkCheckpoint>) {
        access.IsPersisted = BIND([this_ = MakeStrong(this)] (const TMessageId& messageId) {
            return messageId <= this_->State_->MaxPersistedMessageId;
        });
    }
    return access;
}

template <class TState, class TSnapshot>
void TYdbStateBridge<TState, TSnapshot>::Init(const IInitContextPtr& initContext)
{
    initContext->InitClient<TState>(State_, Name_);
}

template <class TState, class TSnapshot>
TSnapshot TYdbStateBridge<TState, TSnapshot>::Read() const
{
    return ReadYdbStateSnapshot(*State_);
}

template <class TState, class TSnapshot>
void TYdbStateBridge<TState, TSnapshot>::Write(const TSnapshot& snapshot)
{
    WriteYdbStateSnapshot(snapshot, State_.Get());
}

template <class TState, class TSnapshot>
TYdbStateAccess<TSnapshot> CreateYdbStateAccess(std::string name)
{
    return New<TYdbStateBridge<TState, TSnapshot>>(std::move(name))->MakeAccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
