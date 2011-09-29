#include "composite_meta_state.h"

#include "../misc/foreach.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

void DeserializeChangeHeader(
    TRef changeData,
    NMetaState::NProto::TMsgChangeHeader* header)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (fixedHeader),
        fixedHeader->HeaderSize));
}

void DeserializeChange(
    TRef changeData,
    NMetaState::NProto::TMsgChangeHeader* header,
    TRef* messageData)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (TFixedChangeHeader),
        fixedHeader->HeaderSize));
    *messageData = TRef(
        changeData.Begin() + sizeof (TFixedChangeHeader) + fixedHeader->HeaderSize,
        fixedHeader->MessageSize);
}

////////////////////////////////////////////////////////////////////////////////

TMetaStatePart::TMetaStatePart(
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState)
    : MetaStateManager(metaStateManager)
    , MetaState(metaState)
    , Role(ERole::None)
{
    YASSERT(~metaStateManager != NULL);
    YASSERT(~metaState != NULL);
}

bool TMetaStatePart::IsLeader() const
{
    return Role == ERole::Leader;
}

bool TMetaStatePart::IsFolllower() const
{
    return Role == ERole::Follower;
}

IInvoker::TPtr TMetaStatePart::GetSnapshotInvoker() const
{
    return MetaState->SnapshotActionQueue->GetInvoker();
}

IInvoker::TPtr TMetaStatePart::GetStateInvoker() const
{
    return MetaState->StateActionQueue->GetInvoker();
}

IInvoker::TPtr TMetaStatePart::GetEpochStateInvoker() const
{
    YASSERT(~MetaState->EpochStateInvoker != NULL);
    return ~MetaState->EpochStateInvoker;
}

void TMetaStatePart::OnStartLeading()
{
    YASSERT(Role == ERole::None);

    Role = ERole::Leader;
}

void TMetaStatePart::OnStopLeading()
{
    YASSERT(Role == ERole::Leader);

    Role = ERole::None;
}

void TMetaStatePart::OnStartFollowing()
{
    YASSERT(Role == ERole::None);

    Role = ERole::Follower;
}

void TMetaStatePart::OnStopFollowing()
{
    YASSERT(Role == ERole::Follower);

    Role = ERole::None;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeMetaState::TCompositeMetaState()
    : StateActionQueue(New<TActionQueue>())
    , SnapshotActionQueue(New<TActionQueue>())
{ }

void TCompositeMetaState::RegisterPart(TMetaStatePart::TPtr part)
{
    Stroka partName = part->GetPartName();
    YVERIFY(Parts.insert(MakePair(partName, part)).Second());
}

IInvoker::TPtr TCompositeMetaState::GetInvoker() const
{
    return StateActionQueue->GetInvoker();
}

TFuture<TVoid>::TPtr TCompositeMetaState::Save(TOutputStream* output)
{
    TFuture<TVoid>::TPtr result;
    FOREACH(auto& pair, Parts) {
        result = pair.Second()->Save(output);
    }
    return result;
}

TFuture<TVoid>::TPtr TCompositeMetaState::Load(TInputStream* input)
{
    TFuture<TVoid>::TPtr result;
    FOREACH(auto& pair, Parts) {
        result = pair.Second()->Load(input);
    }
    return result;
}

void TCompositeMetaState::ApplyChange(const TRef& changeData)
{
    NMetaState::NProto::TMsgChangeHeader header;
    TRef messageData;
    DeserializeChange(
        changeData,
        &header,
        &messageData);

    Stroka changeType = header.GetChangeType();

    auto it = Methods.find(changeType);
    YASSERT(it != Methods.end());

    it->Second()->Do(messageData);
}

void TCompositeMetaState::Clear()
{
    FOREACH(auto& pair, Parts) {
        pair.Second()->Clear();
    }
}

void TCompositeMetaState::OnStartLeading()
{
    StartEpoch();
    FOREACH(auto& pair, Parts) {
        pair.Second()->OnStartLeading();
    }
}

void TCompositeMetaState::OnStopLeading()
{
    FOREACH(auto& pair, Parts) {
        pair.Second()->OnStopLeading();
    }
    StopEpoch();
}

void TCompositeMetaState::OnStartFollowing()
{
    StartEpoch();
    FOREACH(auto& pair, Parts) {
        pair.Second()->OnStartFollowing();
    }
}

void TCompositeMetaState::OnStopFollowing()
{
    FOREACH(auto& pair, Parts) {
        pair.Second()->OnStopFollowing();
    }
    StopEpoch();
}

void TCompositeMetaState::StartEpoch()
{
    YASSERT(~EpochStateInvoker == NULL);
    EpochStateInvoker = New<TCancelableInvoker>(StateActionQueue->GetInvoker());
}

void TCompositeMetaState::StopEpoch()
{
    if (~EpochStateInvoker != NULL) {
        EpochStateInvoker->Cancel();
        EpochStateInvoker.Drop();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
