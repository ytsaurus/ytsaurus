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
    , IsRecovery_(false)
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

bool TMetaStatePart::IsRecovery() const
{
    return IsRecovery_;
}

IInvoker::TPtr TMetaStatePart::GetSnapshotInvoker() const
{
    return MetaState->SnapshotInvoker;
}

IInvoker::TPtr TMetaStatePart::GetStateInvoker() const
{
    return MetaState->StateInvoker;
}

IInvoker::TPtr TMetaStatePart::GetEpochStateInvoker() const
{
    YASSERT(~MetaState->EpochStateInvoker != NULL);
    return ~MetaState->EpochStateInvoker;
}

void TMetaStatePart::OnStartLeading()
{
    YASSERT(Role == ERole::None);
    YASSERT(!IsRecovery_);

    Role = ERole::Leader;
    IsRecovery_ = true;
}

void TMetaStatePart::OnStopLeading()
{
    YASSERT(Role == ERole::Leader);
    YASSERT(!IsRecovery_);

    Role = ERole::None;
}

void TMetaStatePart::OnStartFollowing()
{
    YASSERT(Role == ERole::None);
    YASSERT(!IsRecovery_);

    Role = ERole::Follower;
    IsRecovery_ = true;
}

void TMetaStatePart::OnStopFollowing()
{
    YASSERT(Role == ERole::Follower);
    YASSERT(!IsRecovery_);

    Role = ERole::None;
}

void TMetaStatePart::OnRecoveryComplete()
{
    YASSERT(Role == ERole::Leader || Role == ERole::Follower);
    YASSERT(IsRecovery_);

    IsRecovery_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeMetaState::TCompositeMetaState()
    : StateInvoker(~New<TActionQueue>())
    , SnapshotInvoker(~New<TActionQueue>())
{ }

void TCompositeMetaState::RegisterPart(TMetaStatePart::TPtr part)
{
    Stroka partName = part->GetPartName();
    YVERIFY(Parts.insert(MakePair(partName, part)).Second());
}

IInvoker::TPtr TCompositeMetaState::GetInvoker() const
{
    return StateInvoker;
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

void TCompositeMetaState::OnRecoveryComplete()
{
    FOREACH(auto& pair, Parts) {
        pair.Second()->OnRecoveryComplete();
    }
}

void TCompositeMetaState::StartEpoch()
{
    YASSERT(~EpochStateInvoker == NULL);
    EpochStateInvoker = New<TCancelableInvoker>(StateInvoker);
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
