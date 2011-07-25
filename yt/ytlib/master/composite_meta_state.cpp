#include "composite_meta_state.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

void DeserializeChangeHeader(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header)
{
    TFixedChangeHeader* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (fixedHeader),
        fixedHeader->HeaderSize));
}

void DeserializeChange(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header,
    TRef* messageData)
{
    TFixedChangeHeader* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(header->ParseFromArray(
        changeData.Begin() + sizeof (TFixedChangeHeader),
        fixedHeader->HeaderSize));
    *messageData = TRef(
        changeData.Begin() + sizeof (TFixedChangeHeader) + fixedHeader->HeaderSize,
        fixedHeader->MessageSize);
}

////////////////////////////////////////////////////////////////////////////////

TMetaStatePart::TMetaStatePart(
    TMasterStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState)
    : MetaStateManager(metaStateManager)
    , MetaState(metaState)
{ }

bool TMetaStatePart::IsLeader() const
{
    TMasterStateManager::EState state = MetaStateManager->GetState();
    return state == TMasterStateManager::EState::Leading;
}

bool TMetaStatePart::IsFolllower() const
{
    TMasterStateManager::EState state = MetaStateManager->GetState();
    return state == TMasterStateManager::EState::Following;
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
{ }

void TMetaStatePart::OnStopLeading()
{ }

void TMetaStatePart::OnStartFollowing()
{ }

void TMetaStatePart::OnStopFollowing()
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeMetaState::TCompositeMetaState()
    : StateInvoker(new TActionQueue())
    , SnapshotInvoker(new TActionQueue())
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

TAsyncResult<TVoid>::TPtr TCompositeMetaState::Save(TOutputStream& output)
{
    TAsyncResult<TVoid>::TPtr result;
    for (auto it = Parts.begin(); it != Parts.end(); ++it)
    {
        result = it->Second()->Save(output);
    }
    return result;
}

TAsyncResult<TVoid>::TPtr TCompositeMetaState::Load(TInputStream& input)
{
    TAsyncResult<TVoid>::TPtr result;
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        result = it->Second()->Load(input);
    }
    return result;
}

void TCompositeMetaState::ApplyChange(const TRef& changeData)
{
    NRpcMasterStateManager::TMsgChangeHeader header;
    TRef messageData;
    DeserializeChange(
        changeData,
        &header,
        &messageData);

    Stroka changeType = header.GetChangeType();

    TMethodMap::iterator it = Methods.find(changeType);
    YASSERT(it != Methods.end());

    it->Second()->Do(messageData);
}

void TCompositeMetaState::Clear()
{
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        it->Second()->Clear();
    }
}

void TCompositeMetaState::OnStartLeading()
{
    StartEpoch();
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        it->Second()->OnStartLeading();
    }
}

void TCompositeMetaState::OnStopLeading()
{
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        it->Second()->OnStopLeading();
    }
    StopEpoch();
}

void TCompositeMetaState::OnStartFollowing()
{
    StartEpoch();
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        it->Second()->OnStartFollowing();
    }
}

void TCompositeMetaState::OnStopFollowing()
{
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
    {
        it->Second()->OnStopFollowing();
    }
    StopEpoch();
}

void TCompositeMetaState::StartEpoch()
{
    YASSERT(~EpochStateInvoker == NULL);
    EpochStateInvoker = new TCancelableInvoker(StateInvoker);
}

void TCompositeMetaState::StopEpoch()
{
    YASSERT(~EpochStateInvoker != NULL);
    EpochStateInvoker->Cancel();
    EpochStateInvoker.Drop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
