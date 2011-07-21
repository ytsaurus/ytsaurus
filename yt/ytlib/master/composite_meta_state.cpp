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
    return state == TMasterStateManager::EState::Leading ||
           state == TMasterStateManager::EState::LeaderRecovery;
}

bool TMetaStatePart::IsFolllower() const
{
    TMasterStateManager::EState state = MetaStateManager->GetState();
    return state == TMasterStateManager::EState::Following ||
           state == TMasterStateManager::EState::FollowerRecovery;
}

IInvoker::TPtr TMetaStatePart::GetSnapshotInvoker() const
{
    return MetaState->SnapshotInvoker;
}

IInvoker::TPtr TMetaStatePart::GetStateInvoker() const
{
    return MetaState->StateInvoker;
}

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
    for (TPartMap::iterator it = Parts.begin();
         it != Parts.end();
         ++it)
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

void TCompositeMetaState::ApplyChange(TRef changeData)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
