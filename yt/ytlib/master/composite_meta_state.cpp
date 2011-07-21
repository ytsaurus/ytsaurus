#include "composite_meta_state.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TMetaStateServiceBase::TMetaStateServiceBase(
    IInvoker::TPtr serviceInvoker,
    Stroka serviceName,
    Stroka loggingCategory)
    : NRpc::TServiceBase(
        serviceInvoker,
        serviceName,
        loggingCategory)
{ }

void TMetaStateServiceBase::OnCommitError(NRpc::TServiceContext::TPtr context)
{
    context->Reply(NRpc::EErrorCode::ServiceError);
}

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

TMetaStatePart::TMetaStatePart(TMasterStateManager::TPtr stateManager)
    : StateManager(stateManager)
{ }

bool TMetaStatePart::IsLeader() const
{
    TMasterStateManager::EState state = StateManager->GetState();
    return state == TMasterStateManager::EState::Leading ||
           state == TMasterStateManager::EState::LeaderRecovery;
}

bool TMetaStatePart::IsFolllower() const
{
    TMasterStateManager::EState state = StateManager->GetState();
    return state == TMasterStateManager::EState::Following ||
           state == TMasterStateManager::EState::FollowerRecovery;
}

void TMetaStatePart::OnRegistered(IInvoker::TPtr snapshotInvoker)
{
    SnapshotInvoker = snapshotInvoker;
}

void TMetaStatePart::ApplyChange(Stroka changeType, TRef changeData)
{
    TMethodMap::iterator it = Methods.find(changeType);
    YASSERT(it != Methods.end());
    it->Second()->Do(changeData);
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
    part->OnRegistered(SnapshotInvoker);
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

    Stroka partName = header.GetPartName();
    Stroka changeType = header.GetChangeType();

    TPartMap::iterator it = Parts.find(partName);
    YASSERT(it != Parts.end());

    TMetaStatePart::TPtr part = it->Second();
    part->ApplyChange(changeType, messageData);
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
