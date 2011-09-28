#include "registry_service.h"
#include "registry_service.pb.h"

#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../misc/assert.h"
#include "../misc/string.h"

namespace NYT {
namespace NRegistry {

using namespace NMetaState;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RegistryLogger;

////////////////////////////////////////////////////////////////////////////////

class TRegistryService::TState
    : public NMetaState::TMetaStatePart
    , public NTransaction::ITransactionHandler
{
public:
    typedef TIntrusivePtr<TState> TPtr;

    TState(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , TransactionManager(transactionManager)
    {
//        RegisterMethod(this, &TState::AddChunk);

        transactionManager->RegisterHander(this);
    }

    METAMAP_ACCESSORS_DECL(Node, IRegistryNode, TNodeId);

private:
    TConfig Config;
    TTransactionManager::TPtr TransactionManager;
    
    TMetaStateMap<TLockId, TLock> Locks;
    TMetaStateMap<TNodeId, IRegistryNode, TMetaStateMapPtrTraits<IRegistryNode> > NodeMap;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "RegistryService";
    }

    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream)
    {
    	YASSERT(false);
        UNUSED(stream);
    	return NULL;
    }

    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream)
    {
    	YASSERT(false);
        UNUSED(stream);
    	return NULL;
    }

    virtual void Clear()
    {
    }

    // ITransactionHandler overrides.
    virtual void OnTransactionStarted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionCommitted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionAborted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }
};

METAMAP_ACCESSORS_IMPL(TRegistryService::TState, Node, IRegistryNode, TNodeId, NodeMap)

////////////////////////////////////////////////////////////////////////////////

TRegistryService::TRegistryService(
    const TConfig& config,
    NMetaState::TMetaStateManager::TPtr metaStateManager,
    NMetaState::TCompositeMetaState::TPtr metaState,
    NRpc::TServer::TPtr server,
    TTransactionManager::TPtr transactionManager)
    : TMetaStateServiceBase(
        metaState->GetInvoker(),
        TRegistryServiceProxy::GetServiceName(),
        RegistryLogger.GetCategory())
    , Config(config)
    , TransactionManager(transactionManager)
    , State(New<TState>(
        config,
        metaStateManager,
        metaState,
        transactionManager))
{
    RegisterMethods();
    metaState->RegisterPart(~State);
    server->RegisterService(this);
}

void TRegistryService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Get));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Set));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Lock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Remove));
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TRegistryService, Get)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TRegistryService, Set)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    Stroka value = request->GetValues();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TRegistryService, Remove)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TRegistryService, Lock)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    auto mode = ELockMode(request->GetMode());

    context->SetRequestInfo("TransactionId: %s, Path: %s, Mode: %s",
        ~transactionId.ToString(),
        ~path,
        ~mode.ToString());

    UNUSED(response);
    YASSERT(false);
}

////////////////////////////////////////////////////////////////////////////////

void TMapNode::Clear()
{
    // TODO:
    YASSERT(false);
}

int TMapNode::GetChildCount() const
{
    return NameToChild.ysize();
}

yvector< TPair<Stroka, INode::TConstPtr> > TMapNode::GetChildren() const
{
    yvector< TPair<Stroka, INode::TConstPtr> > result;
    result.reserve(NameToChild.ysize());
    FOREACH (const auto& pair, NameToChild) {
        const auto& child = RegistryService->GetNode(pair.Second());
    // TODO: const_cast is due to stupid intrusiveptr
        result.push_back(MakePair(
            pair.First(),
            const_cast<IRegistryNode*>(&child)));
    }
    return result;
}


INode::TConstPtr TMapNode::FindChild(const Stroka& name) const
{
    auto it = NameToChild.find(name);
    // TODO: const_cast is due to stupid intrusiveptr
    return
        it == NameToChild.end()
        ? NULL
        : const_cast<IRegistryNode*>(&RegistryService->GetNode(it->Second()));
}

bool TMapNode::AddChild(INode::TPtr child, const Stroka& name)
{
    auto* registryChild = dynamic_cast<IRegistryNode*>(~child);
    YASSERT(registryChild != NULL);
    auto childId = registryChild->GetId();
    if (NameToChild.insert(MakePair(name, childId)).Second()) {
        YVERIFY(ChildToName.insert(MakePair(childId, name)).Second());
        child->SetParent(this);
        return true;
    } else {
        return false;
    }
}
/*
bool TMapNode::RemoveChild(const Stroka& name)
{
    auto it = NameToChild.find(name);
    if (it == NameToChild.end())
        return false;

    auto child = it->Second(); 
    child->AsMutable()->SetParent(NULL);
    NameToChild.erase(it);
    YVERIFY(ChildToName.erase(child) == 1);

    return true;
}

void TMapNode::RemoveChild( INode::TPtr child )
{
    auto it = ChildToName.find(child);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();
    ChildToName.erase(it);
    YVERIFY(NameToChild.erase(name) == 1);
}

void TMapNode::ReplaceChild( INode::TPtr oldChild, INode::TPtr newChild )
{
    if (oldChild == newChild)
        return;

    auto it = ChildToName.find(oldChild);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();

    oldChild->SetParent(NULL);
    ChildToName.erase(it);

    NameToChild[name] = newChild;
    newChild->SetParent(this);
    YVERIFY(ChildToName.insert(MakePair(newChild, name)).Second());
}

IYPathService::TNavigateResult TMapNode::Navigate(
    const TYPath& path) const
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        return TNavigateResult::CreateError(Sprintf("Child %s it not found",
            ~prefix.Quote()));
    } else {
        return TNavigateResult::CreateRecurse(child, tailPath);
    }
}

IYPathService::TSetResult TMapNode::Set(
    const TYPath& path,
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        auto newChild = GetFactory()->CreateMap();
        AddChild(~newChild, prefix);
        return TSetResult::CreateRecurse(~newChild, tailPath);
    } else {
        return TSetResult::CreateRecurse(child, tailPath);
    }
}
*/
////////////////////////////////////////////////////////////////////////////////

} // namespace NRegistry
} // namespace NYT
