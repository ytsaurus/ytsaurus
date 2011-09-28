#pragma once

#include "common.h"
#include "registry_service_rpc.h"

#include "../misc/enum.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_state_service.h"
#include "../meta_state/map.h"
#include "../rpc/server.h"
#include "../ytree/node.h"

#include "../ytree/node.h"

namespace NYT {
namespace NRegistry {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TLock
{
public:
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        ELockMode mode)
    : Id(id)
    , NodeId(nodeId)
    , TransactionId(transactionId)
    , Mode(mode)
    { }

    TLockId GetId() const
    {
        return Id;
    }

    TNodeId GetNodeId() const
    {
        return NodeId;
    }

    TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    ELockMode GetMode() const
    {
        return Mode;
    }

private:
    TLockId Id;
    TNodeId NodeId;
    TTransactionId TransactionId;
    ELockMode Mode;

};

////////////////////////////////////////////////////////////////////////////////

struct IRegistryNode
    : virtual INode
{
    virtual TNodeId GetId() const = 0;

    virtual TLockId GetLockId() const = 0;
    virtual void SetLockId(const TLockId& lockId) = 0;

    DECLARE_ENUM(ELockCode,
        (OK)
        (Error)
    );

    struct TLockResult
    {
        ELockCode Code;
        Stroka Message;
    };

    virtual TLockResult Lock(ELockMode mode) = 0;
};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class TRegistryService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TRegistryService> TPtr;
    typedef TRegistryServiceConfig TConfig;

    //! Creates an instance.
    TRegistryService(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        NRpc::TServer::TPtr server,
        TTransactionManager::TPtr transactionManager);

    METAMAP_ACCESSORS_DECL(Node, IRegistryNode, TNodeId);

private:
    typedef TRegistryService TThis;
    typedef TRegistryServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();
    

    RPC_SERVICE_METHOD_DECL(NProto, Get);
    RPC_SERVICE_METHOD_DECL(NProto, Set);
    RPC_SERVICE_METHOD_DECL(NProto, Remove);
    RPC_SERVICE_METHOD_DECL(NProto, Lock);

};

////////////////////////////////////////////////////////////////////////////////

class TRegistryNodeBase
    : public TNodeBase
    , public virtual IRegistryNode
{
public:
    TNodeId GetId() const
    {
        return Id;
    }

    TLockId GetLockId() const
    {
        return LockId;
    }

    void SetLockId(const TLockId& lockId)
    {
        LockId = lockId;
    }

protected:
    TRegistryService::TPtr RegistryService;
    TNodeId Id;
    TLockId LockId;

    TRegistryNodeBase(
        TRegistryService::TPtr registryService,
        const TNodeId& id)
        : RegistryService(registryService)
        , Id(id)
    { }

    virtual TNodeBase* AsMutableImpl() const
    {
        YASSERT(false);
        return NULL;
    }

    virtual const TNodeBase* AsImmutableImpl() const
    {
        return this;
    }

};

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
class TScalarNode
    : public TRegistryNodeBase
    , public virtual IBase
{
public:
    TScalarNode(
        TRegistryService::TPtr registryService,
        const TNodeId& id)
        : TRegistryNodeBase(registryService, id)
        , Value()
    { }

    virtual TValue GetValue() const
    {
        return Value;
    }

    virtual void SetValue(const TValue& value)
    {
        Value = value;
    }

private:
    TValue Value;

};

#define DECLARE_TYPE_OVERRIDES(name) \
    virtual ENodeType GetType() const \
    { \
        return ENodeType::name; \
    } \
    \
    virtual I ## name ## Node::TConstPtr As ## name() const \
    { \
        return const_cast<T ## name ## Node*>(this); \
    } \
    \
    virtual I ## name ## Node::TPtr As ## name() \
    { \
        return this; \
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
    public: \
        DECLARE_TYPE_OVERRIDES(name) \
    \
    };

////////////////////////////////////////////////////////////////////////////////

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TRegistryNodeBase
    , public virtual IMapNode
{
public:
    TMapNode(
        TRegistryService::TPtr registryService,
        const TNodeId& id)
        : TRegistryNodeBase(registryService, id)
    { }

    DECLARE_TYPE_OVERRIDES(Map)

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const;
    virtual INode::TConstPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(INode::TPtr child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

    virtual TNavigateResult Navigate(
        const TYPath& path) const;
    virtual TSetResult Set(
        const TYPath& path,
        TYsonProducer::TPtr producer);

private:
    yhash_map<Stroka, TNodeId> NameToChild;
    yhash_map<TNodeId, Stroka> ChildToName;

};

////////////////////////////////////////////////////////////////////////////////

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////



} // namespace NRegistry
} // namespace NYT
