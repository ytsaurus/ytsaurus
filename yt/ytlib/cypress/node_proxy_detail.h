#pragma once

#include "common.h"
#include "node_proxy.h"
#include "node_detail.h"
#include "cypress_ypath_rpc.pb.h"

#include "../ytree/ytree.h"
#include "../ytree/ypath_service.h"
#include "../ytree/ypath_detail.h"
#include "../ytree/node_detail.h"
#include "../ytree/serialize.h"
#include "../ytree/ephemeral.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public NYTree::INodeFactory
{
public:
    TNodeFactory(
        TCypressManager* cypressManager,
        const TTransactionId& transactionId);

    virtual NYTree::IStringNode::TPtr CreateString();
    virtual NYTree::IInt64Node::TPtr CreateInt64();
    virtual NYTree::IDoubleNode::TPtr CreateDouble();
    virtual NYTree::IMapNode::TPtr CreateMap();
    virtual NYTree::IListNode::TPtr CreateList();
    virtual NYTree::IEntityNode::TPtr CreateEntity();

private:
    const TCypressManager::TPtr CypressManager;
    const TTransactionId TransactionId;

};

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCypressNodeProxyBase
    : public NYTree::TNodeBase
    , public ICypressNodeProxy
    , public virtual IBase
{
public:
    typedef TIntrusivePtr<TCypressNodeProxyBase> TPtr;

    TCypressNodeProxyBase(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TypeHandler(typeHandler)
        , CypressManager(cypressManager)
        , TransactionId(transactionId)
        , NodeId(nodeId)
        , NodeFactory(cypressManager, transactionId)
        , Locked(false)
    {
        YASSERT(typeHandler != NULL);
        YASSERT(cypressManager != NULL);
    }

    NYTree::INodeFactory* GetFactory() const
    {
        return &NodeFactory;
    }

    virtual TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    virtual TNodeId GetNodeId() const
    {
        return NodeId;
    }


    virtual NYTree::ENodeType GetType() const
    {
        return TypeHandler->GetNodeType();
    }


    virtual const ICypressNode& GetImpl() const
    {
        return this->GetTypedImpl();
    }

    virtual ICypressNode& GetImplForUpdate()
    {
        return this->GetTypedImplForUpdate();
    }


    virtual NYTree::ICompositeNode::TPtr GetParent() const
    {
        auto nodeId = GetImpl().GetParentId();
        return nodeId == NullNodeId ? NULL : GetProxy(nodeId)->AsComposite();
    }

    virtual void SetParent(NYTree::ICompositeNode::TPtr parent)
    {
        GetImplForUpdate().SetParentId(
            ~parent == NULL
            ? NullNodeId
            : ToProxy(~parent)->GetNodeId());
    }


    virtual NYTree::IMapNode::TPtr GetAttributes() const
    {
        auto nodeId = GetImpl().GetAttributesId();
        return nodeId == NullNodeId ? NULL : GetProxy(nodeId)->AsMap();
    }

    virtual void SetAttributes(NYTree::IMapNode::TPtr attributes)
    {
        auto& impl = GetImplForUpdate();
        if (impl.GetAttributesId() != NullNodeId) {
            auto& attrImpl = GetImplForUpdate(impl.GetAttributesId());
            DetachChild(attrImpl);
            impl.SetAttributesId(NullNodeId);
        }

        if (~attributes != NULL) {
            auto* attrProxy = ToProxy(~attributes);
            auto& attrImpl = GetImplForUpdate(attrProxy->GetNodeId());
            AttachChild(attrImpl);
            impl.SetAttributesId(attrProxy->GetNodeId());
        }
    }

    
    virtual INodeTypeHandler::TPtr GetTypeHandler() const
    {
        return TypeHandler;
    }


    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        Stroka verb = context->GetVerb();
        if (verb == "Set" ||
            verb == "Remove" ||
            verb == "Lock")
        {
            return true;
        }
        return false;
    }

    virtual bool IsTransactionRequired(NRpc::IServiceContext* context) const
    {
        if (TransactionId != NullTransactionId) {
            return false;
        }
        
        Stroka verb = context->GetVerb();
        if (verb == "Lock") {
            return false;
        }

        return IsLogged(context);
    }

protected:
    const INodeTypeHandler::TPtr TypeHandler;
    const TCypressManager::TPtr CypressManager;
    const TTransactionId TransactionId;
    const TNodeId NodeId;

    mutable TNodeFactory NodeFactory;
    //! Keeps a cached flag that gets raised when the node is locked.
    bool Locked;


    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "Lock") {
            LockThunk(context);
        } else if (verb == "GetId") {
            GetIdThunk(context);
        } else if (verb == "Create") {
            CreateThunk(context);
        } else {
            TNodeBase::DoInvoke(context);
        }
    }


    DECLARE_RPC_SERVICE_METHOD(NProto, Lock)
    {
        UNUSED(request);
        UNUSED(response);

        DoLock();
        context->Reply();
    }
    
    DECLARE_RPC_SERVICE_METHOD(NProto, GetId)
    {
        UNUSED(request);

        response->set_nodeid(GetNodeId().ToProto());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Create)
    {
        UNUSED(request);
        UNUSED(response);

        if (NYTree::IsFinalYPath(context->GetPath())) {
            ythrow yexception() << "Node already exists";
        }

        context->Reply(NRpc::EErrorCode::NoSuchVerb, "Verb is not supported");
    }


    virtual yvector<Stroka> GetVirtualAttributeNames()
    {
        yvector<Stroka> names;
        TypeHandler->GetAttributeNames(GetImpl(), &names);
        return names;
    }

    virtual NYTree::IYPathService::TPtr GetVirtualAttributeService(const Stroka& name)
    {
        return TypeHandler->GetAttributeService(GetImpl(), name);
    }


    const ICypressNode& GetImpl(const TNodeId& nodeId) const
    {
        return CypressManager->GetTransactionNode(nodeId, TransactionId);
    }

    ICypressNode& GetImplForUpdate(const TNodeId& nodeId) const
    {
        return CypressManager->GetTransactionNodeForUpdate(nodeId, TransactionId);
    }


    const TImpl& GetTypedImpl() const
    {
        return dynamic_cast<const TImpl&>(GetImpl(NodeId));
    }

    TImpl& GetTypedImplForUpdate()
    {
        return dynamic_cast<TImpl&>(GetImplForUpdate(NodeId));
    }


    ICypressNodeProxy::TPtr GetProxy(const TNodeId& nodeId) const
    {
        YASSERT(nodeId != NullNodeId);
        return CypressManager->GetNodeProxy(nodeId, TransactionId);
    }

    static ICypressNodeProxy* ToProxy(INode* node)
    {
        YASSERT(node != NULL);
        return &dynamic_cast<ICypressNodeProxy&>(*node);
    }


    void EnsureLocked()
    {
        // A shortcut.
        if (Locked)
            return;

        if (CypressManager->IsTransactionNodeLocked(NodeId, TransactionId))
            return;

        DoLock();
    }

    void DoLock()
    {
        CypressManager->LockTransactionNode(NodeId, TransactionId);

        // Set the flag to speedup further checks.
        Locked = true;
    }


    void AttachChild(ICypressNode& child)
    {
        // One can only attach nodes that are created 
        // by the current transaction.
        YASSERT(child.GetState() == ENodeState::Uncommitted);

        child.SetParentId(NodeId);
        CypressManager->RefNode(child);
    }

    void DetachChild(ICypressNode& child)
    {
        child.SetParentId(NullNodeId);
        CypressManager->UnrefNode(child);
    }
};

//////////////////////////////////////////////////////////////////////////////// 

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    TScalarNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            typeHandler,
            cypressManager,
            transactionId,
            nodeId)
    { }

    virtual TValue GetValue() const
    {
        return this->GetTypedImpl().Value();
    }

    virtual void SetValue(const TValue& value)
    {
        this->EnsureLocked();
        this->GetTypedImplForUpdate().Value() = value;
    }
};

//////////////////////////////////////////////////////////////////////////////// 

#define DECLARE_SCALAR_TYPE(name, type) \
    class T##name##NodeProxy \
        : public TScalarNodeProxy<type, NYTree::I##name##Node, T##name##Node> \
    { \
        YTREE_NODE_TYPE_OVERRIDES(name) \
    \
    public: \
        T##name##NodeProxy( \
            INodeTypeHandler* typeHandler, \
            TCypressManager* cypressManager, \
            const TTransactionId& transactionId, \
            const TNodeId& nodeId) \
            : TScalarNodeProxy<type, NYTree::I##name##Node, T##name##Node>( \
                typeHandler, \
                cypressManager, \
                transactionId, \
                nodeId) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxy::TPtr TScalarNodeTypeHandler<type>::GetProxy( \
        const ICypressNode& node, \
        const TTransactionId& transactionId) \
    { \
        return New<T##name##NodeProxy>( \
            this, \
            ~CypressManager, \
            transactionId, \
            node.GetId().NodeId); \
    }

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCompositeNodeProxyBase
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    virtual TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const
    {
        return this;
    }

    virtual TIntrusivePtr<NYTree::ICompositeNode> AsComposite()
    {
        return this;
    }

protected:
    typedef TCypressNodeProxyBase<IBase, TImpl> TBase;

    TCompositeNodeProxyBase(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            typeHandler,
            cypressManager,
            transactionId,
            nodeId)
    { }

    virtual void CreateRecursive(
        const NYTree::TYPath& path,
        NYTree::INode* value) = 0;

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "Create") {
            CreateThunk(context);
        } else {
            TBase::DoInvoke(context);
        }
    }

    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        Stroka verb = context->GetVerb();
        if (verb == "Create") {
            return true;
        } else {
            return TBase::IsLogged(context);
        }
    }

protected:
    DECLARE_RPC_SERVICE_METHOD(NProto, Create)
    {
        if (NYTree::IsFinalYPath(context->GetPath())) {
            // This should throw an exception.
            TBase::Create(request, response, context);
            return;
        }

        Stroka typeName = request->type();

        auto manifest = NYTree::DeserializeFromYson(request->manifest());
        auto value = this->CypressManager->CreateDynamicNode(
            this->TransactionId,
            typeName,
            ~manifest);
        CreateRecursive(context->GetPath(), ~value);

        response->set_nodeid(value->GetNodeId().ToProto());

        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeProxy
    : public TCompositeNodeProxyBase<NYTree::IMapNode, TMapNode>
    , public NYTree::TMapNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(Map)

public:
    TMapNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, NYTree::INode::TPtr> > GetChildren() const;
    virtual INode::TPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(NYTree::INode::TPtr child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(NYTree::INode::TPtr oldChild, NYTree::INode::TPtr newChild);
    virtual void RemoveChild(NYTree::INode::TPtr child);
    virtual Stroka GetChildKey(INode* child);

protected:
    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual void CreateRecursive(const NYTree::TYPath& path, INode* value);
    virtual IYPathService::TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb);
    virtual void SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet::TPtr context);
    virtual void SetNodeRecursive(const NYTree::TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode::TPtr context);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCompositeNodeProxyBase<NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    TListNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<INode::TPtr> GetChildren() const;
    virtual INode::TPtr FindChild(int index) const;
    virtual void AddChild(NYTree::INode::TPtr child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(NYTree::INode::TPtr oldChild, NYTree::INode::TPtr newChild);
    virtual void RemoveChild(NYTree::INode::TPtr child);
    virtual int GetChildIndex(INode* child);

protected:
    virtual void CreateRecursive(const NYTree::TYPath& path, INode* value);
    virtual TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb);
    virtual void SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet::TPtr context);
    virtual void SetNodeRecursive(const NYTree::TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode::TPtr context);

};

////////////////////////////////////////////////////////////////////////////////

class TRootNodeProxy
    : public TMapNodeProxy
{
public:
    TRootNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

protected:
    virtual IYPathService::TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
