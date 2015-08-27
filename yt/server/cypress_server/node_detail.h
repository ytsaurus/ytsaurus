#pragma once

#include "node.h"
#include "cypress_manager.h"
#include "helpers.h"
#include "type_handler.h"

#include <core/misc/serialize.h>

#include <core/ytree/node_detail.h>
#include <core/ytree/fluent.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/ypath.pb.h>

#include <server/object_server/object_detail.h>
#include <server/object_server/attribute_set.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialize.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TNontemplateCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    virtual bool IsExternalizable();

protected:
    NCellMaster::TBootstrap* const Bootstrap_;


    bool IsLeader() const;
    bool IsRecovery() const;

    void DestroyCore(TCypressNodeBase* node);

    void BranchCore(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);

    void MergeCore(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode);

    TCypressNodeBase* CloneCorePrologue(
        ICypressNodeFactoryPtr factory,
        const TNodeId& hintId,
        NObjectClient::TCellTag externalCellTag);

    void CloneCoreEpilogue(
        TCypressNodeBase* sourceNode,
        TCypressNodeBase* clonedNode,
        ICypressNodeFactoryPtr factory);

};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCypressNodeTypeHandlerBase
    : public TNontemplateCypressNodeTypeHandlerBase
{
public:
    explicit TCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : TNontemplateCypressNodeTypeHandlerBase(bootstrap)
    { }

    virtual ICypressNodeProxyPtr GetProxy(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(static_cast<TImpl*>(trunkNode), transaction);
    }

    virtual std::unique_ptr<TCypressNodeBase> Instantiate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag) override
    {
        std::unique_ptr<TCypressNodeBase> nodeHolder(new TImpl(id));
        nodeHolder->SetTrunkNode(nodeHolder.get());
        nodeHolder->SetExternalCellTag(externalCellTag);
        return nodeHolder;
    }

    virtual std::unique_ptr<TCypressNodeBase> Create(
        const TNodeId& hintId,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(GetObjectType(), hintId);
        return DoCreate(
            TVersionedNodeId(id),
            externalCellTag,
            transaction,
            attributes);
    }

    virtual void Destroy(TCypressNodeBase* node) override
    {
        // Run core stuff.
        DestroyCore(node);

        // Run custom stuff.
        DoDestroy(dynamic_cast<TImpl*>(node));
    }

    virtual std::unique_ptr<TCypressNodeBase> Branch(
        TCypressNodeBase* originatingNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode) override
    {
        auto* typedOriginatingNode = dynamic_cast<TImpl*>(originatingNode);

        // Instantiate a branched copy.
        auto originatingId = originatingNode->GetVersionedId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));
        auto branchedNode = std::make_unique<TImpl>(branchedId);

        // Run core stuff.
        BranchCore(
            originatingNode,
            branchedNode.get(),
            transaction,
            mode);

        // Run custom stuff.
        DoBranch(
            typedOriginatingNode,
            branchedNode.get());

        return std::move(branchedNode);
    }

    virtual void Unbranch(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) override
    {
        // Run custom stuff.
        DoUnbranch(dynamic_cast<TImpl*>(originatingNode), dynamic_cast<TImpl*>(branchedNode));
    }

    virtual void Merge(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) override
    {
        // Run core stuff.
        MergeCore(originatingNode, branchedNode);

        // Run custom stuff.
        DoMerge(dynamic_cast<TImpl*>(originatingNode), dynamic_cast<TImpl*>(branchedNode));
    }

    virtual TCypressNodeBase* Clone(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactoryPtr factory,
        const TNodeId& hintId,
        ENodeCloneMode mode) override
    {
        // Run core prologue stuff.
        auto* clonedNode = CloneCorePrologue(
            factory,
            hintId,
            sourceNode->GetExternalCellTag());

        // Run custom stuff.
        DoClone(
            dynamic_cast<TImpl*>(sourceNode),
            dynamic_cast<TImpl*>(clonedNode),
            factory,
            mode);

        // Run core epilogue stuff.
        CloneCoreEpilogue(sourceNode, clonedNode, factory);

        return clonedNode;
    }

    virtual NSecurityServer::TClusterResources GetTotalResourceUsage(
        const TCypressNodeBase* node) override
    {
        NSecurityServer::TClusterResources result;
        result.NodeCount = 1;
        return result;
    }

    virtual NSecurityServer::TClusterResources GetAccountingResourceUsage(
        const TCypressNodeBase* node) override
    {
        NSecurityServer::TClusterResources result;
        result.NodeCount = 1;
        return result;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual std::unique_ptr<TImpl> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* /*transaction*/,
        NYTree::IAttributeDictionary* /*attributes*/    )
    {
        auto nodeHolder = std::make_unique<TImpl>(id);
        nodeHolder->SetExternalCellTag(externalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());
        return nodeHolder;
    }

    virtual void DoDestroy(TImpl* /*node*/)
    { }

    virtual void DoBranch(
        const TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoMerge(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoUnbranch(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoClone(
        TImpl* /*sourceNode*/,
        TImpl* /*clonedNode*/,
        ICypressNodeFactoryPtr /*factory*/,
        ENodeCloneMode /*mode*/)
    { }

};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<Stroka>
    : NYTree::NDetail::TScalarTypeTraits<Stroka>
{
    static const NObjectClient::EObjectType ObjectType;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const NObjectClient::EObjectType ObjectType;
};

template <>
struct TCypressScalarTypeTraits<ui64>
    : NYTree::NDetail::TScalarTypeTraits<ui64>
{
    static const NObjectClient::EObjectType ObjectType;
};

template <>
struct TCypressScalarTypeTraits<double>
    : NYTree::NDetail::TScalarTypeTraits<double>
{
    static const NObjectClient::EObjectType ObjectType;
};

template <>
struct TCypressScalarTypeTraits<bool>
    : NYTree::NDetail::TScalarTypeTraits<bool>
{
    static const NObjectClient::EObjectType ObjectType;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNode
    : public TCypressNodeBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(TValue, Value)

public:
    explicit TScalarNode(const TVersionedNodeId& id)
        : TCypressNodeBase(id)
        , Value_()
    { }

    virtual void Save(NCellMaster::TSaveContext& context) const override
    {
        TCypressNodeBase::Save(context);
        
        using NYT::Save;
        Save(context, Value_);
    }

    virtual void Load(NCellMaster::TLoadContext& context) override
    {
        TCypressNodeBase::Load(context);

        using NYT::Load;
        Load(context, Value_);
    }
};

typedef TScalarNode<Stroka> TStringNode;
typedef TScalarNode<i64>    TInt64Node;
typedef TScalarNode<ui64>   TUint64Node;
typedef TScalarNode<double> TDoubleNode;
typedef TScalarNode<bool>   TBooleanNode;

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNodeTypeHandler
    : public TCypressNodeTypeHandlerBase< TScalarNode<TValue> >
{
public:
    explicit TScalarNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual NObjectClient::EObjectType GetObjectType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType() override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

protected:
    typedef TCypressNodeTypeHandlerBase< TScalarNode<TValue> > TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TScalarNode<TValue>* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        branchedNode->Value() = originatingNode->Value();
    }

    virtual void DoMerge(
        TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->Value() = branchedNode->Value();
    }

    virtual void DoClone(
        TScalarNode<TValue>* sourceNode,
        TScalarNode<TValue>* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override
    {
        TBase::DoClone(sourceNode, clonedNode, factory, mode);

        clonedNode->Value() = sourceNode->Value();
    }

};

typedef TScalarNodeTypeHandler<Stroka> TStringNodeTypeHandler;
typedef TScalarNodeTypeHandler<i64>    TInt64NodeTypeHandler;
typedef TScalarNodeTypeHandler<ui64>   TUint64NodeTypeHandler;
typedef TScalarNodeTypeHandler<double> TDoubleNodeTypeHandler;
typedef TScalarNodeTypeHandler<bool>   TBooleanNodeTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCypressNodeBase
{
public:
    typedef yhash_map<Stroka, TCypressNodeBase*> TKeyToChild;
    typedef yhash_map<TCypressNodeBase*, Stroka> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);
    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    explicit TMapNode(const TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    explicit TMapNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TCypressNodeTypeHandlerBase<TMapNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TMapNode* node) override;

    virtual void DoBranch(
        const TMapNode* originatingNode,
        TMapNode* branchedNode) override;

    virtual void DoMerge(
        TMapNode* originatingNode,
        TMapNode* branchedNode) override;

    virtual void DoClone(
        TMapNode* sourceNode,
        TMapNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCypressNodeBase
{
public:
    typedef std::vector<TCypressNodeBase*> TIndexToChild;
    typedef yhash_map<TCypressNodeBase*, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    explicit TListNode(const TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    explicit TListNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TCypressNodeTypeHandlerBase<TListNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TListNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TListNode* node) override;

    virtual void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoClone(
        TListNode* sourceNode,
        TListNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkNode
    : public TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NObjectServer::TObjectId, TargetId);

public:
    explicit TLinkNode(const TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TLinkNode>
{
public:
    explicit TLinkNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TCypressNodeTypeHandlerBase<TLinkNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TLinkNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual std::unique_ptr<TLinkNode> DoCreate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag cellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes) override;

    virtual void DoBranch(
        const TLinkNode* originatingNode,
        TLinkNode* branchedNode) override;

    virtual void DoMerge(
        TLinkNode* originatingNode,
        TLinkNode* branchedNode) override;

    virtual void DoClone(
        TLinkNode* sourceNode,
        TLinkNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNode
    : public TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYTree::INodePtr, Value);

public:
    explicit TDocumentNode(const TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TDocumentNode>
{
public:
    explicit TDocumentNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() override;
    virtual NYTree::ENodeType GetNodeType() override;

private:
    typedef TCypressNodeTypeHandlerBase<TDocumentNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TDocumentNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TDocumentNode* originatingNode,
        TDocumentNode* branchedNode) override;

    virtual void DoMerge(
        TDocumentNode* originatingNode,
        TDocumentNode* branchedNode) override;

    virtual void DoClone(
        TDocumentNode* sourceNode,
        TDocumentNode* clonedNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
