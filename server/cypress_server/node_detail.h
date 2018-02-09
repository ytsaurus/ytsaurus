#pragma once

#include "cypress_manager.h"
#include "helpers.h"
#include "node.h"
#include "private.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/object_server/attribute_set.h>
#include <yt/server/object_server/object_detail.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/security_manager.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeTypeHandlerBase
    : public INodeTypeHandler
{
public:
    explicit TNontemplateCypressNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

    virtual bool IsExternalizable() const override;

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    NObjectServer::TObjectTypeMetadata Metadata_;


    bool IsLeader() const;
    bool IsRecovery() const;

    void DestroyCore(TCypressNodeBase* node);

    void BranchCore(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& lockRequest);

    void MergeCore(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode);

    TCypressNodeBase* CloneCorePrologue(
        ICypressNodeFactory* factory,
        const TNodeId& hintId,
        NObjectClient::TCellTag externalCellTag);

    void CloneCoreEpilogue(
        TCypressNodeBase* sourceNode,
        TCypressNodeBase* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode);

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
        return DoGetProxy(trunkNode->As<TImpl>(), transaction);
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
        NYTree::IAttributeDictionary* attributes,
        NSecurityServer::TAccount* account,
        bool enableAccounting) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(GetObjectType(), hintId);
        return DoCreate(
            TVersionedNodeId(id),
            externalCellTag,
            transaction,
            attributes,
            account,
            enableAccounting);
    }

    virtual void Destroy(TCypressNodeBase* node) override
    {
        // Run core stuff.
        DestroyCore(node);

        // Run custom stuff.
        DoDestroy(node->As<TImpl>());
    }

    virtual std::unique_ptr<TCypressNodeBase> Branch(
        TCypressNodeBase* originatingNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& lockRequest) override
    {
        // Instantiate a branched copy.
        auto originatingId = originatingNode->GetVersionedId();
        auto branchedId = TVersionedNodeId(originatingId.ObjectId, GetObjectId(transaction));
        auto branchedNodeHolder = std::make_unique<TImpl>(branchedId);
        auto* typedBranchedNode = branchedNodeHolder.get();

        // Run core stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        BranchCore(typedOriginatingNode, typedBranchedNode, transaction, lockRequest);

        // Run custom stuff.
        DoBranch(typedOriginatingNode, typedBranchedNode, lockRequest);
        DoLogBranch(typedOriginatingNode, typedBranchedNode, lockRequest);

        return std::move(branchedNodeHolder);
    }

    virtual void Unbranch(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) override
    {
        // Run custom stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        auto* typedBranchedNode = branchedNode->As<TImpl>();
        DoUnbranch(typedOriginatingNode, typedBranchedNode);
        DoLogUnbranch(typedOriginatingNode, typedBranchedNode);
    }

    virtual void Merge(
        TCypressNodeBase* originatingNode,
        TCypressNodeBase* branchedNode) override
    {
        // Run core stuff.
        auto* typedOriginatingNode = originatingNode->As<TImpl>();
        auto* typedBranchedNode = branchedNode->As<TImpl>();
        MergeCore(typedOriginatingNode, typedBranchedNode);

        // Run custom stuff.
        DoMerge(typedOriginatingNode, typedBranchedNode);
        DoLogMerge(typedOriginatingNode, typedBranchedNode);
    }

    virtual TCypressNodeBase* Clone(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactory* factory,
        const TNodeId& hintId,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        // Run core prologue stuff.
        auto* clonedNode = CloneCorePrologue(
            factory,
            hintId,
            sourceNode->GetExternalCellTag());

        // Run custom stuff.
        auto* typedSourceNode = sourceNode->template As<TImpl>();
        auto* typedClonedNode = clonedNode->template As<TImpl>();
        DoClone(typedSourceNode, typedClonedNode, factory, mode, account);

        // Run core epilogue stuff.
        CloneCoreEpilogue(
            sourceNode,
            clonedNode,
            factory,
            mode);

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
        NYTree::IAttributeDictionary* /*attributes*/,
        NSecurityServer::TAccount* account,
        bool enableAccounting)
    {
        auto nodeHolder = std::make_unique<TImpl>(id);
        nodeHolder->SetExternalCellTag(externalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(account, user, NSecurityServer::EPermission::Use);
        securityManager->SetAccount(nodeHolder.get(), account);

        return nodeHolder;
    }

    virtual void DoDestroy(TImpl* node)
    {
        const auto& securityManager = this->Bootstrap_->GetSecurityManager();
        securityManager->ResetAccount(node);
    }

    virtual void DoBranch(
        const TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/,
        const TLockRequest& /*lockRequest*/)
    { }

    virtual void DoLogBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest)
    {
        const auto& Logger = CypressServerLogger;
        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, Mode: %v, LockTimestamp: %llx)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId(),
            lockRequest.Mode,
            lockRequest.Timestamp);
    }

    virtual void DoMerge(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoLogMerge(
        TImpl* originatingNode,
        TImpl* branchedNode)
    {
        const auto& Logger = CypressServerLogger;
        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node merged (OriginatingNodeId: %v, BranchedNodeId: %v)",
            originatingNode->GetVersionedId(),
            branchedNode->GetVersionedId());
    }

    virtual void DoUnbranch(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoLogUnbranch(
        TImpl* /*originatingNode*/,
        TImpl* /*branchedNode*/)
    { }

    virtual void DoClone(
        TImpl* /*sourceNode*/,
        TImpl* /*clonedNode*/,
        ICypressNodeFactory* /*factory*/,
        ENodeCloneMode /*mode*/,
        NSecurityServer::TAccount* /*account*/)
    { }

};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
struct TCypressScalarTypeTraits
{ };

template <>
struct TCypressScalarTypeTraits<TString>
    : public NYTree::NDetail::TScalarTypeTraits<TString>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<i64>
    : public NYTree::NDetail::TScalarTypeTraits<i64>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<ui64>
    : public NYTree::NDetail::TScalarTypeTraits<ui64>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<double>
    : public NYTree::NDetail::TScalarTypeTraits<double>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
};

template <>
struct TCypressScalarTypeTraits<bool>
    : public NYTree::NDetail::TScalarTypeTraits<bool>
{
    static const NObjectClient::EObjectType ObjectType;
    static const NYTree::ENodeType NodeType;
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

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

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

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TScalarNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TScalarNode<TValue>>
{
public:
    explicit TScalarNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual NObjectClient::EObjectType GetObjectType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::ObjectType;
    }

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

protected:
    typedef TCypressNodeTypeHandlerBase<TScalarNode<TValue>> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TScalarNode<TValue>* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TScalarNode<TValue>* originatingNode,
        TScalarNode<TValue>* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

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
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

        clonedNode->Value() = sourceNode->Value();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCypressNodeBase
{
public:
    typedef THashMap<TString, TCypressNodeBase*> TKeyToChild;
    typedef THashMap<TCypressNodeBase*, TString> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);
    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    explicit TMapNode(const TVersionedNodeId& id);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TMapNode>
{
public:
    explicit TMapNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
    typedef TCypressNodeTypeHandlerBase<TMapNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TMapNode* node) override;

    virtual void DoBranch(
        const TMapNode* originatingNode,
        TMapNode* branchedNode,
        const TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TMapNode* originatingNode,
        TMapNode* branchedNode) override;

    virtual void DoClone(
        TMapNode* sourceNode,
        TMapNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCypressNodeBase
{
public:
    typedef std::vector<TCypressNodeBase*> TIndexToChild;
    typedef THashMap<TCypressNodeBase*, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    explicit TListNode(const TVersionedNodeId& id);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TListNode>
{
public:
    explicit TListNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
    typedef TCypressNodeTypeHandlerBase<TListNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TListNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoDestroy(TListNode* node) override;

    virtual void DoBranch(
        const TListNode* originatingNode,
        TListNode* branchedNode,
        const TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TListNode* originatingNode,
        TListNode* branchedNode) override;

    virtual void DoClone(
        TListNode* sourceNode,
        TListNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkNode
    : public TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TargetPath);

public:
    explicit TLinkNode(const TVersionedNodeId& id);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TLinkNode>
{
public:
    explicit TLinkNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
    typedef TCypressNodeTypeHandlerBase<TLinkNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TLinkNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual std::unique_ptr<TLinkNode> DoCreate(
        const TVersionedNodeId& id,
        NObjectClient::TCellTag cellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes,
        NSecurityServer::TAccount* account,
        bool enableAccounting) override;

    virtual void DoBranch(
        const TLinkNode* originatingNode,
        TLinkNode* branchedNode,
        const TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TLinkNode* originatingNode,
        TLinkNode* branchedNode) override;

    virtual void DoClone(
        TLinkNode* sourceNode,
        TLinkNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNode
    : public TCypressNodeBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYTree::INodePtr, Value);

public:
    explicit TDocumentNode(const TVersionedNodeId& id);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TDocumentNode>
{
public:
    explicit TDocumentNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
    typedef TCypressNodeTypeHandlerBase<TDocumentNode> TBase;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TDocumentNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override;

    virtual void DoBranch(
        const TDocumentNode* originatingNode,
        TDocumentNode* branchedNode,
        const TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TDocumentNode* originatingNode,
        TDocumentNode* branchedNode) override;

    virtual void DoClone(
        TDocumentNode* sourceNode,
        TDocumentNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
