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

#include <yt/server/tablet_server/tablet_cell_bundle.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/overlaid_attribute_dictionaries.h>
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
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(GetObjectType(), hintId);
        return DoCreate(
            TVersionedNodeId(id),
            externalCellTag,
            transaction,
            inheritedAttributes,
            explicitAttributes,
            account);
    }

    virtual void FillAttributes(
        TCypressNodeBase* trunkNode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes) override
    {
        for (const auto& key : inheritedAttributes->List()) {
            if (!IsSupportedInheritableAttribute(key)) {
                inheritedAttributes->Remove(key);
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto combinedAttributes = NYTree::OverlayAttributeDictionaries(explicitAttributes, inheritedAttributes);
        objectManager->FillAttributes(trunkNode, combinedAttributes);
    }

    virtual bool IsSupportedInheritableAttribute(const TString& key) const
    {
        // NB: most node types don't inherit attributes. That would lead to
        // a lot of pseudo-user attributes.
        return false;
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

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TImpl* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual std::unique_ptr<TImpl> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* /*transaction*/,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account)
    {
        auto nodeHolder = std::make_unique<TImpl>(id);
        nodeHolder->SetExternalCellTag(externalCellTag);
        nodeHolder->SetTrunkNode(nodeHolder.get());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(account, user, NSecurityServer::EPermission::Use);
        // Null is passed as transaction because DoCreate() always creates trunk nodes.
        securityManager->SetAccount(
            nodeHolder.get(),
            nullptr /* oldAccount */,
            account,
            nullptr /* transaction*/);

        return nodeHolder;
    }

    virtual void DoDestroy(TImpl* node)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetAccount(node);
    }

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& /*lockRequest*/)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* transaction = branchedNode->GetTransaction();
        auto* account = originatingNode->GetAccount();
        YCHECK(!branchedNode->GetAccount());
        securityManager->SetAccount(branchedNode, nullptr, account, transaction);
    }

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
        TImpl* branchedNode)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetAccount(branchedNode);
    }

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
        TImpl* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode /*mode*/,
        NSecurityServer::TAccount* account)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* transaction = clonedNode->IsTrunk() ? nullptr : factory->GetTransaction();
        securityManager->SetAccount(clonedNode, nullptr /* oldAccount */, account, transaction);
    }

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

class TCompositeNodeBase
    : public TCypressNodeBase
{
public:
    explicit TCompositeNodeBase(const TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    bool HasInheritableAttributes() const;

    // NB: the list of inheritable attributes doesn't include the "account"
    // attribute because that's already present on every Cypress node.

    TNullable<NCompression::ECodec> GetCompressionCodec() const;
    void SetCompressionCodec(TNullable<NCompression::ECodec> compressionCodec);

    TNullable<NErasure::ECodec> GetErasureCodec() const;
    void SetErasureCodec(TNullable<NErasure::ECodec> erasureCodec);

    TNullable<int> GetPrimaryMediumIndex() const;
    void SetPrimaryMediumIndex(TNullable<int> primaryMediumIndex);

    TNullable<NChunkServer::TChunkReplication> GetMedia() const;
    void SetMedia(TNullable<NChunkServer::TChunkReplication> media);

    // Although both Vital and ReplicationFactor can be deduced from Media, it's
    // important to be able to specify just the ReplicationFactor (or the Vital
    // flag) while leaving Media null.

    TNullable<int> GetReplicationFactor() const;
    void SetReplicationFactor(TNullable<int> replicationFactor);

    TNullable<bool> GetVital() const;
    void SetVital(TNullable<bool> vital);

    NTabletServer::TTabletCellBundle* GetTabletCellBundle() const;
    void SetTabletCellBundle(NTabletServer::TTabletCellBundle* tabletCellBundle);

    TNullable<NTransactionClient::EAtomicity> GetAtomicity() const;
    void SetAtomicity(TNullable<NTransactionClient::EAtomicity> atomicity);

    TNullable<NTransactionClient::ECommitOrdering> GetCommitOrdering() const;
    void SetCommitOrdering(TNullable<NTransactionClient::ECommitOrdering> commitOrdering);

    TNullable<NTabletClient::EInMemoryMode> GetInMemoryMode() const;
    void SetInMemoryMode(TNullable<NTabletClient::EInMemoryMode> inMemoryMode);

    TNullable<NTableClient::EOptimizeFor> GetOptimizeFor() const;
    void SetOptimizeFor(TNullable<NTableClient::EOptimizeFor> optimizeFor);

    struct TAttributes
    {
        TNullable<NCompression::ECodec> CompressionCodec;
        TNullable<NErasure::ECodec> ErasureCodec;
        TNullable<int> PrimaryMediumIndex;
        TNullable<NChunkServer::TChunkReplication> Media;
        TNullable<int> ReplicationFactor;
        TNullable<bool> Vital;
        NTabletServer::TTabletCellBundle* TabletCellBundle = nullptr;
        TNullable<NTransactionClient::EAtomicity> Atomicity;
        TNullable<NTransactionClient::ECommitOrdering> CommitOrdering;
        TNullable<NTabletClient::EInMemoryMode> InMemoryMode;
        TNullable<NTableClient::EOptimizeFor> OptimizeFor;

        void Persist(NCellMaster::TPersistenceContext& context);

        // Are all attributes not null?
        bool AreFull() const;

        // Are all attributes null?
        bool AreEmpty() const;
    };

    const TAttributes* Attributes() const;
    void SetAttributes(const TAttributes* attributes);

private:
    std::unique_ptr<TAttributes> Attributes_;
};

// Beware: changing these macros changes snapshot format.
#define FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(CompressionCodec, compression_codec) \
    process(ErasureCodec, erasure_codec) \
    process(ReplicationFactor, replication_factor) \
    process(Vital, vital) \
    process(Atomicity, atomicity) \
    process(CommitOrdering, commit_ordering) \
    process(InMemoryMode, in_memory_mode) \
    process(OptimizeFor, optimize_for)

#define FOR_EACH_INHERITABLE_ATTRIBUTE(process) \
    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(process) \
    process(PrimaryMediumIndex, primary_medium) \
    process(Media, media) \
    process(TabletCellBundle, tablet_cell_bundle)

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCompositeNodeBaseTypeHandler
    : public TCypressNodeTypeHandlerBase<TImpl>
{
    using TBase = TCypressNodeTypeHandlerBase<TImpl>;

public:
    using TBase::TBase;

protected:
    virtual void DoDestroy(TImpl* node) override
    {
        if (node->GetTabletCellBundle()) {
            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(node->GetTabletCellBundle());
        }

        TBase::DoDestroy(node);
    }

    virtual void DoClone(
        TImpl* sourceNode,
        TImpl* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

        clonedNode->SetAttributes(sourceNode->Attributes());

        if (clonedNode->GetTabletCellBundle()) {
            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->RefObject(clonedNode->GetTabletCellBundle());
        }
    }

    virtual void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetAttributes(originatingNode->Attributes());

        if (branchedNode->GetTabletCellBundle()) {
            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->RefObject(branchedNode->GetTabletCellBundle());
        }
    }

    virtual void DoUnbranch(
        TImpl* originatingNode,
        TImpl* branchedNode) override
    {
        TBase::DoUnbranch(originatingNode, branchedNode);

        if (branchedNode->GetTabletCellBundle()) {
            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(branchedNode->GetTabletCellBundle());
        }

        branchedNode->SetAttributes(nullptr); // just in case
    }

    virtual void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        if (originatingNode->GetTabletCellBundle()) {
            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(originatingNode->GetTabletCellBundle());
        }

        originatingNode->SetAttributes(branchedNode->Attributes());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCompositeNodeBase
{
public:
    typedef THashMap<TString, TCypressNodeBase*> TKeyToChild;
    typedef THashMap<TCypressNodeBase*, TString> TChildToKey;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);
    DEFINE_BYREF_RW_PROPERTY(int, ChildCountDelta);

public:
    using TCompositeNodeBase::TCompositeNodeBase;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeTypeHandler
    : public TCompositeNodeBaseTypeHandler<TMapNode>
{
public:
    using TBase = TCompositeNodeBaseTypeHandler<TMapNode>;

    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
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
    : public TCompositeNodeBase
{
public:
    typedef std::vector<TCypressNodeBase*> TIndexToChild;
    typedef THashMap<TCypressNodeBase*, int> TChildToIndex;

    DEFINE_BYREF_RW_PROPERTY(TIndexToChild, IndexToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToIndex, ChildToIndex);

public:
    using TCompositeNodeBase::TCompositeNodeBase;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual int GetGCWeight() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeTypeHandler
    : public TCompositeNodeBaseTypeHandler<TListNode>
{
public:
    using TBase = TCompositeNodeBaseTypeHandler<TListNode>;

    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override;
    virtual NYTree::ENodeType GetNodeType() const override;

private:
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
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account) override;

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
